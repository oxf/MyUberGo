package command

import (
	"billing-service/internal/application/services"
	"billing-service/internal/common/decorator"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"

	"github.com/sirupsen/logrus"
)

type AddPaymentMethod struct {
	ClientID                string
	ProviderPaymentMethodID string
	Brand                   string
	Last4                   string
	ExpMonth                int
	ExpYear                 int
	SetDefault              bool
}

type AddPaymentMethodResult struct {
	ID string
}

type AddPaymentMethodHandler struct {
	customerRepo      domain.CustomerRepository
	paymentMethodRepo domain.PaymentMethodRepository
	vault             services.CustomerVault
	transaction       services.TransactionManager
	providerName      string
}

func NewAddPaymentMethodHandler(
	customerRepo domain.CustomerRepository,
	paymentMethodRepo domain.PaymentMethodRepository,
	vault services.CustomerVault,
	transaction services.TransactionManager,
	providerName string,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[AddPaymentMethod, AddPaymentMethodResult] {

	handler := &AddPaymentMethodHandler{
		customerRepo:      customerRepo,
		paymentMethodRepo: paymentMethodRepo,
		vault:             vault,
		transaction:       transaction,
		providerName:      providerName,
	}

	return decorator.ApplyCommandDecorators[AddPaymentMethod, AddPaymentMethodResult](
		handler,
		logger,
		metricsClient,
	)
}

// Handle deliberately keeps provider network calls (EnsureCustomer,
// AttachPaymentMethod) outside any DB transaction — the same
// claim/charge/finalize discipline ChargeWorker follows, for the same
// reason: a real provider call is a network round-trip and must never hold
// a Postgres transaction open around it.
func (h *AddPaymentMethodHandler) Handle(ctx context.Context, cmd AddPaymentMethod) (AddPaymentMethodResult, error) {
	providerCustomerID, err := h.ensureCustomer(ctx, cmd.ClientID)
	if err != nil {
		return AddPaymentMethodResult{}, err
	}

	details, err := h.vault.AttachPaymentMethod(ctx, providerCustomerID, cmd.ProviderPaymentMethodID)
	if err != nil {
		return AddPaymentMethodResult{}, err
	}

	// Prefer the provider's own view of the card's display metadata over
	// whatever the caller claimed in the request body — a real adapter
	// returns the truth, so billing stops persisting an unverifiable
	// client-supplied brand/last4. The stub returns zero values, so this
	// preserves today's behaviour exactly.
	brand, last4, expMonth, expYear := cmd.Brand, cmd.Last4, cmd.ExpMonth, cmd.ExpYear
	if details.Brand != "" {
		brand, last4, expMonth, expYear = details.Brand, details.Last4, details.ExpMonth, details.ExpYear
	}

	var result AddPaymentMethodResult
	txErr := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		if cmd.SetDefault {
			if err := h.paymentMethodRepo.ClearDefault(ctx, cmd.ClientID); err != nil {
				return err
			}
		}

		id, err := h.paymentMethodRepo.Create(ctx, &domain.PaymentMethod{
			ClientID:                cmd.ClientID,
			Provider:                h.providerName,
			ProviderPaymentMethodID: cmd.ProviderPaymentMethodID,
			Brand:                   brand,
			Last4:                   last4,
			ExpMonth:                expMonth,
			ExpYear:                 expYear,
			IsDefault:               cmd.SetDefault,
			Status:                  domain.PaymentMethodStatusActive,
		})
		if err != nil {
			return err
		}

		result = AddPaymentMethodResult{ID: id}
		return nil
	})
	if txErr == nil {
		return result, nil
	}

	// A retried/double-submitted attach of the same underlying card —
	// re-read and return the existing row instead of erroring, the same
	// idiom ensureCustomer above uses for ErrCustomerExists.
	if errors.Is(txErr, domain.ErrPaymentMethodExists) {
		existing, err := h.paymentMethodRepo.GetActiveByProviderID(ctx, cmd.ClientID, h.providerName, cmd.ProviderPaymentMethodID)
		if err != nil {
			return AddPaymentMethodResult{}, err
		}
		return AddPaymentMethodResult{ID: existing.ID}, nil
	}
	return AddPaymentMethodResult{}, txErr
}

// ensureCustomer get-or-creates the billing.customer row for a client,
// tolerating the race where two concurrent first-time calls both find no
// row and both call the provider: the DB's (client_id, provider) unique
// constraint is the actual source of truth, and the loser just re-reads
// instead of erroring.
func (h *AddPaymentMethodHandler) ensureCustomer(ctx context.Context, clientID string) (string, error) {
	existing, err := h.customerRepo.GetByClientID(ctx, clientID, h.providerName)
	if err == nil {
		return existing.ProviderCustomerID, nil
	}
	if !errors.Is(err, commonerrors.ErrNotFound) {
		return "", err
	}

	providerCustomerID, err := h.vault.EnsureCustomer(ctx, clientID)
	if err != nil {
		return "", err
	}

	insertErr := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		_, err := h.customerRepo.Create(ctx, &domain.Customer{
			ClientID: clientID, Provider: h.providerName, ProviderCustomerID: providerCustomerID,
		})
		return err
	})
	if insertErr == nil {
		return providerCustomerID, nil
	}
	if errors.Is(insertErr, domain.ErrCustomerExists) {
		existing, err := h.customerRepo.GetByClientID(ctx, clientID, h.providerName)
		if err != nil {
			return "", err
		}
		return existing.ProviderCustomerID, nil
	}
	return "", insertErr
}
