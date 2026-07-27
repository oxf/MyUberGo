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
	transaction       services.TransactionManager
}

func NewAddPaymentMethodHandler(
	customerRepo domain.CustomerRepository,
	paymentMethodRepo domain.PaymentMethodRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[AddPaymentMethod, AddPaymentMethodResult] {

	handler := &AddPaymentMethodHandler{
		customerRepo:      customerRepo,
		paymentMethodRepo: paymentMethodRepo,
		transaction:       transaction,
	}

	return decorator.ApplyCommandDecorators[AddPaymentMethod, AddPaymentMethodResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *AddPaymentMethodHandler) Handle(ctx context.Context, cmd AddPaymentMethod) (AddPaymentMethodResult, error) {
	var result AddPaymentMethodResult
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		// Get-or-create the billing.customer row on first call.
		if _, err := h.customerRepo.GetByClientID(ctx, cmd.ClientID, domain.ProviderStub); err != nil {
			if !errors.Is(err, commonerrors.ErrNotFound) {
				return err
			}
			if _, err := h.customerRepo.Create(ctx, &domain.Customer{
				ClientID:           cmd.ClientID,
				Provider:           domain.ProviderStub,
				ProviderCustomerID: "cus_stub_" + cmd.ClientID,
			}); err != nil {
				return err
			}
		}

		if cmd.SetDefault {
			if err := h.paymentMethodRepo.ClearDefault(ctx, cmd.ClientID); err != nil {
				return err
			}
		}

		id, err := h.paymentMethodRepo.Create(ctx, &domain.PaymentMethod{
			ClientID:                cmd.ClientID,
			Provider:                domain.ProviderStub,
			ProviderPaymentMethodID: cmd.ProviderPaymentMethodID,
			Brand:                   cmd.Brand,
			Last4:                   cmd.Last4,
			ExpMonth:                cmd.ExpMonth,
			ExpYear:                 cmd.ExpYear,
			IsDefault:               cmd.SetDefault,
			Status:                  domain.PaymentMethodStatusActive,
		})
		if err != nil {
			return err
		}

		result = AddPaymentMethodResult{ID: id}
		return nil
	})

	return result, err
}
