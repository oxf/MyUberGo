package command

import (
	"billing-service/internal/application/services"
	"billing-service/internal/common/decorator"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"

	"github.com/sirupsen/logrus"
)

type RemovePaymentMethod struct {
	ID       string
	ClientID string
}

type RemovePaymentMethodHandler struct {
	paymentMethodRepo domain.PaymentMethodRepository
	invoiceRepo       domain.InvoiceRepository
	transaction       services.TransactionManager
}

func NewRemovePaymentMethodHandler(
	paymentMethodRepo domain.PaymentMethodRepository,
	invoiceRepo domain.InvoiceRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[RemovePaymentMethod] {

	handler := &RemovePaymentMethodHandler{
		paymentMethodRepo: paymentMethodRepo,
		invoiceRepo:       invoiceRepo,
		transaction:       transaction,
	}

	return decorator.ApplyCommandDecoratorsNoResult[RemovePaymentMethod](
		handler,
		logger,
		metricsClient,
	)
}

// Handle soft-deletes a payment method (status='removed'). 409s if it's the
// client's active default and they have open invoices — the partial unique
// index guarantees a client has at most one active default, so "is
// default" already implies "the only one".
func (h *RemovePaymentMethodHandler) Handle(ctx context.Context, cmd RemovePaymentMethod) error {
	return h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		m, err := h.paymentMethodRepo.GetByID(ctx, cmd.ID)
		if err != nil {
			return err
		}
		if m.ClientID != cmd.ClientID {
			return commonerrors.ErrForbidden
		}

		if m.IsDefault {
			openCount, err := h.invoiceRepo.CountOpenByClientID(ctx, cmd.ClientID)
			if err != nil {
				return err
			}
			if openCount > 0 {
				return commonerrors.ErrConflict
			}
		}

		return h.paymentMethodRepo.MarkRemoved(ctx, cmd.ID)
	})
}
