package query

import (
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"context"

	"github.com/sirupsen/logrus"
)

type ListPaymentMethods struct {
	ClientID string
}

type ListPaymentMethodsHandler struct {
	repo domain.PaymentMethodRepository
}

func NewListPaymentMethodsHandler(
	repo domain.PaymentMethodRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[ListPaymentMethods, []*domain.PaymentMethod] {

	handler := &ListPaymentMethodsHandler{repo: repo}

	return decorator.ApplyQueryDecorators[ListPaymentMethods, []*domain.PaymentMethod](
		handler,
		logger,
		metricsClient,
	)
}

func (h *ListPaymentMethodsHandler) Handle(ctx context.Context, q ListPaymentMethods) ([]*domain.PaymentMethod, error) {
	return h.repo.ListByClientID(ctx, q.ClientID)
}
