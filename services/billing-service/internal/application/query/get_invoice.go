package query

import (
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"context"

	"github.com/sirupsen/logrus"
)

type GetInvoice struct {
	ID string
}

type GetInvoiceHandler struct {
	repo domain.InvoiceRepository
}

func NewGetInvoiceHandler(
	repo domain.InvoiceRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetInvoice, *domain.Invoice] {

	handler := &GetInvoiceHandler{repo: repo}

	return decorator.ApplyQueryDecorators[GetInvoice, *domain.Invoice](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetInvoiceHandler) Handle(ctx context.Context, q GetInvoice) (*domain.Invoice, error) {
	return h.repo.GetByID(ctx, q.ID)
}
