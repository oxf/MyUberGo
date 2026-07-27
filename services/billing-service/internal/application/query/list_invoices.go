package query

import (
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"context"

	"github.com/sirupsen/logrus"
)

type ListInvoices struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

type ListInvoicesHandler struct {
	repo domain.InvoiceRepository
}

func NewListInvoicesHandler(
	repo domain.InvoiceRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[ListInvoices, PagedResult[*domain.Invoice]] {

	handler := &ListInvoicesHandler{repo: repo}

	return decorator.ApplyQueryDecorators[ListInvoices, PagedResult[*domain.Invoice]](
		handler,
		logger,
		metricsClient,
	)
}

func (h *ListInvoicesHandler) Handle(ctx context.Context, q ListInvoices) (PagedResult[*domain.Invoice], error) {
	total, err := h.repo.CountInvoices(ctx)
	if err != nil {
		return PagedResult[*domain.Invoice]{}, err
	}

	items, err := h.repo.GetList(ctx, domain.PageRequest{
		Page:     q.Page,
		PageSize: q.PageSize,
		SortBy:   q.SortBy,
		SortDir:  q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.Invoice]{}, err
	}

	return PagedResult[*domain.Invoice]{Items: items, TotalCount: total}, nil
}
