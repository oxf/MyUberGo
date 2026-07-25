package query

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetDriverList struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

type GetDriverListHandler struct {
	repo domain.DriverRepository
}

func NewGetDriverListHandler(repo domain.DriverRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetDriverList, PagedResult[*domain.Driver]] {
	handler := &GetDriverListHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetDriverList, PagedResult[*domain.Driver]](handler, logger, metricsClient)
}

func (h *GetDriverListHandler) Handle(ctx context.Context, q GetDriverList) (PagedResult[*domain.Driver], error) {
	total, err := h.repo.CountDrivers(ctx)
	if err != nil {
		return PagedResult[*domain.Driver]{}, err
	}

	items, err := h.repo.GetDriverList(ctx, domain.PageRequest{
		Page: q.Page, PageSize: q.PageSize, SortBy: q.SortBy, SortDir: q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.Driver]{}, err
	}

	return PagedResult[*domain.Driver]{Items: items, TotalCount: total}, nil
}
