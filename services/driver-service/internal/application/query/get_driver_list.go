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
	repo domain.DriverProfileRepository
}

func NewGetDriverListHandler(repo domain.DriverProfileRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetDriverList, PagedResult[*domain.DriverProfile]] {
	handler := &GetDriverListHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetDriverList, PagedResult[*domain.DriverProfile]](handler, logger, metricsClient)
}

func (h *GetDriverListHandler) Handle(ctx context.Context, q GetDriverList) (PagedResult[*domain.DriverProfile], error) {
	total, err := h.repo.CountDriverProfiles(ctx)
	if err != nil {
		return PagedResult[*domain.DriverProfile]{}, err
	}

	items, err := h.repo.GetDriverProfileList(ctx, domain.PageRequest{
		Page: q.Page, PageSize: q.PageSize, SortBy: q.SortBy, SortDir: q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.DriverProfile]{}, err
	}

	return PagedResult[*domain.DriverProfile]{Items: items, TotalCount: total}, nil
}
