package query

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetShiftList struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

type GetShiftListHandler struct {
	repo domain.ShiftRepository
}

func NewGetShiftListHandler(repo domain.ShiftRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetShiftList, PagedResult[*domain.Shift]] {
	handler := &GetShiftListHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetShiftList, PagedResult[*domain.Shift]](handler, logger, metricsClient)
}

func (h *GetShiftListHandler) Handle(ctx context.Context, q GetShiftList) (PagedResult[*domain.Shift], error) {
	total, err := h.repo.CountShifts(ctx)
	if err != nil {
		return PagedResult[*domain.Shift]{}, err
	}

	items, err := h.repo.GetShiftList(ctx, domain.PageRequest{
		Page: q.Page, PageSize: q.PageSize, SortBy: q.SortBy, SortDir: q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.Shift]{}, err
	}

	return PagedResult[*domain.Shift]{Items: items, TotalCount: total}, nil
}
