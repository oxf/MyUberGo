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
}

type GetShiftListHandler struct {
	repo domain.ShiftRepository
}

func NewGetShiftListHandler(repo domain.ShiftRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetShiftList, []*domain.Shift] {
	handler := &GetShiftListHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetShiftList, []*domain.Shift](handler, logger, metricsClient)
}

func (h *GetShiftListHandler) Handle(ctx context.Context, q GetShiftList) ([]*domain.Shift, error) {
	return h.repo.GetShiftList(ctx, q.Page, q.PageSize)
}
