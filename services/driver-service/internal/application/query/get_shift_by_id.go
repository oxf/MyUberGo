package query

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetShiftByID struct {
	ID string
}

type GetShiftByIDHandler struct {
	repo domain.ShiftRepository
}

func NewGetShiftByIDHandler(repo domain.ShiftRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetShiftByID, *domain.Shift] {
	handler := &GetShiftByIDHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetShiftByID, *domain.Shift](handler, logger, metricsClient)
}

func (h *GetShiftByIDHandler) Handle(ctx context.Context, q GetShiftByID) (*domain.Shift, error) {
	return h.repo.GetShiftByID(ctx, q.ID)
}
