package command

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type UpdateShift struct {
	ID     string
	Status string
}

type UpdateShiftHandler struct {
	repo domain.ShiftRepository
}

func NewUpdateShiftHandler(repo domain.ShiftRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandlerNoResult[UpdateShift] {
	handler := &UpdateShiftHandler{repo: repo}
	return decorator.ApplyCommandDecoratorsNoResult[UpdateShift](handler, logger, metricsClient)
}

func (h *UpdateShiftHandler) Handle(ctx context.Context, cmd UpdateShift) error {
	if cmd.Status == "Ended" {
		return h.repo.EndShift(ctx, cmd.ID)
	}
	return nil
}
