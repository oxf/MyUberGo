package command

import (
	"context"
	"matching-service/internal/common/decorator"
	"matching-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type UpsertDriver struct {
	Event contracts.ShiftUpdatedEvent
}

type UpsertDriverHandler struct {
	repo domain.DriverRepository
}

func NewUpsertDriverHandler(repo domain.DriverRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandlerNoResult[UpsertDriver] {
	if repo == nil {
		panic("nil repo")
	}

	handler := &UpsertDriverHandler{repo: repo}
	return decorator.ApplyCommandDecoratorsNoResult[UpsertDriver](handler, logger, metricsClient)
}

func (h *UpsertDriverHandler) Handle(ctx context.Context, cmd UpsertDriver) error {
	err := h.repo.UpsertDriver(ctx, cmd.Event)
	return err
}
