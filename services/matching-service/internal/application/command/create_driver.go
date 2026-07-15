package command

import (
	"context"
	"matching-service/internal/common/decorator"
	"matching-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type CreateDriver struct {
	Event contracts.ShiftUpdatedEvent
}

type CreateDriverHandler struct {
	repo domain.DriverRepository
}

func NewCreateDriverHandler(repo domain.DriverRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandlerNoResult[CreateDriver] {
	if repo == nil {
		panic("nil repo")
	}

	handler := &CreateDriverHandler{repo: repo}
	return decorator.ApplyCommandDecoratorsNoResult[CreateDriver](handler, logger, metricsClient)
}

func (h *CreateDriverHandler) Handle(ctx context.Context, cmd CreateDriver) error {
	err := h.repo.CreateDriver(ctx, cmd.Event)
	return err
}
