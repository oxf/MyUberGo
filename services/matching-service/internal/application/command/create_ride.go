package command

import (
	"context"
	"matching-service/internal/common/decorator"
	"matching-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type CreateRide struct {
	Event contracts.RideRequestedEvent
}

type CreateRideHandler struct {
	repo domain.RideRepository
}

func NewCreateRideHandler(repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandlerNoResult[CreateRide] {
	if repo == nil {
		panic("nil repo")
	}

	handler := &CreateRideHandler{repo: repo}
	return decorator.ApplyCommandDecoratorsNoResult[CreateRide](handler, logger, metricsClient)
}

func (h *CreateRideHandler) Handle(ctx context.Context, cmd CreateRide) error {
	err := h.repo.SaveRide(ctx, cmd.Event)
	return err
}
