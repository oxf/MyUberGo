package command

import (
	"context"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"
	"time"

	"github.com/sirupsen/logrus"
)

type MarkRideMatched struct {
	RideID     string
	DriverID   string
	AcceptedAt string // RFC3339, from the Kafka event
}

type MarkRideMatchedHandler struct {
	repo domain.RideRepository
}

func NewMarkRideMatchedHandler(
	repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[MarkRideMatched] {

	handler := &MarkRideMatchedHandler{repo: repo}

	return decorator.ApplyCommandDecoratorsNoResult[MarkRideMatched](
		handler,
		logger,
		metricsClient,
	)
}

func (h *MarkRideMatchedHandler) Handle(ctx context.Context, cmd MarkRideMatched) error {
	matchedAt, err := time.Parse(time.RFC3339, cmd.AcceptedAt)
	if err != nil {
		matchedAt = time.Now().UTC()
	}
	return h.repo.MarkRideMatched(ctx, cmd.RideID, cmd.DriverID, matchedAt)
}
