package command

import (
	"context"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"
	"ride-service/internal/infrastructure/metrics"
	"time"

	"github.com/sirupsen/logrus"
)

type MarkRideMatched struct {
	RideID      string
	DriverID    string
	AcceptedAt  string // RFC3339, from the Kafka event
	RequestedAt string // RFC3339, from the Kafka event; empty for an older producer
}

type MarkRideMatchedHandler struct {
	repo    domain.RideRepository
	metrics decorator.MetricsClient
}

func NewMarkRideMatchedHandler(
	repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[MarkRideMatched] {
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}

	handler := &MarkRideMatchedHandler{repo: repo, metrics: metricsClient}

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

	if err := h.repo.MarkRideMatched(ctx, cmd.RideID, cmd.DriverID, matchedAt); err != nil {
		return err
	}

	// RequestedAt is empty if this event came from a producer that predates
	// the field — skip the SLI rather than fail the match itself.
	if requestedAt, parseErr := time.Parse(time.RFC3339, cmd.RequestedAt); parseErr == nil {
		h.metrics.RecordValue(ctx, "myubergo.ride.time_to_match", matchedAt.Sub(requestedAt).Seconds())
	}

	return nil
}
