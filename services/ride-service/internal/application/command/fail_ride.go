package command

import (
	"context"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"
	"ride-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

// FailRide flips a ride to Failed once matching-service gives up after exhausting
// its retries (ride.matching_failed) — otherwise the Postgres row stays 'Requested'
// forever with no signal anywhere in the system of record (docs/AUDIT_2026-08-15.md #11).
type FailRide struct {
	RideID string
}

type FailRideHandler struct {
	repo    domain.RideRepository
	metrics decorator.MetricsClient
}

func NewFailRideHandler(
	repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[FailRide] {
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}

	handler := &FailRideHandler{repo: repo, metrics: metricsClient}

	return decorator.ApplyCommandDecoratorsNoResult[FailRide](
		handler,
		logger,
		metricsClient,
	)
}

func (h *FailRideHandler) Handle(ctx context.Context, cmd FailRide) error {
	if err := h.repo.FailRide(ctx, cmd.RideID); err != nil {
		return err
	}
	h.metrics.IncCounter(ctx, "myubergo.rides.failed")
	return nil
}
