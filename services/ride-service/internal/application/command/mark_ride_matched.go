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
	repo    domain.RideRepository
	metrics decorator.MetricsClient
}

func NewMarkRideMatchedHandler(
	repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[MarkRideMatched] {

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

	// Fetched before the write purely for the requested_at timestamp — the
	// headline ride.time_to_match SLI (requested -> matched) has nowhere
	// else to read it from, since MarkRideMatched only takes rideID/driverID.
	// Only done when there's a metrics client to report to, so tests
	// exercising this handler via a partial fake repo (embedding
	// domain.RideRepository with only MarkRideMatched overridden) aren't
	// forced to also implement GetRideByID.
	var ride *domain.Ride
	var rideErr error
	if h.metrics != nil {
		ride, rideErr = h.repo.GetRideByID(ctx, cmd.RideID)
	}

	if err := h.repo.MarkRideMatched(ctx, cmd.RideID, cmd.DriverID, matchedAt); err != nil {
		return err
	}

	if h.metrics != nil && rideErr == nil {
		if requestedAt, parseErr := time.Parse(time.RFC3339, ride.CreatedAt); parseErr == nil {
			h.metrics.RecordValue(ctx, "myubergo.ride.time_to_match", matchedAt.Sub(requestedAt).Seconds())
		}
	}

	return nil
}
