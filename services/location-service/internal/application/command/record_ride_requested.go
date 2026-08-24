package command

import (
	"context"

	"location-service/internal/common/decorator"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

// RecordRideRequested caches a ride's clientID pending its ride.accepted
// counterpart — see TrackingRepository for the completion-order design.
type RecordRideRequested struct {
	RideID   string
	ClientID string
}

type RecordRideRequestedHandler struct {
	tracking domain.TrackingRepository
}

func NewRecordRideRequestedHandler(
	tracking domain.TrackingRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[RecordRideRequested] {
	if tracking == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &RecordRideRequestedHandler{tracking: tracking}
	return decorator.ApplyCommandDecoratorsNoResult[RecordRideRequested](handler, logger, metricsClient)
}

func (h *RecordRideRequestedHandler) Handle(ctx context.Context, cmd RecordRideRequested) error {
	if cmd.RideID == "" || cmd.ClientID == "" {
		return nil
	}
	return h.tracking.RecordRideRequested(ctx, cmd.RideID, cmd.ClientID)
}
