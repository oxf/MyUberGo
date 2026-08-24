package command

import (
	"context"
	"time"

	"location-service/internal/common/decorator"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

// RecordRideAccepted caches a ride's driverID pending its ride.requested
// counterpart — see TrackingRepository for the completion-order design.
type RecordRideAccepted struct {
	RideID     string
	DriverID   string
	AcceptedAt time.Time
}

type RecordRideAcceptedHandler struct {
	tracking domain.TrackingRepository
}

func NewRecordRideAcceptedHandler(
	tracking domain.TrackingRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[RecordRideAccepted] {
	if tracking == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &RecordRideAcceptedHandler{tracking: tracking}
	return decorator.ApplyCommandDecoratorsNoResult[RecordRideAccepted](handler, logger, metricsClient)
}

func (h *RecordRideAcceptedHandler) Handle(ctx context.Context, cmd RecordRideAccepted) error {
	if cmd.RideID == "" || cmd.DriverID == "" {
		return nil
	}
	acceptedAt := cmd.AcceptedAt
	if acceptedAt.IsZero() {
		acceptedAt = time.Now().UTC()
	}
	return h.tracking.RecordRideAccepted(ctx, cmd.RideID, cmd.DriverID, acceptedAt)
}
