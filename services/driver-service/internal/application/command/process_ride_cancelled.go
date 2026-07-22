package command

import (
	"context"
	"driver-service/internal/common/decorator"
	"log"

	"github.com/sirupsen/logrus"
)

type ProcessRideCancelled struct {
	RideID      string
	DriverID    *string // nil if the ride was cancelled before being matched
	CancelledAt string  // RFC3339, from the Kafka event
}

type ProcessRideCancelledHandler struct{}

func NewProcessRideCancelledHandler(
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[ProcessRideCancelled] {

	handler := &ProcessRideCancelledHandler{}

	return decorator.ApplyCommandDecoratorsNoResult[ProcessRideCancelled](
		handler,
		logger,
		metricsClient,
	)
}

// Handle is a placeholder, same as ProcessRideAccepted: driver-service has no
// persisted "on a ride" state today, so there's nothing here to reverse.
// Revisit once that flow exists.
func (h *ProcessRideCancelledHandler) Handle(ctx context.Context, cmd ProcessRideCancelled) error {
	log.Printf("ride.cancelled processed for driver. RideID=%s DriverID=%v", cmd.RideID, cmd.DriverID)
	return nil
}
