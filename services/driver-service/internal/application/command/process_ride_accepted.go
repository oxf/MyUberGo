package command

import (
	"context"
	"driver-service/internal/common/decorator"
	"log"

	"github.com/sirupsen/logrus"
)

type ProcessRideAccepted struct {
	RideID     string
	DriverID   string
	AcceptedAt string // RFC3339, from the Kafka event
}

type ProcessRideAcceptedHandler struct{}

func NewProcessRideAcceptedHandler(
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[ProcessRideAccepted] {

	handler := &ProcessRideAcceptedHandler{}

	return decorator.ApplyCommandDecoratorsNoResult[ProcessRideAccepted](
		handler,
		logger,
		metricsClient,
	)
}

// Handle is a placeholder: driver-service has no persisted "on a ride" state
// today (driver_profile.status only allows Offline/Online, and there's no
// ride-completion event yet to reverse whatever gets set). Revisit once that
// flow exists.
func (h *ProcessRideAcceptedHandler) Handle(ctx context.Context, cmd ProcessRideAccepted) error {
	log.Printf("ride.accepted processed for driver. RideID=%s DriverID=%s", cmd.RideID, cmd.DriverID)
	return nil
}
