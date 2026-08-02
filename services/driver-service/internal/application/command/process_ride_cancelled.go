package command

import (
	"context"
	"driver-service/internal/application/services"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type ProcessRideCancelled struct {
	RideID      string
	DriverID    *string // nil if the ride was cancelled before being matched
	CancelledAt string  // RFC3339, from the Kafka event
}

type ProcessRideCancelledHandler struct {
	profileRepo domain.DriverRepository
	transaction services.TransactionManager
	logger      *logrus.Entry
	metrics     decorator.MetricsClient
}

func NewProcessRideCancelledHandler(
	profileRepo domain.DriverRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[ProcessRideCancelled] {

	handler := &ProcessRideCancelledHandler{
		profileRepo: profileRepo,
		transaction: transaction,
		logger:      logger,
		metrics:     metricsClient,
	}

	return decorator.ApplyCommandDecoratorsNoResult[ProcessRideCancelled](
		handler,
		logger,
		metricsClient,
	)
}

// Handle flips the driver OnRide -> Online. No-ops cleanly if DriverID is
// nil (ride cancelled before a match existed - nothing to reverse). Guarded
// the same way as ProcessRideAccepted: if the driver isn't currently
// OnRide (e.g. they already went Offline on their own), we deliberately do
// NOT force them back Online - a driver who ended their shift mid-ride
// should stay Offline, not be silently reactivated by a cancellation event.
func (h *ProcessRideCancelledHandler) Handle(ctx context.Context, cmd ProcessRideCancelled) error {
	if cmd.DriverID == nil {
		return nil
	}

	driverID := *cmd.DriverID
	return h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		changed, err := h.profileRepo.UpdateDriverStatus(ctx, driverID, "OnRide", "Online")
		if err != nil {
			return err
		}
		if !changed {
			h.logger.Warnf("ride.cancelled: driver %s not flipped to Online (not currently OnRide) for ride %s", driverID, cmd.RideID)
		} else if h.metrics != nil {
			h.metrics.IncCounter(ctx, "myubergo.driver.status_transitions",
				attribute.String("from", "OnRide"), attribute.String("to", "Online"))
		}
		return nil
	})
}
