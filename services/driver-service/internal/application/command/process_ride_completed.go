package command

import (
	"context"
	"driver-service/internal/application/services"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type ProcessRideCompleted struct {
	RideID     string
	DriverID   string
	FinishedAt string // RFC3339, from the Kafka event
}

type ProcessRideCompletedHandler struct {
	profileRepo domain.DriverRepository
	transaction services.TransactionManager
	logger      *logrus.Entry
	metrics     decorator.MetricsClient
}

func NewProcessRideCompletedHandler(
	profileRepo domain.DriverRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[ProcessRideCompleted] {

	handler := &ProcessRideCompletedHandler{
		profileRepo: profileRepo,
		transaction: transaction,
		logger:      logger,
		metrics:     metricsClient,
	}

	return decorator.ApplyCommandDecoratorsNoResult[ProcessRideCompleted](
		handler,
		logger,
		metricsClient,
	)
}

// Handle flips the driver OnRide -> Online, same guard philosophy as
// ProcessRideCancelledHandler. The total_rides_completed increment is tied
// to the flip actually happening: on a redelivered ride.completed the
// driver is already Online, the guard misses, and the increment is skipped
// too - this reuses UpdateDriverStatus's guard as the redelivery-dedup
// signal instead of needing a separate mechanism.
func (h *ProcessRideCompletedHandler) Handle(ctx context.Context, cmd ProcessRideCompleted) error {
	return h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		changed, err := h.profileRepo.UpdateDriverStatus(ctx, cmd.DriverID, "OnRide", "Online")
		if err != nil {
			return err
		}
		if !changed {
			h.logger.Warnf("ride.completed: driver %s not flipped to Online (not currently OnRide) for ride %s; skipping total_rides_completed increment", cmd.DriverID, cmd.RideID)
			return nil
		}
		if h.metrics != nil {
			h.metrics.IncCounter(ctx, "myubergo.driver.status_transitions",
				attribute.String("from", "OnRide"), attribute.String("to", "Online"))
		}
		return h.profileRepo.IncrementRidesCompleted(ctx, cmd.DriverID)
	})
}
