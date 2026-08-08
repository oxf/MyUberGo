package command

import (
	"context"
	"driver-service/internal/application/services"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"
	"driver-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type ProcessRideAccepted struct {
	RideID     string
	DriverID   string
	AcceptedAt string // RFC3339, from the Kafka event
}

type ProcessRideAcceptedHandler struct {
	profileRepo domain.DriverRepository
	transaction services.TransactionManager
	logger      *logrus.Entry
	metrics     decorator.MetricsClient
}

func NewProcessRideAcceptedHandler(
	profileRepo domain.DriverRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[ProcessRideAccepted] {
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}

	handler := &ProcessRideAcceptedHandler{
		profileRepo: profileRepo,
		transaction: transaction,
		logger:      logger,
		metrics:     metricsClient,
	}

	return decorator.ApplyCommandDecoratorsNoResult[ProcessRideAccepted](
		handler,
		logger,
		metricsClient,
	)
}

// Handle flips the driver Online -> OnRide. Guarded so a duplicate/late
// ride.accepted delivery (Kafka is at-most-once here, but redelivery/replay
// is still possible) is a silent no-op rather than an error - e.g. if the
// driver already went offline, or this is a redelivery after the flip
// already happened.
func (h *ProcessRideAcceptedHandler) Handle(ctx context.Context, cmd ProcessRideAccepted) error {
	return h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		changed, err := h.profileRepo.UpdateDriverStatus(ctx, cmd.DriverID, "Online", "OnRide")
		if err != nil {
			return err
		}
		if !changed {
			h.logger.Warnf("ride.accepted: driver %s not flipped to OnRide (not currently Online) for ride %s", cmd.DriverID, cmd.RideID)
		} else {
			h.metrics.IncCounter(ctx, "myubergo.driver.status_transitions",
				attribute.String("from", "Online"), attribute.String("to", "OnRide"))
		}
		return nil
	})
}
