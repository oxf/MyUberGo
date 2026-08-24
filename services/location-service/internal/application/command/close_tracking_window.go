package command

import (
	"context"

	"location-service/internal/common/decorator"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

// CloseTrackingWindow closes a ride's tracking window on ride.completed or
// ride.cancelled. Redis-only — WS force-close is wired at the consumer layer.
type CloseTrackingWindow struct {
	RideID string
}

type CloseTrackingWindowHandler struct {
	tracking domain.TrackingRepository
}

func NewCloseTrackingWindowHandler(
	tracking domain.TrackingRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[CloseTrackingWindow] {
	if tracking == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &CloseTrackingWindowHandler{tracking: tracking}
	return decorator.ApplyCommandDecoratorsNoResult[CloseTrackingWindow](handler, logger, metricsClient)
}

func (h *CloseTrackingWindowHandler) Handle(ctx context.Context, cmd CloseTrackingWindow) error {
	if cmd.RideID == "" {
		return nil
	}
	return h.tracking.CloseWindow(ctx, cmd.RideID)
}
