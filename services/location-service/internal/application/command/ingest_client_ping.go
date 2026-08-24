package command

import (
	"context"
	"time"

	"location-service/internal/common/decorator"
	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

// IngestClientPing ingests one position for ClientID — the caller's own
// X-Client-Id. Gated on an open window, unlike ungated driver ingest (§17 decision 5).
type IngestClientPing struct {
	ClientID string
	Ping     PingInput
}

type IngestClientPingHandler struct {
	tracking  domain.TrackingRepository
	clients   domain.ClientLocationRepository
	publisher domain.PositionPublisher
	config    domain.ValidationConfig
	logger    *logrus.Entry
}

func NewIngestClientPingHandler(
	tracking domain.TrackingRepository,
	clients domain.ClientLocationRepository,
	publisher domain.PositionPublisher,
	config domain.ValidationConfig,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[IngestClientPing, IngestPingsResult] {
	if tracking == nil || clients == nil || publisher == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &IngestClientPingHandler{tracking: tracking, clients: clients, publisher: publisher, config: config, logger: logger}
	return decorator.ApplyCommandDecorators[IngestClientPing, IngestPingsResult](handler, logger, metricsClient)
}

func (h *IngestClientPingHandler) Handle(ctx context.Context, cmd IngestClientPing) (IngestPingsResult, error) {
	rideID, err := h.tracking.ActiveRideForClient(ctx, cmd.ClientID)
	if err != nil {
		return IngestPingsResult{}, err
	}
	if rideID == "" {
		return IngestPingsResult{}, cmnerrors.ErrForbidden
	}

	previous, err := h.clients.LastPosition(ctx, cmd.ClientID)
	if err != nil {
		return IngestPingsResult{}, err
	}

	now := time.Now().UTC()
	pos, reason := domain.ValidatePing(domain.RawPing{
		Lat: cmd.Ping.Lat, Lon: cmd.Ping.Lon, AccuracyM: cmd.Ping.AccuracyM,
		HeadingDeg: cmd.Ping.HeadingDeg, SpeedMps: cmd.Ping.SpeedMps, DeviceTs: cmd.Ping.DeviceTs,
	}, previous, h.config, now)

	if reason != domain.RejectNone {
		return IngestPingsResult{Rejected: 1}, nil
	}

	if err := h.clients.UpsertPosition(ctx, cmd.ClientID, pos); err != nil {
		return IngestPingsResult{}, err
	}

	// Publish failure doesn't fail the ingest — the write above already
	// succeeded (same reasoning as IngestPingsHandler.publishIfTracked).
	if err := h.publisher.Publish(ctx, domain.PositionUpdate{RideID: rideID, Subject: domain.SubjectClient, Position: pos}); err != nil {
		h.logger.WithError(err).WithField("client_id", cmd.ClientID).Warn("failed to publish position to tracking window")
	}

	return IngestPingsResult{Accepted: 1}, nil
}
