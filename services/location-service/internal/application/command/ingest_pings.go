package command

import (
	"context"
	"sort"
	"time"

	"location-service/internal/common/decorator"
	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

// PingInput is one unvalidated ping as decoded from the wire, before the
// domain's coordinate/bounds validation runs.
type PingInput struct {
	Lat        float64
	Lon        float64
	AccuracyM  float64
	HeadingDeg float64
	SpeedMps   float64
	DeviceTs   time.Time
}

// IngestPings ingests one batch of pings for the driver owned by UserID —
// no DriverID field: identity is resolved server-side, not caller-asserted.
type IngestPings struct {
	UserID string
	Pings  []PingInput
}

type IngestPingsResult struct {
	Accepted int
	Rejected int
}

type IngestPingsHandler struct {
	owner   domain.OwnerRepository
	drivers domain.DriverLocationRepository
	config  domain.ValidationConfig
	logger  *logrus.Entry
	metrics decorator.MetricsClient
}

func NewIngestPingsHandler(
	owner domain.OwnerRepository,
	drivers domain.DriverLocationRepository,
	config domain.ValidationConfig,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[IngestPings, IngestPingsResult] {
	if owner == nil || drivers == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &IngestPingsHandler{owner: owner, drivers: drivers, config: config, logger: logger, metrics: metricsClient}
	return decorator.ApplyCommandDecorators[IngestPings, IngestPingsResult](handler, logger, metricsClient)
}

// Handle resolves the caller's driverId, validates each ping in DeviceTs
// order, then writes only the last accepted position — one Redis round trip.
func (h *IngestPingsHandler) Handle(ctx context.Context, cmd IngestPings) (IngestPingsResult, error) {
	driverID, err := h.owner.DriverIDForUser(ctx, cmd.UserID)
	if err != nil {
		return IngestPingsResult{}, err
	}
	if driverID == "" {
		return IngestPingsResult{}, cmnerrors.ErrForbidden
	}

	sorted := make([]PingInput, len(cmd.Pings))
	copy(sorted, cmd.Pings)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].DeviceTs.Before(sorted[j].DeviceTs) })

	previous, err := h.drivers.LastPosition(ctx, driverID)
	if err != nil {
		return IngestPingsResult{}, err
	}

	now := time.Now().UTC()
	result := IngestPingsResult{}
	var latestAccepted *domain.Position

	for _, raw := range sorted {
		pos, reason := domain.ValidatePing(domain.RawPing{
			Lat: raw.Lat, Lon: raw.Lon, AccuracyM: raw.AccuracyM,
			HeadingDeg: raw.HeadingDeg, SpeedMps: raw.SpeedMps, DeviceTs: raw.DeviceTs,
		}, previous, h.config, now)

		if reason != domain.RejectNone {
			result.Rejected++
			h.metrics.IncCounter(ctx, "myubergo.location.pings_rejected", attribute.String("reason", string(reason)))
			continue
		}

		result.Accepted++
		h.metrics.IncCounter(ctx, "myubergo.location.pings_accepted")
		latestAccepted = &pos
		previous = &pos // subsequent pings in this batch validate against this one, not just Redis's stored value
	}

	if latestAccepted != nil {
		if err := h.drivers.UpsertPosition(ctx, driverID, *latestAccepted); err != nil {
			return IngestPingsResult{}, err
		}
		h.metrics.RecordDuration(ctx, "myubergo.location.ingest_lag", now.Sub(latestAccepted.DeviceTs))
	}

	return result, nil
}
