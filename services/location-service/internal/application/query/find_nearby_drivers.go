package query

import (
	"context"

	"location-service/internal/common/decorator"
	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"

	"github.com/sirupsen/logrus"
)

// FindNearbyDrivers answers GET /internal/drivers/nearby — geographic
// candidates only; matching-service owns availability filtering and ranking.
type FindNearbyDrivers struct {
	Lat      float64
	Lon      float64
	RadiusKm float64
	Limit    int
}

type FindNearbyDriversHandler struct {
	drivers domain.DriverLocationRepository
	metrics decorator.MetricsClient
}

func NewFindNearbyDriversHandler(
	drivers domain.DriverLocationRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[FindNearbyDrivers, []domain.NearbyDriver] {
	if drivers == nil {
		panic("nil repo")
	}
	handler := &FindNearbyDriversHandler{drivers: drivers, metrics: metricsClient}
	return decorator.ApplyQueryDecorators[FindNearbyDrivers, []domain.NearbyDriver](handler, logger, metricsClient)
}

func (h *FindNearbyDriversHandler) Handle(ctx context.Context, q FindNearbyDrivers) ([]domain.NearbyDriver, error) {
	center, err := domain.NewCoordinate(q.Lat, q.Lon)
	if err != nil {
		return nil, cmnerrors.ErrInvalidInput
	}
	if q.RadiusKm <= 0 || q.Limit <= 0 {
		return nil, cmnerrors.ErrInvalidInput
	}
	candidates, err := h.drivers.Nearby(ctx, center, q.RadiusKm, q.Limit)
	if err != nil {
		return nil, err
	}
	// nearby-candidates-returned (LOCATION_SPEC.md §12, docs/AUDIT_2026-08-15.md
	// #6) — query latency itself is already covered by the generic
	// myubergo.query.duration decorator metric, no separate histogram needed.
	if h.metrics != nil {
		h.metrics.RecordValue(ctx, "myubergo.location.nearby_candidates_returned", float64(len(candidates)))
	}
	return candidates, nil
}
