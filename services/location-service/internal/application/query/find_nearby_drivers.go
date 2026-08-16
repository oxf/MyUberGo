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
}

func NewFindNearbyDriversHandler(
	drivers domain.DriverLocationRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[FindNearbyDrivers, []domain.NearbyDriver] {
	if drivers == nil {
		panic("nil repo")
	}
	handler := &FindNearbyDriversHandler{drivers: drivers}
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
	return h.drivers.Nearby(ctx, center, q.RadiusKm, q.Limit)
}
