package services

import "context"

// NearbyDriver mirrors location-service's own domain.NearbyDriver, kept distinct since the two
// services are separate Go modules with no shared domain package.
type NearbyDriver struct {
	DriverID  string
	DistanceM int64
}

// LocationClient calls location-service's internal-only GET /internal/drivers/nearby: geographic
// candidates only, no availability filtering or ranking (Location owns *where*, matching owns *who*).
type LocationClient interface {
	Nearby(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]NearbyDriver, error)
}
