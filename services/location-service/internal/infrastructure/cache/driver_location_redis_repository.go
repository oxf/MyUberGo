package cache

import (
	"context"
	"math"
	"strconv"
	"time"

	"location-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

const (
	geoKey      = "loc:drivers:geo"
	lastSeenKey = "loc:drivers:lastseen"
	locationTTL = 5 * time.Minute
)

func driverKey(driverID string) string { return "loc:driver:" + driverID }

type DriverLocationRepository struct {
	rdb *redis.Client
}

func NewDriverLocationRepository(rdb *redis.Client) *DriverLocationRepository {
	return &DriverLocationRepository{rdb: rdb}
}

// LastPosition reads loc:driver:{id} in one round trip. A missing key (never
// pinged, or evicted) returns (nil, nil) — not an error case.
func (r *DriverLocationRepository) LastPosition(ctx context.Context, driverID string) (*domain.Position, error) {
	m, err := r.rdb.HGetAll(ctx, driverKey(driverID)).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}

	lat, err := strconv.ParseFloat(m["lat"], 64)
	if err != nil {
		return nil, err
	}
	lon, err := strconv.ParseFloat(m["lon"], 64)
	if err != nil {
		return nil, err
	}
	accuracyM, err := strconv.ParseFloat(m["accuracyM"], 64)
	if err != nil {
		return nil, err
	}
	headingDeg, err := strconv.ParseFloat(m["headingDeg"], 64)
	if err != nil {
		return nil, err
	}
	speedMps, err := strconv.ParseFloat(m["speedMps"], 64)
	if err != nil {
		return nil, err
	}
	deviceTs, err := time.Parse(time.RFC3339, m["deviceTs"])
	if err != nil {
		return nil, err
	}
	serverTs, err := time.Parse(time.RFC3339, m["serverTs"])
	if err != nil {
		return nil, err
	}

	coord, err := domain.NewCoordinate(lat, lon)
	if err != nil {
		return nil, err
	}

	return &domain.Position{
		Coordinate: coord,
		AccuracyM:  accuracyM,
		HeadingDeg: headingDeg,
		SpeedMps:   speedMps,
		DeviceTs:   deviceTs,
		ServerTs:   serverTs,
	}, nil
}

// UpsertPosition writes the geo index, lastseen score, and detail hash in one
// pipeline. GeoLocation fields are set by name (lon before lat) to avoid the classic geo swap bug.
func (r *DriverLocationRepository) UpsertPosition(ctx context.Context, driverID string, pos domain.Position) error {
	pipe := r.rdb.Pipeline()
	pipe.GeoAdd(ctx, geoKey, &redis.GeoLocation{
		Name:      driverID,
		Longitude: pos.Coordinate.Lon,
		Latitude:  pos.Coordinate.Lat,
	})
	pipe.ZAdd(ctx, lastSeenKey, redis.Z{Score: float64(pos.ServerTs.UnixMilli()), Member: driverID})
	pipe.HSet(ctx, driverKey(driverID), map[string]any{
		"lat":        pos.Coordinate.Lat,
		"lon":        pos.Coordinate.Lon,
		"accuracyM":  pos.AccuracyM,
		"headingDeg": pos.HeadingDeg,
		"speedMps":   pos.SpeedMps,
		"deviceTs":   pos.DeviceTs.UTC().Format(time.RFC3339),
		"serverTs":   pos.ServerTs.UTC().Format(time.RFC3339),
	})
	pipe.Expire(ctx, driverKey(driverID), locationTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// Nearby runs GEOSEARCH ... BYRADIUS ... ASC WITHDIST — geographic
// candidates only, nearest first, no availability filtering.
func (r *DriverLocationRepository) Nearby(ctx context.Context, center domain.Coordinate, radiusKm float64, limit int) ([]domain.NearbyDriver, error) {
	locs, err := r.rdb.GeoSearchLocation(ctx, geoKey, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  center.Lon,
			Latitude:   center.Lat,
			Radius:     radiusKm,
			RadiusUnit: "km",
			Sort:       "ASC",
			Count:      limit,
		},
		WithDist: true,
	}).Result()
	if err != nil {
		return nil, err
	}

	out := make([]domain.NearbyDriver, 0, len(locs))
	for _, loc := range locs {
		// loc.Dist arrives in km; round once to the nearest metre and cast to
		// int64 — never persist/compare an intermediate float distance.
		out = append(out, domain.NearbyDriver{
			DriverID:  loc.Name,
			DistanceM: int64(math.Round(loc.Dist * 1000)),
		})
	}
	return out, nil
}

// StaleDriverIDs returns driver ids whose lastseen score is older than
// olderThan. Uses ZRangeArgs/ByScore, not the deprecated ZRangeByScore.
func (r *DriverLocationRepository) StaleDriverIDs(ctx context.Context, olderThan time.Time) ([]string, error) {
	return r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     lastSeenKey,
		Start:   "0",
		Stop:    strconv.FormatInt(olderThan.UnixMilli(), 10),
		ByScore: true,
	}).Result()
}

// Evict removes driverIDs from both the geo index and lastseen set in one
// pipeline. Redis GEO has no per-member TTL and no GEODEL — ZREM is the only way out.
func (r *DriverLocationRepository) Evict(ctx context.Context, driverIDs []string) error {
	if len(driverIDs) == 0 {
		return nil
	}
	members := make([]interface{}, len(driverIDs))
	for i, id := range driverIDs {
		members[i] = id
	}
	pipe := r.rdb.Pipeline()
	pipe.ZRem(ctx, geoKey, members...)
	pipe.ZRem(ctx, lastSeenKey, members...)
	_, err := pipe.Exec(ctx)
	return err
}
