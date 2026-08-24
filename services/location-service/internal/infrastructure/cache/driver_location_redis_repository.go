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
	// stalenessThreshold filters Nearby's results against loc:drivers:lastseen so a
	// driver who stopped pinging doesn't stay a candidate for the ~150s worst-case
	// gap before StalenessWorker's next sweep evicts them (docs/AUDIT_2026-08-15.md #5).
	stalenessThreshold time.Duration
}

func NewDriverLocationRepository(rdb *redis.Client, stalenessThreshold time.Duration) *DriverLocationRepository {
	return &DriverLocationRepository{rdb: rdb, stalenessThreshold: stalenessThreshold}
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
	pos, err := decodePositionHash(m)
	if err != nil {
		return nil, err
	}
	return &pos, nil
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
	pipe.HSet(ctx, driverKey(driverID), positionHash(pos))
	pipe.Expire(ctx, driverKey(driverID), locationTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// Nearby runs GEOSEARCH ... BYRADIUS ... ASC WITHDIST — geographic candidates
// only, nearest first — then drops any candidate whose lastseen score is
// older than stalenessThreshold, so a driver mid-gap before the next
// StalenessWorker sweep isn't returned as a live candidate.
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
	if len(locs) == 0 {
		return nil, nil
	}

	ids := make([]string, len(locs))
	for i, loc := range locs {
		ids[i] = loc.Name
	}
	scores, err := r.rdb.ZMScore(ctx, lastSeenKey, ids...).Result()
	if err != nil {
		return nil, err
	}
	cutoff := float64(time.Now().Add(-r.stalenessThreshold).UnixMilli())

	out := make([]domain.NearbyDriver, 0, len(locs))
	for i, loc := range locs {
		// A missing lastseen entry scores 0 in go-redis's ZMScore, which is
		// always older than cutoff — treated the same as genuinely stale.
		if scores[i] < cutoff {
			continue
		}
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

// AllPositions returns every driver within stalenessThreshold of
// loc:drivers:lastseen, each paired with its detail hash — same staleness
// cutoff Nearby applies, just without the geo-radius filter.
func (r *DriverLocationRepository) AllPositions(ctx context.Context) ([]domain.DriverPosition, error) {
	cutoff := float64(time.Now().Add(-r.stalenessThreshold).UnixMilli())
	ids, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     lastSeenKey,
		Start:   strconv.FormatFloat(cutoff, 'f', -1, 64),
		Stop:    "+inf",
		ByScore: true,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, driverKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	out := make([]domain.DriverPosition, 0, len(ids))
	for i, id := range ids {
		m, err := cmds[i].Result()
		if err != nil || len(m) == 0 {
			// Hash TTL'd out between the ZRANGE and HGETALL reads — skip, not an error.
			continue
		}
		pos, err := decodePositionHash(m)
		if err != nil {
			continue
		}
		out = append(out, domain.DriverPosition{DriverID: id, Position: pos})
	}
	return out, nil
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
