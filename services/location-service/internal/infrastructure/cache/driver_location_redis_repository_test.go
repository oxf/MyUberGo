package cache

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"location-service/internal/domain"
)

var cacheSeedSeq atomic.Int64

func nextCacheID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, cacheSeedSeq.Add(1))
}

func mustCoord(t *testing.T, lat, lon float64) domain.Coordinate {
	t.Helper()
	c, err := domain.NewCoordinate(lat, lon)
	if err != nil {
		t.Fatalf("invalid test coordinate (%v,%v): %v", lat, lon, err)
	}
	return c
}

func TestUpsertPositionAndNearby_FiltersByRadius(t *testing.T) {
	ctx := context.Background()
	repo := NewDriverLocationRepository(testRedis)

	center := mustCoord(t, 34.707, 33.022) // Limassol
	near := nextCacheID("driver")
	far := nextCacheID("driver")

	now := time.Now().UTC()
	if err := repo.UpsertPosition(ctx, near, domain.Position{
		Coordinate: center, AccuracyM: 10, DeviceTs: now, ServerTs: now,
	}); err != nil {
		t.Fatalf("upsert near driver: %v", err)
	}
	// ~1 degree of latitude away (~111km) — well outside a 5km search.
	farCoord := mustCoord(t, center.Lat+1, center.Lon)
	if err := repo.UpsertPosition(ctx, far, domain.Position{
		Coordinate: farCoord, AccuracyM: 10, DeviceTs: now, ServerTs: now,
	}); err != nil {
		t.Fatalf("upsert far driver: %v", err)
	}

	results, err := repo.Nearby(ctx, center, 5, 10)
	if err != nil {
		t.Fatalf("nearby: %v", err)
	}

	var gotNear, gotFar bool
	for _, r := range results {
		if r.DriverID == near {
			gotNear = true
			if r.DistanceM > 50 {
				t.Errorf("near driver distance = %dm, want < 50m", r.DistanceM)
			}
		}
		if r.DriverID == far {
			gotFar = true
		}
	}
	if !gotNear {
		t.Fatal("expected near driver in results")
	}
	if gotFar {
		t.Fatal("far driver should not be within a 5km search")
	}
}

// TestNearby_DistanceMatchesHaversineWithinTolerance guards a GEOADD lon/lat
// swap: it'd put the driver on the wrong side of the planet, failing loudly.
func TestNearby_DistanceMatchesHaversineWithinTolerance(t *testing.T) {
	ctx := context.Background()
	repo := NewDriverLocationRepository(testRedis)

	center := mustCoord(t, 34.707, 33.022)
	target := mustCoord(t, 34.707+0.01, 33.022) // ~1.1km north
	driverID := nextCacheID("driver")

	now := time.Now().UTC()
	if err := repo.UpsertPosition(ctx, driverID, domain.Position{
		Coordinate: target, AccuracyM: 10, DeviceTs: now, ServerTs: now,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	results, err := repo.Nearby(ctx, center, 5, 10)
	if err != nil {
		t.Fatalf("nearby: %v", err)
	}

	want := domain.HaversineMeters(center, target)
	found := false
	for _, r := range results {
		if r.DriverID != driverID {
			continue
		}
		found = true
		diff := math.Abs(float64(r.DistanceM) - want)
		// Redis GEO uses its own spherical distance calc, so allow a small
		// tolerance rather than exact equality.
		if diff > want*0.01+5 {
			t.Errorf("distance = %dm, want ~%.0fm (within 1%%+5m)", r.DistanceM, want)
		}
	}
	if !found {
		t.Fatal("expected driver in nearby results")
	}
}

func TestLastPosition_RoundTripsAllFields(t *testing.T) {
	ctx := context.Background()
	repo := NewDriverLocationRepository(testRedis)

	driverID := nextCacheID("driver")
	coord := mustCoord(t, 34.707, 33.022)
	deviceTs := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	serverTs := time.Now().UTC().Truncate(time.Second)

	want := domain.Position{
		Coordinate: coord, AccuracyM: 12.5, HeadingDeg: 270, SpeedMps: 13.9,
		DeviceTs: deviceTs, ServerTs: serverTs,
	}
	if err := repo.UpsertPosition(ctx, driverID, want); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := repo.LastPosition(ctx, driverID)
	if err != nil {
		t.Fatalf("last position: %v", err)
	}
	if got == nil {
		t.Fatal("expected a position, got nil")
	}
	if got.Coordinate != want.Coordinate || got.AccuracyM != want.AccuracyM ||
		got.HeadingDeg != want.HeadingDeg || got.SpeedMps != want.SpeedMps ||
		!got.DeviceTs.Equal(want.DeviceTs) || !got.ServerTs.Equal(want.ServerTs) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestLastPosition_MissingDriverReturnsNilNotError(t *testing.T) {
	ctx := context.Background()
	repo := NewDriverLocationRepository(testRedis)

	got, err := repo.LastPosition(ctx, nextCacheID("never-pinged"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for a driver that never pinged, got %+v", got)
	}
}

func TestStaleDriverIDsAndEvict(t *testing.T) {
	ctx := context.Background()
	repo := NewDriverLocationRepository(testRedis)

	coord := mustCoord(t, 34.707, 33.022)
	stale := nextCacheID("driver")
	fresh := nextCacheID("driver")

	base := time.Now().UTC()
	if err := repo.UpsertPosition(ctx, stale, domain.Position{
		Coordinate: coord, DeviceTs: base.Add(-5 * time.Minute), ServerTs: base.Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("upsert stale: %v", err)
	}
	if err := repo.UpsertPosition(ctx, fresh, domain.Position{
		Coordinate: coord, DeviceTs: base, ServerTs: base,
	}); err != nil {
		t.Fatalf("upsert fresh: %v", err)
	}

	cutoff := base.Add(-1 * time.Minute)
	staleIDs, err := repo.StaleDriverIDs(ctx, cutoff)
	if err != nil {
		t.Fatalf("stale driver ids: %v", err)
	}

	var found bool
	for _, id := range staleIDs {
		if id == fresh {
			t.Fatal("fresh driver should not be reported stale")
		}
		if id == stale {
			found = true
		}
	}
	if !found {
		t.Fatal("expected stale driver in results")
	}

	if err := repo.Evict(ctx, []string{stale}); err != nil {
		t.Fatalf("evict: %v", err)
	}

	// Evict must clear both keys — a driver left in the geo index after
	// eviction from lastseen (or vice versa) is exactly §16.2/§16.3's trap.
	if _, err := testRedis.ZScore(ctx, lastSeenKey, stale).Result(); err == nil {
		t.Fatal("expected stale driver to be removed from lastseen set")
	}
	if _, err := testRedis.ZScore(ctx, geoKey, stale).Result(); err == nil {
		t.Fatal("expected stale driver to be removed from geo index")
	}
	// Untouched.
	if _, err := testRedis.ZScore(ctx, lastSeenKey, fresh).Result(); err != nil {
		t.Fatalf("expected fresh driver to remain in lastseen set: %v", err)
	}
}

func TestEvict_EmptyIsNoop(t *testing.T) {
	if err := NewDriverLocationRepository(testRedis).Evict(context.Background(), nil); err != nil {
		t.Fatalf("expected no-op, got error: %v", err)
	}
}
