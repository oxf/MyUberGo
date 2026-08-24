package cache

import (
	"context"
	"testing"
	"time"

	"location-service/internal/domain"
)

func TestClientLocationRepository_UpsertAndLastPosition_RoundTrips(t *testing.T) {
	ctx := context.Background()
	repo := NewClientLocationRepository(testRedis)

	clientID := nextCacheID("client")
	now := time.Now().UTC().Truncate(time.Second)
	pos := domain.Position{
		Coordinate: mustCoord(t, 34.707, 33.022),
		AccuracyM:  10,
		HeadingDeg: 90,
		SpeedMps:   5,
		DeviceTs:   now,
		ServerTs:   now,
	}

	if err := repo.UpsertPosition(ctx, clientID, pos); err != nil {
		t.Fatalf("upsert position: %v", err)
	}

	got, err := repo.LastPosition(ctx, clientID)
	if err != nil {
		t.Fatalf("last position: %v", err)
	}
	if got == nil {
		t.Fatal("expected a position, got nil")
	}
	if got.Coordinate != pos.Coordinate || !got.DeviceTs.Equal(pos.DeviceTs) {
		t.Fatalf("got %+v, want %+v", *got, pos)
	}
}

func TestClientLocationRepository_LastPosition_MissingReturnsNilNoError(t *testing.T) {
	ctx := context.Background()
	repo := NewClientLocationRepository(testRedis)

	got, err := repo.LastPosition(ctx, nextCacheID("never-pinged-client"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}
