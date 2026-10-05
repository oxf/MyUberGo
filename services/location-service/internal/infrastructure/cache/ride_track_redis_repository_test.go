package cache

import (
	"context"
	"testing"
	"time"

	"location-service/internal/domain"
)

func TestRideTrackRepository_AppendAndRange(t *testing.T) {
	ctx := context.Background()
	repo := NewRideTrackRepository(testRedis)
	rideID := nextCacheID("ride-track")

	base := time.Now().UTC()
	positions := []domain.Position{
		{Coordinate: mustCoord(t, 34.700, 33.000), DeviceTs: base, ServerTs: base},
		{Coordinate: mustCoord(t, 34.701, 33.000), DeviceTs: base.Add(time.Second), ServerTs: base.Add(time.Second)},
		{Coordinate: mustCoord(t, 34.702, 33.000), DeviceTs: base.Add(2 * time.Second), ServerTs: base.Add(2 * time.Second)},
	}

	for _, p := range positions {
		if err := repo.Append(ctx, rideID, domain.SubjectDriver, p); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	got, err := repo.Range(ctx, rideID, "")
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(got) != len(positions) {
		t.Fatalf("Range: want %d entries, got %d", len(positions), len(got))
	}
	for i, entry := range got {
		if entry.Subject != domain.SubjectDriver {
			t.Fatalf("entry %d: want subject driver, got %v", i, entry.Subject)
		}
		if entry.Position.Coordinate != positions[i].Coordinate {
			t.Fatalf("entry %d: coordinate mismatch: got %+v want %+v", i, entry.Position.Coordinate, positions[i].Coordinate)
		}
		if entry.StreamID == "" {
			t.Fatalf("entry %d: empty stream id", i)
		}
	}
}

func TestRideTrackRepository_RangeAfterID(t *testing.T) {
	ctx := context.Background()
	repo := NewRideTrackRepository(testRedis)
	rideID := nextCacheID("ride-track")

	base := time.Now().UTC()
	for i := 0; i < 3; i++ {
		pos := domain.Position{
			Coordinate: mustCoord(t, 34.700+float64(i)*0.001, 33.000),
			DeviceTs:   base.Add(time.Duration(i) * time.Second),
			ServerTs:   base.Add(time.Duration(i) * time.Second),
		}
		if err := repo.Append(ctx, rideID, domain.SubjectDriver, pos); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	all, err := repo.Range(ctx, rideID, "")
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("want 3 entries, got %d", len(all))
	}

	after, err := repo.Range(ctx, rideID, all[0].StreamID)
	if err != nil {
		t.Fatalf("Range after: %v", err)
	}
	if len(after) != 2 {
		t.Fatalf("want 2 entries after the first id, got %d", len(after))
	}
}

func TestRideTrackRepository_ArchivedCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := NewRideTrackRepository(testRedis)
	rideID := nextCacheID("ride-track")

	id, err := repo.LastArchivedID(ctx, rideID)
	if err != nil {
		t.Fatalf("LastArchivedID (unset): %v", err)
	}
	if id != "" {
		t.Fatalf("want empty checkpoint, got %q", id)
	}

	if err := repo.SetLastArchivedID(ctx, rideID, "123-0"); err != nil {
		t.Fatalf("SetLastArchivedID: %v", err)
	}

	id, err = repo.LastArchivedID(ctx, rideID)
	if err != nil {
		t.Fatalf("LastArchivedID: %v", err)
	}
	if id != "123-0" {
		t.Fatalf("want checkpoint 123-0, got %q", id)
	}
}

func TestRideTrackRepository_Trim(t *testing.T) {
	ctx := context.Background()
	repo := NewRideTrackRepository(testRedis)
	rideID := nextCacheID("ride-track")

	pos := domain.Position{Coordinate: mustCoord(t, 34.7, 33.0), DeviceTs: time.Now(), ServerTs: time.Now()}
	if err := repo.Append(ctx, rideID, domain.SubjectDriver, pos); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := repo.Trim(ctx, rideID); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	got, err := repo.Range(ctx, rideID, "")
	if err != nil {
		t.Fatalf("Range after trim: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty track after trim, got %d entries", len(got))
	}
}
