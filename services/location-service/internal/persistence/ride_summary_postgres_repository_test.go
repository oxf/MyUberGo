package persistence

import (
	"context"
	"testing"
	"time"

	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"
)

func testSummary(rideID string) domain.RideSummary {
	started := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	return domain.RideSummary{
		RideID:     rideID,
		ClientID:   nextRideID(),
		DriverID:   nextRideID(),
		StartedAt:  started,
		EndedAt:    started.Add(10 * time.Minute),
		Start:      domain.Coordinate{Lat: 34.700, Lon: 33.000},
		End:        domain.Coordinate{Lat: 34.710, Lon: 33.010},
		Polyline:   "_p~iF~ps|U",
		DistanceM:  1500,
		DurationS:  600,
		PointCount: 42,
		Source:     domain.SourceSimplified,
	}
}

func TestRideSummaryRepository_InsertAndGetByRideID(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideSummaryRepository(testDB)

	rideID := nextRideID()
	summary := testSummary(rideID)

	if inserted, err := repo.Insert(ctx, summary); err != nil || !inserted {
		t.Fatalf("Insert: inserted=%v err=%v", inserted, err)
	}

	got, err := repo.GetByRideID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetByRideID: %v", err)
	}
	if got.RideID != rideID || got.DistanceM != 1500 || got.Source != domain.SourceSimplified {
		t.Fatalf("got %+v, want distance=1500 source=Simplified", got)
	}
	if got.Polyline != summary.Polyline || got.PointCount != 42 {
		t.Fatalf("got %+v, polyline/point_count mismatch", got)
	}
}

func TestRideSummaryRepository_GetByRideID_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideSummaryRepository(testDB)

	_, err := repo.GetByRideID(ctx, nextRideID())
	if err != cmnerrors.ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

// TestRideSummaryRepository_Insert_IdempotentAgainstRedelivery is the
// contract a redelivered ride.completed/ride.cancelled event relies on: a
// second Insert for the same ride_id is a silent no-op, not an error, and
// doesn't overwrite the original row.
func TestRideSummaryRepository_Insert_IdempotentAgainstRedelivery(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideSummaryRepository(testDB)

	rideID := nextRideID()
	first := testSummary(rideID)
	if inserted, err := repo.Insert(ctx, first); err != nil || !inserted {
		t.Fatalf("first Insert: inserted=%v err=%v", inserted, err)
	}

	second := testSummary(rideID)
	second.DistanceM = 9999 // deliberately different, must not overwrite
	inserted, err := repo.Insert(ctx, second)
	if err != nil {
		t.Fatalf("second Insert (redelivery): %v", err)
	}
	if inserted {
		t.Fatal("second Insert (redelivery) reported inserted=true, want false on conflict")
	}

	got, err := repo.GetByRideID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetByRideID: %v", err)
	}
	if got.DistanceM != 1500 {
		t.Fatalf("redelivery overwrote the original row: got distance_m=%d, want 1500", got.DistanceM)
	}
}
