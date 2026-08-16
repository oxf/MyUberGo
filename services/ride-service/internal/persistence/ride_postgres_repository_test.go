package persistence

import (
	"context"
	"errors"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"testing"
	"time"
)

func TestCreateRide_RoundTrip(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)

	ride := &domain.Ride{
		ClientID:            clientID,
		Status:              "Requested",
		PickupLat:           40.0,
		PickupLng:           -73.0,
		PickupAddress:       "123 Main St",
		DestLat:             40.1,
		DestLng:             -73.1,
		DestAddress:         "456 Elm St",
		EstimatedPriceMinor: 1234,
		Currency:            "USD",
		EstimatedDistanceKm: 5.5,
	}

	id, err := repo.CreateRide(ctx, ride)
	if err != nil {
		t.Fatalf("CreateRide: %v", err)
	}
	if id == "" {
		t.Fatal("expected a non-empty id")
	}

	got, err := repo.GetRideByID(ctx, id)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if got.ClientID != clientID || got.Status != "Requested" || got.EstimatedPriceMinor != 1234 || got.Currency != "USD" {
		t.Fatalf("round-tripped ride mismatch: %+v", got)
	}
	if got.DriverID != nil {
		t.Fatalf("expected nil DriverID before matching, got %q", *got.DriverID)
	}
}

func TestGetRideByID_NotFound_ReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)

	_, err := repo.GetRideByID(ctx, "00000000-0000-0000-0000-000000000099")
	if !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetRideList_PaginationAndSortFallback(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)

	before, err := repo.CountRides(ctx)
	if err != nil {
		t.Fatalf("CountRides before: %v", err)
	}

	for range 3 {
		seedRide(t, testDB, clientID, "Requested")
	}

	after, err := repo.CountRides(ctx)
	if err != nil {
		t.Fatalf("CountRides after: %v", err)
	}
	if after-before != 3 {
		t.Fatalf("expected count delta of 3, got %d", after-before)
	}

	// An invalid SortBy (not in domain.RideSortColumns) must fall back to
	// created_at rather than erroring — the handler is expected to validate
	// SortBy, but the repository defends against that anyway.
	list, err := repo.GetRideList(ctx, domain.PageRequest{
		Page:     1,
		PageSize: after,
		SortBy:   "not-a-real-column",
		SortDir:  "DESC",
	})
	if err != nil {
		t.Fatalf("GetRideList: %v", err)
	}
	if len(list) != after {
		t.Fatalf("expected %d rides, got %d", after, len(list))
	}
}

func TestMarkRideMatched_GuardBlocksRedelivery(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)
	driver1 := seedDriver(t, testDB)
	driver2 := seedDriver(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	matchedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.MarkRideMatched(ctx, rideID, driver1, matchedAt); err != nil {
		t.Fatalf("first MarkRideMatched: %v", err)
	}

	ride, err := repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "Matched" || ride.DriverID == nil || *ride.DriverID != driver1 {
		t.Fatalf("expected ride matched to driver1, got status=%s driverID=%v", ride.Status, ride.DriverID)
	}

	// Simulate a redelivered ride.accepted naming a different driver — the
	// "AND status = 'Requested'" guard must make this a no-op.
	if err := repo.MarkRideMatched(ctx, rideID, driver2, time.Now()); err != nil {
		t.Fatalf("second (redelivered) MarkRideMatched: %v", err)
	}

	ride, err = repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID after redelivery: %v", err)
	}
	if ride.Status != "Matched" || ride.DriverID == nil || *ride.DriverID != driver1 {
		t.Fatalf("guard failed: redelivery changed driver to %v (want unchanged driver1=%s)", ride.DriverID, driver1)
	}
}

// TestFailRide_GuardBlocksRedeliveryAndAlreadyMatched guards docs/AUDIT_2026-08-15.md #11:
// matching-service publishes ride.matching_failed after exhausting its retries, and the
// "AND status = 'Requested'" guard must (a) make a redelivered failure a no-op and (b) never
// clobber a ride that matched in the meantime (a real race: the retry that gave up and the
// accept that won could be in flight at the same time).
func TestFailRide_GuardBlocksRedeliveryAndAlreadyMatched(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	if err := repo.FailRide(ctx, rideID); err != nil {
		t.Fatalf("first FailRide: %v", err)
	}
	ride, err := repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "Failed" {
		t.Fatalf("expected ride Failed, got status=%s", ride.Status)
	}

	// Redelivery must be a no-op — the guard only matches status = 'Requested'.
	if err := repo.FailRide(ctx, rideID); err != nil {
		t.Fatalf("second (redelivered) FailRide: %v", err)
	}
	ride, err = repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID after redelivery: %v", err)
	}
	if ride.Status != "Failed" {
		t.Fatalf("guard failed: redelivery changed status to %s (want unchanged Failed)", ride.Status)
	}

	// A ride that already matched must never be clobbered by a late/racing failure.
	driver1 := seedDriver(t, testDB)
	matchedRideID := seedRide(t, testDB, clientID, "Requested")
	if err := repo.MarkRideMatched(ctx, matchedRideID, driver1, time.Now()); err != nil {
		t.Fatalf("MarkRideMatched: %v", err)
	}
	if err := repo.FailRide(ctx, matchedRideID); err != nil {
		t.Fatalf("FailRide on already-matched ride: %v", err)
	}
	ride, err = repo.GetRideByID(ctx, matchedRideID)
	if err != nil {
		t.Fatalf("GetRideByID for matched ride: %v", err)
	}
	if ride.Status != "Matched" {
		t.Fatalf("guard failed: FailRide clobbered a Matched ride, got status=%s", ride.Status)
	}
}

func TestMarkRideBilled_GuardBlocksRedelivery(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Completed")

	firstBillID := "11111111-1111-1111-1111-111111111111"
	secondBillID := "22222222-2222-2222-2222-222222222222"

	if err := repo.MarkRideBilled(ctx, rideID, firstBillID); err != nil {
		t.Fatalf("first MarkRideBilled: %v", err)
	}

	ride, err := repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.BillID == nil || *ride.BillID != firstBillID {
		t.Fatalf("expected bill_id %s, got %v", firstBillID, ride.BillID)
	}

	// Simulate a redelivered payment.completed — the "AND bill_id IS NULL"
	// guard must make this a no-op.
	if err := repo.MarkRideBilled(ctx, rideID, secondBillID); err != nil {
		t.Fatalf("second (redelivered) MarkRideBilled: %v", err)
	}

	ride, err = repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID after redelivery: %v", err)
	}
	if ride.BillID == nil || *ride.BillID != firstBillID {
		t.Fatalf("guard failed: redelivery changed bill_id to %v (want unchanged %s)", ride.BillID, firstBillID)
	}
}

// TestGetRideForUpdate_LocksRowBlocksConcurrentReader exercises the real
// "FOR UPDATE" row lock under two genuinely concurrent, separately-committed
// transactions — a behavior no fake/mock repository can express.
func TestGetRideForUpdate_LocksRowBlocksConcurrentReader(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	tx1, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer func() { _ = tx1.Rollback() }()

	if _, err := repo.GetRideForUpdate(WithTx(ctx, tx1), rideID); err != nil {
		t.Fatalf("tx1 GetRideForUpdate: %v", err)
	}

	unblocked := make(chan struct{})
	go func() {
		tx2, err := testDB.BeginTx(ctx, nil)
		if err != nil {
			t.Errorf("begin tx2: %v", err)
			return
		}
		defer func() { _ = tx2.Rollback() }()

		if _, err := repo.GetRideForUpdate(WithTx(ctx, tx2), rideID); err != nil {
			t.Errorf("tx2 GetRideForUpdate: %v", err)
			return
		}
		close(unblocked)
	}()

	select {
	case <-unblocked:
		t.Fatal("tx2's GetRideForUpdate returned before tx1 released the row lock")
	case <-time.After(300 * time.Millisecond):
		// expected: tx2 is still blocked on tx1's row lock.
	}

	if err := tx1.Rollback(); err != nil {
		t.Fatalf("rollback tx1: %v", err)
	}

	select {
	case <-unblocked:
		// success: tx2 acquired the lock once tx1 released it.
	case <-time.After(2 * time.Second):
		t.Fatal("tx2's GetRideForUpdate did not unblock after tx1 released the row lock")
	}
}

func TestCancelRide_SetsCancelledFields(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	if err := repo.CancelRide(ctx, rideID, "rider changed their mind"); err != nil {
		t.Fatalf("CancelRide: %v", err)
	}

	ride, err := repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "Cancelled" {
		t.Fatalf("expected status Cancelled, got %s", ride.Status)
	}
}

func TestMarkRideStarted_SetsInProgress(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Matched")

	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.MarkRideStarted(ctx, rideID, startedAt); err != nil {
		t.Fatalf("MarkRideStarted: %v", err)
	}

	ride, err := repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "InProgress" {
		t.Fatalf("expected status InProgress, got %s", ride.Status)
	}
}

func TestCompleteRide_SetsCompleted(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresRideRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "InProgress")

	finishedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.CompleteRide(ctx, rideID, finishedAt); err != nil {
		t.Fatalf("CompleteRide: %v", err)
	}

	ride, err := repo.GetRideByID(ctx, rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "Completed" {
		t.Fatalf("expected status Completed, got %s", ride.Status)
	}
}
