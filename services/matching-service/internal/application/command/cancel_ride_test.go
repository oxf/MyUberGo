package command

import (
	"context"
	"testing"

	"matching-service/internal/domain"
)

func TestCancelRide_StillSearching_NoDriverRestored(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	drivers := &fakeDrivers{}
	offers := &fakeOffers{}
	h := &CancelRideHandler{rides: rides, drivers: drivers, offers: offers}

	if err := h.Handle(context.Background(), CancelRide{RideID: "r1", DriverID: nil}); err != nil {
		t.Fatal(err)
	}
	if !offers.cancelled {
		t.Fatal("expected SetCancelled to be called")
	}
	if len(offers.deletedPending) != 1 {
		t.Fatalf("expected DeletePending, got %v", offers.deletedPending)
	}
	if len(drivers.addedBack) != 0 {
		t.Fatalf("expected no driver restored, got %v", drivers.addedBack)
	}
	if len(rides.cancelled) != 1 {
		t.Fatalf("expected MarkCancelled, got %v", rides.cancelled)
	}
}

// TestCancelRide_MatchedRide_RestoresDriverFromLiveState is the regression
// test for the stranded-driver bug: even when the event's DriverID is nil
// (ride-service's own row was still "Requested" when it cancelled), the
// driver must still be restored if matching-service's own AcceptedBy already
// shows the ride as matched.
func TestCancelRide_MatchedRide_RestoresDriverFromLiveState(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusMatched, DriverRating: 4.7}}
	drivers := &fakeDrivers{}
	offers := &fakeOffers{acceptedBy: "d1"}
	h := &CancelRideHandler{rides: rides, drivers: drivers, offers: offers}

	if err := h.Handle(context.Background(), CancelRide{RideID: "r1", DriverID: nil}); err != nil {
		t.Fatal(err)
	}
	if !offers.cancelled {
		t.Fatal("expected SetCancelled to be called")
	}
	rating, restored := drivers.addedBack["d1"]
	if !restored {
		t.Fatalf("expected driver d1 restored to drivers:online, got %v", drivers.addedBack)
	}
	if rating != 4.7 {
		t.Fatalf("expected restored rating 4.7, got %v", rating)
	}
	if len(rides.cancelled) != 1 {
		t.Fatalf("expected MarkCancelled, got %v", rides.cancelled)
	}
}
