package command

import (
	"context"
	"testing"
)

func TestProcessRideCompleted_FlipsOnRideToOnlineAndIncrements(t *testing.T) {
	repo := &fakeDriverStatusRepo{changed: true}
	h := &ProcessRideCompletedHandler{profileRepo: repo, transaction: fakeTx{}, logger: testLogger()}

	err := h.Handle(context.Background(), ProcessRideCompleted{
		RideID:     "ride-1",
		DriverID:   "driver-1",
		FinishedAt: "2026-07-23T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(repo.calls) != 1 || repo.calls[0].ID != "driver-1" || repo.calls[0].From != "OnRide" || repo.calls[0].To != "Online" {
		t.Fatalf("unexpected UpdateDriverStatus call(s): %+v", repo.calls)
	}
	if len(repo.incrementCalls) != 1 || repo.incrementCalls[0] != "driver-1" {
		t.Fatalf("expected exactly one IncrementRidesCompleted call for driver-1, got %v", repo.incrementCalls)
	}
}

func TestProcessRideCompleted_GuardMissSkipsIncrement(t *testing.T) {
	repo := &fakeDriverStatusRepo{changed: false}
	h := &ProcessRideCompletedHandler{profileRepo: repo, transaction: fakeTx{}, logger: testLogger()}

	err := h.Handle(context.Background(), ProcessRideCompleted{RideID: "ride-1", DriverID: "driver-1"})
	if err != nil {
		t.Fatalf("expected nil error on guard miss, got %v", err)
	}
	if len(repo.incrementCalls) != 0 {
		t.Fatalf("expected no IncrementRidesCompleted calls on guard miss, got %v", repo.incrementCalls)
	}
}
