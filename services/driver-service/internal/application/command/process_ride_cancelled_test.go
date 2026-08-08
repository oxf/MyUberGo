package command

import (
	"context"
	"driver-service/internal/infrastructure/metrics"
	"testing"
)

func TestProcessRideCancelled_FlipsOnRideToOnline(t *testing.T) {
	repo := &fakeDriverStatusRepo{changed: true}
	h := &ProcessRideCancelledHandler{profileRepo: repo, transaction: fakeTx{}, logger: testLogger(), metrics: metrics.NewNoopMetricsClient()}

	driverID := "driver-1"
	err := h.Handle(context.Background(), ProcessRideCancelled{
		RideID:      "ride-1",
		DriverID:    &driverID,
		CancelledAt: "2026-07-23T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(repo.calls) != 1 || repo.calls[0].ID != "driver-1" || repo.calls[0].From != "OnRide" || repo.calls[0].To != "Online" {
		t.Fatalf("unexpected UpdateDriverStatus call(s): %+v", repo.calls)
	}
}

func TestProcessRideCancelled_NilDriverIDIsNoOp(t *testing.T) {
	repo := &fakeDriverStatusRepo{changed: true}
	h := &ProcessRideCancelledHandler{profileRepo: repo, transaction: fakeTx{}, logger: testLogger()}

	err := h.Handle(context.Background(), ProcessRideCancelled{RideID: "ride-1", DriverID: nil})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(repo.calls) != 0 {
		t.Fatalf("expected no UpdateDriverStatus calls for nil DriverID, got %+v", repo.calls)
	}
}

func TestProcessRideCancelled_GuardMissIsNotAnError(t *testing.T) {
	repo := &fakeDriverStatusRepo{changed: false}
	h := &ProcessRideCancelledHandler{profileRepo: repo, transaction: fakeTx{}, logger: testLogger()}

	driverID := "driver-1"
	err := h.Handle(context.Background(), ProcessRideCancelled{RideID: "ride-1", DriverID: &driverID})
	if err != nil {
		t.Fatalf("expected nil error on guard miss, got %v", err)
	}
}
