package command

import (
	"context"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"testing"
)

type fakeFeeCalculator struct{}

func (fakeFeeCalculator) Calculate(ctx context.Context, ride *domain.Ride) (float64, error) {
	return 0, nil
}

func TestCancelRide_InProgressIsConflict(t *testing.T) {
	ride := newInProgressRide()
	repo := &fakeLifecycleRideRepo{ride: ride}
	h := &CancelRideHandler{
		repo:          repo,
		outboxRepo:    &fakeOutboxRepo{},
		transaction:   fakeTx{},
		feeCalculator: fakeFeeCalculator{},
	}

	_, err := h.Handle(context.Background(), CancelRide{RideID: "ride-1", ClientID: "client-1"})
	if err != commonerrors.ErrConflict {
		t.Fatalf("expected ErrConflict for InProgress ride, got %v", err)
	}
	if len(repo.cancelledCalls) != 0 {
		t.Fatalf("CancelRide should not have been called: %v", repo.cancelledCalls)
	}
}

func TestCancelRide_MatchedIsStillCancellable(t *testing.T) {
	driverID := "driver-1"
	ride := &domain.Ride{ID: "ride-1", ClientID: "client-1", DriverID: &driverID, Status: "Matched"}
	repo := &fakeLifecycleRideRepo{ride: ride}
	h := &CancelRideHandler{
		repo:          repo,
		outboxRepo:    &fakeOutboxRepo{},
		transaction:   fakeTx{},
		feeCalculator: fakeFeeCalculator{},
	}

	result, err := h.Handle(context.Background(), CancelRide{RideID: "ride-1", ClientID: "client-1"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if result.Status != "Cancelled" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(repo.cancelledCalls) != 1 {
		t.Fatalf("expected CancelRide to be called once: %v", repo.cancelledCalls)
	}
}
