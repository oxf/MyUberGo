package command

import (
	"context"
	"encoding/json"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"testing"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func newInProgressRide() *domain.Ride {
	driverID := "driver-1"
	return &domain.Ride{ID: "ride-1", ClientID: "client-1", DriverID: &driverID, Status: "InProgress"}
}

func TestCompleteRide_HappyPath(t *testing.T) {
	repo := &fakeLifecycleRideRepo{ride: newInProgressRide()}
	outbox := &fakeOutboxRepo{}
	h := &CompleteRideHandler{repo: repo, outboxRepo: outbox, transaction: fakeTx{}}

	result, err := h.Handle(context.Background(), CompleteRide{RideID: "ride-1", DriverID: "driver-1"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if result.Status != "Completed" || result.FinishedAt == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(repo.completedCalls) != 1 || repo.completedCalls[0] != "ride-1" {
		t.Fatalf("CompleteRide not called correctly: %v", repo.completedCalls)
	}
	if len(outbox.inserted) != 1 || outbox.inserted[0].Topic != "ride.completed" {
		t.Fatalf("expected 1 ride.completed outbox message, got %+v", outbox.inserted)
	}
	var ev contractsKafka.RideCompletedEvent
	if err := json.Unmarshal(outbox.inserted[0].Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.RideID != "ride-1" || ev.DriverID != "driver-1" || ev.FinishedAt == "" {
		t.Fatalf("bad event payload: %+v", ev)
	}
}

func TestCompleteRide_DriverMismatchForbidden(t *testing.T) {
	repo := &fakeLifecycleRideRepo{ride: newInProgressRide()}
	h := &CompleteRideHandler{repo: repo, outboxRepo: &fakeOutboxRepo{}, transaction: fakeTx{}}

	_, err := h.Handle(context.Background(), CompleteRide{RideID: "ride-1", DriverID: "someone-else"})
	if err != commonerrors.ErrForbidden {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}

func TestCompleteRide_WrongStatusConflict(t *testing.T) {
	ride := newInProgressRide()
	ride.Status = "Matched"
	repo := &fakeLifecycleRideRepo{ride: ride}
	h := &CompleteRideHandler{repo: repo, outboxRepo: &fakeOutboxRepo{}, transaction: fakeTx{}}

	_, err := h.Handle(context.Background(), CompleteRide{RideID: "ride-1", DriverID: "driver-1"})
	if err != commonerrors.ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestCompleteRide_NotFound(t *testing.T) {
	repo := &fakeLifecycleRideRepo{}
	h := &CompleteRideHandler{repo: repo, outboxRepo: &fakeOutboxRepo{}, transaction: fakeTx{}}

	_, err := h.Handle(context.Background(), CompleteRide{RideID: "missing", DriverID: "driver-1"})
	if err != commonerrors.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
