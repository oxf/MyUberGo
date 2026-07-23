package command

import (
	"context"
	"encoding/json"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"testing"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func newMatchedRide() *domain.Ride {
	driverID := "driver-1"
	return &domain.Ride{ID: "ride-1", ClientID: "client-1", DriverID: &driverID, Status: "Matched"}
}

func TestStartRide_HappyPath(t *testing.T) {
	repo := &fakeLifecycleRideRepo{ride: newMatchedRide()}
	outbox := &fakeOutboxRepo{}
	h := &StartRideHandler{repo: repo, outboxRepo: outbox, transaction: fakeTx{}}

	result, err := h.Handle(context.Background(), StartRide{RideID: "ride-1", DriverID: "driver-1"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if result.Status != "InProgress" || result.StartedAt == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(repo.startedCalls) != 1 || repo.startedCalls[0] != "ride-1" {
		t.Fatalf("MarkRideStarted not called correctly: %v", repo.startedCalls)
	}
	if len(outbox.inserted) != 1 || outbox.inserted[0].Topic != "ride.started" {
		t.Fatalf("expected 1 ride.started outbox message, got %+v", outbox.inserted)
	}
	var ev contractsKafka.RideStartedEvent
	if err := json.Unmarshal(outbox.inserted[0].Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.RideID != "ride-1" || ev.DriverID != "driver-1" || ev.StartedAt == "" {
		t.Fatalf("bad event payload: %+v", ev)
	}
}

func TestStartRide_DriverMismatchForbidden(t *testing.T) {
	repo := &fakeLifecycleRideRepo{ride: newMatchedRide()}
	h := &StartRideHandler{repo: repo, outboxRepo: &fakeOutboxRepo{}, transaction: fakeTx{}}

	_, err := h.Handle(context.Background(), StartRide{RideID: "ride-1", DriverID: "someone-else"})
	if err != commonerrors.ErrForbidden {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}

func TestStartRide_WrongStatusConflict(t *testing.T) {
	ride := newMatchedRide()
	ride.Status = "Requested"
	repo := &fakeLifecycleRideRepo{ride: ride}
	h := &StartRideHandler{repo: repo, outboxRepo: &fakeOutboxRepo{}, transaction: fakeTx{}}

	_, err := h.Handle(context.Background(), StartRide{RideID: "ride-1", DriverID: "driver-1"})
	if err != commonerrors.ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestStartRide_NotFound(t *testing.T) {
	repo := &fakeLifecycleRideRepo{}
	h := &StartRideHandler{repo: repo, outboxRepo: &fakeOutboxRepo{}, transaction: fakeTx{}}

	_, err := h.Handle(context.Background(), StartRide{RideID: "missing", DriverID: "driver-1"})
	if err != commonerrors.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
