package command

import (
	"context"
	"encoding/json"
	"ride-service/internal/domain"
	"testing"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

type fakeRideRepo struct {
	domain.RideRepository
	created *domain.Ride
}

func (f *fakeRideRepo) CreateRide(ctx context.Context, r *domain.Ride) (string, error) {
	f.created = r
	return "ride-1", nil
}

type fakeOutboxRepo struct {
	domain.OutboxRepository
	inserted []*domain.OutboxMessage
}

func (f *fakeOutboxRepo) Insert(ctx context.Context, m *domain.OutboxMessage) error {
	f.inserted = append(f.inserted, m)
	return nil
}

type fakeTx struct{}

func (fakeTx) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func TestCreateRide_InsertsRideAndOutboxRowAtomically(t *testing.T) {
	rideRepo := &fakeRideRepo{}
	outbox := &fakeOutboxRepo{}
	h := &CreateRideHandler{repo: rideRepo, outboxRepo: outbox, transaction: fakeTx{}}

	result, err := h.Handle(context.Background(), CreateRide{
		ClientID: "u1", PickupLat: 1, PickupLng: 2, PickupAddress: "A",
		DestLat: 3, DestLng: 4, DestAddress: "B",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RideID != "ride-1" || result.Status != "Requested" {
		t.Fatalf("bad result: %+v", result)
	}
	if rideRepo.created == nil || rideRepo.created.ClientID != "u1" {
		t.Fatalf("ride not created correctly")
	}

	if len(outbox.inserted) != 1 {
		t.Fatalf("expected 1 outbox message, got %d", len(outbox.inserted))
	}
	if outbox.inserted[0].Topic != "ride.requested" || outbox.inserted[0].EventType != "RideRequested" {
		t.Fatalf("bad outbox envelope: %+v", outbox.inserted[0])
	}

	var ev contractsKafka.RideRequestedEvent
	if err := json.Unmarshal(outbox.inserted[0].Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.RideID != "ride-1" || ev.ClientID != "u1" || ev.DistanceKm != 10.0 || ev.Price != 10.0 {
		t.Fatalf("bad event payload: %+v", ev)
	}
}
