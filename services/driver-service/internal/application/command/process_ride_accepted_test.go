package command

import (
	"context"
	"testing"

	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type fakeDriverStatusRepo struct {
	domain.DriverProfileRepository
	changed bool
	err     error
	calls   []struct{ ID, From, To string }
}

func (f *fakeDriverStatusRepo) UpdateDriverStatus(ctx context.Context, id, fromStatus, toStatus string) (bool, error) {
	f.calls = append(f.calls, struct{ ID, From, To string }{id, fromStatus, toStatus})
	return f.changed, f.err
}

func testLogger() *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}

func TestProcessRideAccepted_FlipsOnlineToOnRide(t *testing.T) {
	repo := &fakeDriverStatusRepo{changed: true}
	h := &ProcessRideAcceptedHandler{profileRepo: repo, transaction: fakeTx{}, logger: testLogger()}

	err := h.Handle(context.Background(), ProcessRideAccepted{
		RideID:     "ride-1",
		DriverID:   "driver-1",
		AcceptedAt: "2026-07-21T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(repo.calls) != 1 || repo.calls[0].ID != "driver-1" || repo.calls[0].From != "Online" || repo.calls[0].To != "OnRide" {
		t.Fatalf("unexpected UpdateDriverStatus call(s): %+v", repo.calls)
	}
}

func TestProcessRideAccepted_GuardMissIsNotAnError(t *testing.T) {
	repo := &fakeDriverStatusRepo{changed: false}
	h := &ProcessRideAcceptedHandler{profileRepo: repo, transaction: fakeTx{}, logger: testLogger()}

	err := h.Handle(context.Background(), ProcessRideAccepted{RideID: "ride-1", DriverID: "driver-1"})
	if err != nil {
		t.Fatalf("expected nil error on guard miss, got %v", err)
	}
}
