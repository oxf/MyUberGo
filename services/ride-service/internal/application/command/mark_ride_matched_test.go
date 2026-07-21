package command

import (
	"context"
	"ride-service/internal/domain"
	"testing"
	"time"
)

type fakeMarkMatchedRepo struct {
	domain.RideRepository
	rideID    string
	driverID  string
	matchedAt time.Time
}

func (f *fakeMarkMatchedRepo) MarkRideMatched(ctx context.Context, rideID, driverID string, matchedAt time.Time) error {
	f.rideID = rideID
	f.driverID = driverID
	f.matchedAt = matchedAt
	return nil
}

func TestMarkRideMatched_ParsesAcceptedAtAndForwardsToRepo(t *testing.T) {
	repo := &fakeMarkMatchedRepo{}
	h := &MarkRideMatchedHandler{repo: repo}

	acceptedAt := "2026-07-21T10:00:00Z"
	if err := h.Handle(context.Background(), MarkRideMatched{
		RideID:     "ride-1",
		DriverID:   "driver-1",
		AcceptedAt: acceptedAt,
	}); err != nil {
		t.Fatal(err)
	}

	if repo.rideID != "ride-1" || repo.driverID != "driver-1" {
		t.Fatalf("bad forwarded args: rideID=%s driverID=%s", repo.rideID, repo.driverID)
	}
	want, _ := time.Parse(time.RFC3339, acceptedAt)
	if !repo.matchedAt.Equal(want) {
		t.Fatalf("expected matchedAt %v, got %v", want, repo.matchedAt)
	}
}

func TestMarkRideMatched_FallsBackToNowOnUnparseableAcceptedAt(t *testing.T) {
	repo := &fakeMarkMatchedRepo{}
	h := &MarkRideMatchedHandler{repo: repo}

	before := time.Now().UTC()
	if err := h.Handle(context.Background(), MarkRideMatched{
		RideID:     "ride-1",
		DriverID:   "driver-1",
		AcceptedAt: "not-a-timestamp",
	}); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()

	if repo.matchedAt.Before(before) || repo.matchedAt.After(after) {
		t.Fatalf("expected matchedAt to fall back to now(), got %v (window %v-%v)", repo.matchedAt, before, after)
	}
}
