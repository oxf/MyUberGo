package command

import (
	"context"
	"testing"
	"time"

	"location-service/internal/domain"
)

type fakeTracking struct {
	domain.TrackingRepository
	requested      map[string]string // rideID -> clientID
	accepted       map[string]string // rideID -> driverID
	closed         []string
	requestedCalls int
	acceptedCalls  int
}

func newFakeTracking() *fakeTracking {
	return &fakeTracking{requested: map[string]string{}, accepted: map[string]string{}}
}

func (f *fakeTracking) RecordRideRequested(ctx context.Context, rideID, clientID string) error {
	f.requestedCalls++
	f.requested[rideID] = clientID
	return nil
}

func (f *fakeTracking) RecordRideAccepted(ctx context.Context, rideID, driverID string, acceptedAt time.Time) error {
	f.acceptedCalls++
	f.accepted[rideID] = driverID
	return nil
}

func (f *fakeTracking) CloseWindow(ctx context.Context, rideID string) error {
	f.closed = append(f.closed, rideID)
	return nil
}

func TestRecordRideRequestedHandler_ForwardsToRepo(t *testing.T) {
	tracking := newFakeTracking()
	h := &RecordRideRequestedHandler{tracking: tracking}

	if err := h.Handle(context.Background(), RecordRideRequested{RideID: "ride-1", ClientID: "client-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tracking.requested["ride-1"] != "client-1" {
		t.Fatalf("got %v, want ride-1 -> client-1", tracking.requested)
	}
}

func TestRecordRideRequestedHandler_MissingFieldsAreNoop(t *testing.T) {
	tracking := newFakeTracking()
	h := &RecordRideRequestedHandler{tracking: tracking}

	if err := h.Handle(context.Background(), RecordRideRequested{RideID: "", ClientID: "client-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tracking.requestedCalls != 0 {
		t.Fatalf("expected no repo call for an empty rideID, got %d calls", tracking.requestedCalls)
	}
}

func TestRecordRideAcceptedHandler_ForwardsToRepo(t *testing.T) {
	tracking := newFakeTracking()
	h := &RecordRideAcceptedHandler{tracking: tracking}

	if err := h.Handle(context.Background(), RecordRideAccepted{RideID: "ride-1", DriverID: "driver-1", AcceptedAt: time.Now()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tracking.accepted["ride-1"] != "driver-1" {
		t.Fatalf("got %v, want ride-1 -> driver-1", tracking.accepted)
	}
}

func TestRecordRideAcceptedHandler_MissingFieldsAreNoop(t *testing.T) {
	tracking := newFakeTracking()
	h := &RecordRideAcceptedHandler{tracking: tracking}

	if err := h.Handle(context.Background(), RecordRideAccepted{RideID: "ride-1", DriverID: ""}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tracking.acceptedCalls != 0 {
		t.Fatalf("expected no repo call for an empty driverID, got %d calls", tracking.acceptedCalls)
	}
}

func TestCloseTrackingWindowHandler_ForwardsToRepo(t *testing.T) {
	tracking := newFakeTracking()
	h := &CloseTrackingWindowHandler{tracking: tracking}

	if err := h.Handle(context.Background(), CloseTrackingWindow{RideID: "ride-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tracking.closed) != 1 || tracking.closed[0] != "ride-1" {
		t.Fatalf("got %v, want [ride-1]", tracking.closed)
	}
}

func TestCloseTrackingWindowHandler_MissingRideIDIsNoop(t *testing.T) {
	tracking := newFakeTracking()
	h := &CloseTrackingWindowHandler{tracking: tracking}

	if err := h.Handle(context.Background(), CloseTrackingWindow{RideID: ""}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tracking.closed) != 0 {
		t.Fatalf("expected no repo call for an empty rideID, got %v", tracking.closed)
	}
}
