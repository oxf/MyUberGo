package command

import (
	"context"
	"ride-service/internal/domain"
	"ride-service/internal/infrastructure/metrics"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
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
	h := &MarkRideMatchedHandler{repo: repo, metrics: metrics.NewNoopMetricsClient()}

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
	h := &MarkRideMatchedHandler{repo: repo, metrics: metrics.NewNoopMetricsClient()}

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

type recordingMetricsClient struct {
	metrics.NoopMetricsClient
	recordedValues map[string]float64
}

func (m *recordingMetricsClient) RecordValue(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	if m.recordedValues == nil {
		m.recordedValues = map[string]float64{}
	}
	m.recordedValues[name] = value
}

// TestMarkRideMatched_UsesRequestedAtFromEventNotARepoRead proves the SLI is
// computed from cmd.RequestedAt alone — repo is a bare RideRepository (nil,
// via embedding) with only MarkRideMatched overridden, so a call to
// GetRideByID would nil-panic instead of silently passing.
func TestMarkRideMatched_UsesRequestedAtFromEventNotARepoRead(t *testing.T) {
	repo := &fakeMarkMatchedRepo{}
	fakeMetrics := &recordingMetricsClient{}
	h := &MarkRideMatchedHandler{repo: repo, metrics: fakeMetrics}

	if err := h.Handle(context.Background(), MarkRideMatched{
		RideID:      "ride-1",
		DriverID:    "driver-1",
		AcceptedAt:  "2026-07-21T10:05:00Z",
		RequestedAt: "2026-07-21T10:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}

	got, ok := fakeMetrics.recordedValues["myubergo.ride.time_to_match"]
	if !ok {
		t.Fatal("expected myubergo.ride.time_to_match to be recorded")
	}
	if got != 300 {
		t.Fatalf("expected time_to_match of 300s, got %v", got)
	}
}
