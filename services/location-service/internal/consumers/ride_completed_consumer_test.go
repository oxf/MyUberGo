package consumers

import (
	"context"
	"testing"
	"time"

	"location-service/internal/domain"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideCompletedConsumer_ClosesOpenWindow(t *testing.T) {
	ctx := context.Background()
	rideID := nextID("ride")
	clientID := "c-" + rideID
	driverID := "d-" + rideID

	if err := testTracking.RecordRideRequested(ctx, rideID, clientID); err != nil {
		t.Fatalf("seed requested: %v", err)
	}
	if err := testTracking.RecordRideAccepted(ctx, rideID, driverID, time.Now().UTC()); err != nil {
		t.Fatalf("seed accepted: %v", err)
	}
	if _, ok, err := testTracking.Participants(ctx, rideID); err != nil || !ok {
		t.Fatalf("expected window open before completing, ok=%v err=%v", ok, err)
	}

	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      rideID,
		ClientID:    clientID,
		DriverID:    driverID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		_, ok, err := testTracking.Participants(ctx, rideID)
		return err == nil && !ok
	})
}

// TestRideCompletedConsumer_BuildsSummaryFromArchivedTrack proves the full
// path this consumer now drives: pings appended to the ride's track stream
// during the ride are turned into a persisted summary and an outbox row,
// before the tracking window (and the data BuildRideSummary reads from it)
// is closed.
func TestRideCompletedConsumer_BuildsSummaryFromArchivedTrack(t *testing.T) {
	ctx := context.Background()
	rideID := nextID("ride")
	clientID := "c-" + rideID
	driverID := "d-" + rideID

	if err := testTracking.RecordRideRequested(ctx, rideID, clientID); err != nil {
		t.Fatalf("seed requested: %v", err)
	}
	if err := testTracking.RecordRideAccepted(ctx, rideID, driverID, time.Now().UTC()); err != nil {
		t.Fatalf("seed accepted: %v", err)
	}

	base := time.Now().UTC()
	for i := 0; i < 3; i++ {
		pos := domain.Position{
			Coordinate: domain.Coordinate{Lat: 34.700 + float64(i)*0.001, Lon: 33.000},
			DeviceTs:   base.Add(time.Duration(i) * time.Second),
			ServerTs:   base.Add(time.Duration(i) * time.Second),
		}
		if err := testTracks.Append(ctx, rideID, domain.SubjectDriver, pos); err != nil {
			t.Fatalf("seed track entry %d: %v", i, err)
		}
	}

	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      rideID,
		ClientID:    clientID,
		DriverID:    driverID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  base.Add(10 * time.Minute).Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		_, ok := testSummaries.get(rideID)
		return ok
	})

	summary, _ := testSummaries.get(rideID)
	if summary.Source != domain.SourceSimplified {
		t.Fatalf("got source %v, want Simplified (no map matcher configured)", summary.Source)
	}
	if summary.PointCount != 3 {
		t.Fatalf("got point count %d, want 3", summary.PointCount)
	}
	if summary.ClientID != clientID || summary.DriverID != driverID {
		t.Fatalf("got %+v, want client=%s driver=%s", summary, clientID, driverID)
	}

	testOutbox.mu.Lock()
	found := false
	for _, m := range testOutbox.inserted {
		if m.Topic == "ride.summary.ready" {
			found = true
		}
	}
	testOutbox.mu.Unlock()
	if !found {
		t.Fatal("expected a ride.summary.ready outbox row")
	}
}

func TestRideCompletedConsumer_RedeliveryOfAlreadyClosedWindowIsSafe(t *testing.T) {
	rideID := nextID("ride")

	// No window was ever opened for this ride — completing it must still be
	// a safe no-op (idempotent against redelivery, per SocketCloser's doc).
	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      rideID,
		ClientID:    "c-" + rideID,
		DriverID:    "d-" + rideID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  time.Now().UTC().Format(time.RFC3339),
	})

	// No positive event to wait on — a beat, then confirm no poison-loop via a second event.
	time.Sleep(2 * time.Second)

	probeRide := nextID("ride")
	produce(t, rideRequestedTestTopic, contractsKafka.RideRequestedEvent{RideID: probeRide, ClientID: "probe"})
	waitFor(t, 20*time.Second, func() bool {
		got, err := testRedis.Get(context.Background(), "loc:ride:"+probeRide+":pending_client").Result()
		return err == nil && got == "probe"
	})
}
