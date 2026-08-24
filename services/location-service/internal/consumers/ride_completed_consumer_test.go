package consumers

import (
	"context"
	"testing"
	"time"

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
