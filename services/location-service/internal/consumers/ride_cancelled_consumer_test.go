package consumers

import (
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideCancelledConsumer_ClosesOpenWindow(t *testing.T) {
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

	driverIDPtr := driverID
	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      rideID,
		ClientID:    clientID,
		DriverID:    &driverIDPtr,
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		_, ok, err := testTracking.Participants(ctx, rideID)
		return err == nil && !ok
	})
}

func TestRideCancelledConsumer_PreMatchCancelIsSafeNoop(t *testing.T) {
	// A ride cancelled before ever being matched never had a window open —
	// CloseWindow must be a safe no-op, not an error.
	rideID := nextID("ride")
	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      rideID,
		ClientID:    "c-" + rideID,
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	time.Sleep(2 * time.Second)

	probeRide := nextID("ride")
	produce(t, rideRequestedTestTopic, contractsKafka.RideRequestedEvent{RideID: probeRide, ClientID: "probe"})
	waitFor(t, 20*time.Second, func() bool {
		got, err := testRedis.Get(context.Background(), "loc:ride:"+probeRide+":pending_client").Result()
		return err == nil && got == "probe"
	})
}
