package consumers

import (
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideAcceptedConsumer_CachesPendingDriverBeforeRequest(t *testing.T) {
	rideID := nextID("ride")
	driverID := "d-" + rideID
	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     rideID,
		DriverID:   driverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		got, err := testRedis.Get(context.Background(), "loc:ride:"+rideID+":pending_driver").Result()
		return err == nil && len(got) > 0
	})

	if _, ok, err := testTracking.Participants(context.Background(), rideID); err != nil {
		t.Fatalf("participants: %v", err)
	} else if ok {
		t.Fatal("window shouldn't open on ride.accepted alone")
	}
}

// TestTrackingWindow_CompletesRegardlessOfArrivalOrder is the ordering-
// independence proof at the live-consumer layer (see tracking_redis_repository_test.go for the repo layer).
func TestTrackingWindow_CompletesRegardlessOfArrivalOrder(t *testing.T) {
	t.Run("accepted before requested", func(t *testing.T) {
		rideID := nextID("ride")
		clientID := "c-" + rideID
		driverID := "d-" + rideID

		produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
			RideID:     rideID,
			DriverID:   driverID,
			AcceptedAt: time.Now().UTC().Format(time.RFC3339),
		})
		produce(t, rideRequestedTestTopic, contractsKafka.RideRequestedEvent{
			RideID:   rideID,
			ClientID: clientID,
		})

		waitFor(t, 20*time.Second, func() bool {
			p, ok, err := testTracking.Participants(context.Background(), rideID)
			return err == nil && ok && p.ClientID == clientID && p.DriverID == driverID
		})
	})

	t.Run("requested before accepted", func(t *testing.T) {
		rideID := nextID("ride")
		clientID := "c-" + rideID
		driverID := "d-" + rideID

		produce(t, rideRequestedTestTopic, contractsKafka.RideRequestedEvent{
			RideID:   rideID,
			ClientID: clientID,
		})
		produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
			RideID:     rideID,
			DriverID:   driverID,
			AcceptedAt: time.Now().UTC().Format(time.RFC3339),
		})

		waitFor(t, 20*time.Second, func() bool {
			p, ok, err := testTracking.Participants(context.Background(), rideID)
			return err == nil && ok && p.ClientID == clientID && p.DriverID == driverID
		})
	})
}
