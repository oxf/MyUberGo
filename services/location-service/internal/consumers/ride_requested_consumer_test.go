package consumers

import (
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideRequestedConsumer_CachesPendingClientBeforeAccept(t *testing.T) {
	rideID := nextID("ride")
	clientID := "c-" + rideID
	produce(t, rideRequestedTestTopic, contractsKafka.RideRequestedEvent{
		RideID:   rideID,
		ClientID: clientID,
	})

	waitFor(t, 20*time.Second, func() bool {
		got, err := testRedis.Get(context.Background(), "loc:ride:"+rideID+":pending_client").Result()
		return err == nil && got == clientID
	})

	if _, ok, err := testTracking.Participants(context.Background(), rideID); err != nil {
		t.Fatalf("participants: %v", err)
	} else if ok {
		t.Fatal("window shouldn't open on ride.requested alone")
	}
}
