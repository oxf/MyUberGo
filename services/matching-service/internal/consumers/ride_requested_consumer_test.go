package consumers

import (
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideRequestedConsumer_CreatesRideAndBroadcastsOffer(t *testing.T) {
	driverID := nextID("driver")
	produce(t, shiftUpdatedTestTopic, contractsKafka.ShiftUpdatedEvent{
		ShiftID:   nextID("shift"),
		DriverID:  driverID,
		Status:    "Online",
		Rating:    5.0,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		_, err := testRedis.ZScore(context.Background(), "drivers:online", driverID).Result()
		return err == nil
	})

	rideID := nextID("ride")
	produce(t, rideRequestedTestTopic, contractsKafka.RideRequestedEvent{
		RideID:     rideID,
		ClientID:   nextID("client"),
		DistanceKm: 5,
		PriceMinor: 1000,
		Currency:   "EUR",
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		status, err := testRedis.HGet(context.Background(), "ride:"+rideID, "status").Result()
		return err == nil && status == "searching"
	})
	waitFor(t, 20*time.Second, func() bool {
		offered, err := testRedis.Get(context.Background(), "driver:"+driverID+":current_offer").Result()
		return err == nil && offered == rideID
	})
}
