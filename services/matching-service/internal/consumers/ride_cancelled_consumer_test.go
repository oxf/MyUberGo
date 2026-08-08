package consumers

import (
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideCancelledConsumer_MarksRideCancelled(t *testing.T) {
	rideID := nextID("ride")
	ctx := context.Background()
	if err := testRedis.HSet(ctx, "ride:"+rideID, "status", "searching").Err(); err != nil {
		t.Fatalf("seed ride hash: %v", err)
	}

	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      rideID,
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		status, err := testRedis.HGet(ctx, "ride:"+rideID, "status").Result()
		return err == nil && status == "cancelled"
	})
}

func TestRideCancelledConsumer_RestoresMatchedDriverToPool(t *testing.T) {
	ctx := context.Background()
	rideID := nextID("ride")
	driverID := nextID("driver")

	if err := testRedis.HSet(ctx, "ride:"+rideID, map[string]any{
		"status":       "matched",
		"driverId":     driverID,
		"driverRating": 4.2,
	}).Err(); err != nil {
		t.Fatalf("seed ride hash: %v", err)
	}
	if err := testRedis.SetNX(ctx, "ride:"+rideID+":accepted_by", driverID, time.Hour).Err(); err != nil {
		t.Fatalf("seed accepted_by: %v", err)
	}

	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      rideID,
		DriverID:    &driverID,
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		score, err := testRedis.ZScore(ctx, "drivers:online", driverID).Result()
		return err == nil && score == 4.2
	})
}
