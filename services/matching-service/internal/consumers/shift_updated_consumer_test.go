package consumers

import (
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestShiftUpdatedConsumer_OnlineAddsToPool(t *testing.T) {
	driverID := nextID("driver")
	produce(t, shiftUpdatedTestTopic, contractsKafka.ShiftUpdatedEvent{
		ShiftID:   nextID("shift"),
		DriverID:  driverID,
		Status:    "Online",
		Rating:    4.5,
		UserID:    "u-" + driverID,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		score, err := testRedis.ZScore(context.Background(), "drivers:online", driverID).Result()
		return err == nil && score == 4.5
	})

	waitFor(t, 20*time.Second, func() bool {
		userID, err := testRedis.HGet(context.Background(), "driver:"+driverID, "userId").Result()
		return err == nil && userID == "u-"+driverID
	})
}

func TestShiftUpdatedConsumer_EndedRemovesFromPool(t *testing.T) {
	driverID := nextID("driver")
	produce(t, shiftUpdatedTestTopic, contractsKafka.ShiftUpdatedEvent{
		ShiftID:   nextID("shift"),
		DriverID:  driverID,
		Status:    "Online",
		Rating:    4.5,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		_, err := testRedis.ZScore(context.Background(), "drivers:online", driverID).Result()
		return err == nil
	})

	produce(t, shiftUpdatedTestTopic, contractsKafka.ShiftUpdatedEvent{
		ShiftID:   nextID("shift"),
		DriverID:  driverID,
		Status:    "Ended",
		Rating:    4.5,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		_, err := testRedis.ZScore(context.Background(), "drivers:online", driverID).Result()
		return err != nil // redis.Nil once removed from the ZSET
	})
}
