package consumers

import (
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestShiftUpdatedConsumer_CachesOwnerBothDirections(t *testing.T) {
	driverID := nextID("driver")
	userID := "u-" + driverID
	produce(t, shiftUpdatedTestTopic, contractsKafka.ShiftUpdatedEvent{
		ShiftID:   nextID("shift"),
		DriverID:  driverID,
		Status:    "Online",
		Rating:    4.5,
		UserID:    userID,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	waitFor(t, 20*time.Second, func() bool {
		got, err := testRedis.Get(context.Background(), "loc:driver:"+driverID+":owner").Result()
		return err == nil && got == userID
	})
	waitFor(t, 20*time.Second, func() bool {
		got, err := testRedis.Get(context.Background(), "loc:user:"+userID+":driver").Result()
		return err == nil && got == driverID
	})
}

func TestShiftUpdatedConsumer_EmptyUserIDIsNotCached(t *testing.T) {
	driverID := nextID("driver")
	produce(t, shiftUpdatedTestTopic, contractsKafka.ShiftUpdatedEvent{
		ShiftID:   nextID("shift"),
		DriverID:  driverID,
		Status:    "Online",
		Rating:    4.5,
		UserID:    "",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	// Give the consumer a beat to (not) process it, then assert the key was
	// never written — there's no positive event to wait on here.
	time.Sleep(2 * time.Second)
	exists, err := testRedis.Exists(context.Background(), "loc:driver:"+driverID+":owner").Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists != 0 {
		t.Fatalf("expected no owner key to be written for an empty userId")
	}
}
