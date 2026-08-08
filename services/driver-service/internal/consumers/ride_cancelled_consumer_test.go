package consumers

import (
	"context"
	"driver-service/internal/persistence"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideCancelledConsumer_FlipsOnRideToOnline(t *testing.T) {
	driverID := seedDriver(t, testDB, "OnRide")

	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      "ride-1",
		DriverID:    &driverID,
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	repo := persistence.NewPostgresDriverRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), driverID)
		return err == nil && d.Status == "Online"
	})
}

func TestRideCancelledConsumer_NilDriverIDIsNoOp(t *testing.T) {
	// A pre-match cancellation carries no DriverID — the consumer must drain
	// it without erroring, proven via a control event on the same topic.
	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      "ride-nil-driver",
		DriverID:    nil,
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	controlDriverID := seedDriver(t, testDB, "OnRide")
	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      "ride-2",
		DriverID:    &controlDriverID,
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	repo := persistence.NewPostgresDriverRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), controlDriverID)
		return err == nil && d.Status == "Online"
	})
}
