package consumers

import (
	"context"
	"driver-service/internal/persistence"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideCompletedConsumer_FlipsOnRideToOnlineAndIncrements(t *testing.T) {
	driverID := seedDriver(t, testDB, "OnRide")

	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      "ride-1",
		DriverID:    driverID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  time.Now().UTC().Format(time.RFC3339),
	})

	repo := persistence.NewPostgresDriverRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), driverID)
		return err == nil && d.Status == "Online" && d.TotalRidesCompleted == 1
	})
}

func TestRideCompletedConsumer_RedeliverySkipsDoubleIncrement(t *testing.T) {
	driverID := seedDriver(t, testDB, "OnRide")
	repo := persistence.NewPostgresDriverRepository(testDB)

	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      "ride-1",
		DriverID:    driverID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), driverID)
		return err == nil && d.Status == "Online" && d.TotalRidesCompleted == 1
	})

	// Redelivered ride.completed for the same driver — the guarded
	// OnRide->Online transition no-ops, so the increment must be skipped too.
	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      "ride-1",
		DriverID:    driverID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  time.Now().UTC().Format(time.RFC3339),
	})

	controlDriverID := seedDriver(t, testDB, "OnRide")
	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      "ride-2",
		DriverID:    controlDriverID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), controlDriverID)
		return err == nil && d.Status == "Online"
	})

	d, err := repo.GetDriverByID(context.Background(), driverID)
	if err != nil {
		t.Fatalf("GetDriverByID: %v", err)
	}
	if d.TotalRidesCompleted != 1 {
		t.Fatalf("expected exactly one increment despite redelivery, got %d", d.TotalRidesCompleted)
	}
}
