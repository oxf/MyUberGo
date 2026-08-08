package consumers

import (
	"context"
	"driver-service/internal/persistence"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideAcceptedConsumer_FlipsOnlineToOnRide(t *testing.T) {
	driverID := seedDriver(t, testDB, "Online")

	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     "ride-1",
		DriverID:   driverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})

	repo := persistence.NewPostgresDriverRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), driverID)
		return err == nil && d.Status == "OnRide"
	})
}

func TestRideAcceptedConsumer_RedeliveryIsIdempotent(t *testing.T) {
	driverID := seedDriver(t, testDB, "Online")
	repo := persistence.NewPostgresDriverRepository(testDB)

	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     "ride-1",
		DriverID:   driverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), driverID)
		return err == nil && d.Status == "OnRide"
	})

	// Redelivered ride.accepted for the same driver — the guarded
	// Online->OnRide transition must no-op, not error, since the driver is
	// no longer Online.
	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     "ride-1",
		DriverID:   driverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})

	// No positive event to wait on for a no-op, so a control event (different
	// driver) proves the consumer is still alive and draining afterward.
	controlDriverID := seedDriver(t, testDB, "Online")
	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     "ride-2",
		DriverID:   controlDriverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		d, err := repo.GetDriverByID(context.Background(), controlDriverID)
		return err == nil && d.Status == "OnRide"
	})

	d, err := repo.GetDriverByID(context.Background(), driverID)
	if err != nil {
		t.Fatalf("GetDriverByID: %v", err)
	}
	if d.Status != "OnRide" {
		t.Fatalf("expected driver to remain OnRide after redelivery, got status=%s", d.Status)
	}
}
