package consumers

import (
	"context"
	"ride-service/internal/persistence"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

// TestRideMatchingFailedConsumer_FailsRide guards docs/AUDIT_2026-08-15.md #11: without
// this consumer, a ride matching-service gave up on stayed 'Requested' in Postgres forever.
func TestRideMatchingFailedConsumer_FailsRide(t *testing.T) {
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	produce(t, rideMatchingFailedTestTopic, contractsKafka.RideMatchingFailedEvent{
		RideID: rideID,
	})

	rideRepo := persistence.NewPostgresRideRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), rideID)
		if err != nil {
			return false
		}
		return ride.Status == "Failed"
	})
}

func TestRideMatchingFailedConsumer_RedeliveryIsIdempotent(t *testing.T) {
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	rideRepo := persistence.NewPostgresRideRepository(testDB)

	produce(t, rideMatchingFailedTestTopic, contractsKafka.RideMatchingFailedEvent{RideID: rideID})
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), rideID)
		return err == nil && ride.Status == "Failed"
	})

	// Redelivering the same event must stay a no-op (guarded by "AND status = 'Requested'",
	// which no longer matches once the ride is Failed).
	produce(t, rideMatchingFailedTestTopic, contractsKafka.RideMatchingFailedEvent{RideID: rideID})

	// No positive event to wait on for a no-op, so a control event (different ride) is
	// produced and awaited afterward to prove the consumer is still alive and draining.
	controlRideID := seedRide(t, testDB, clientID, "Requested")
	produce(t, rideMatchingFailedTestTopic, contractsKafka.RideMatchingFailedEvent{RideID: controlRideID})
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), controlRideID)
		return err == nil && ride.Status == "Failed"
	})

	ride, err := rideRepo.GetRideByID(context.Background(), rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "Failed" {
		t.Fatalf("expected ride to remain Failed after redelivery, got status=%s", ride.Status)
	}
}
