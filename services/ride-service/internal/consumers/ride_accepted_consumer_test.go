package consumers

import (
	"context"
	"ride-service/internal/persistence"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideAcceptedConsumer_MatchesRide(t *testing.T) {
	clientID := seedClient(t, testDB)
	driverID := seedDriver(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     rideID,
		DriverID:   driverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})

	rideRepo := persistence.NewPostgresRideRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), rideID)
		if err != nil {
			return false
		}
		return ride.Status == "Matched" && ride.DriverID != nil && *ride.DriverID == driverID
	})
}

func TestRideAcceptedConsumer_RedeliveryIsIdempotent(t *testing.T) {
	clientID := seedClient(t, testDB)
	driver1 := seedDriver(t, testDB)
	driver2 := seedDriver(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")

	rideRepo := persistence.NewPostgresRideRepository(testDB)

	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     rideID,
		DriverID:   driver1,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), rideID)
		return err == nil && ride.Status == "Matched" && ride.DriverID != nil && *ride.DriverID == driver1
	})

	// Simulate a redelivered ride.accepted naming a different driver — the
	// consumer must remain a no-op (repository guard: status='Requested'),
	// so the ride must still show driver1, never driver2.
	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     rideID,
		DriverID:   driver2,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})

	// There's no positive event to wait on for a no-op, so give the consumer
	// a beat to (not) process it, then assert the state never moved. Produce
	// a control event afterward (a different ride) and wait for it first, to
	// prove the consumer is actually still alive and draining the topic —
	// otherwise a silently-undelivered redelivery would pass for free.
	controlRideID := seedRide(t, testDB, clientID, "Requested")
	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     controlRideID,
		DriverID:   driver2,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), controlRideID)
		return err == nil && ride.Status == "Matched"
	})

	ride, err := rideRepo.GetRideByID(context.Background(), rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "Matched" || ride.DriverID == nil || *ride.DriverID != driver1 {
		t.Fatalf("expected ride to remain matched to driver1 after redelivery, got status=%s driverID=%v", ride.Status, ride.DriverID)
	}
}
