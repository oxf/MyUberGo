package consumers

import (
	"context"
	"fmt"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"
	"ride-service/internal/infrastructure/metrics"
	"ride-service/internal/persistence"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/google/uuid"
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

	// Simulate a redelivered ride.accepted naming a different driver — the guarded
	// status='Requested' repository update must no-op, so the ride still shows driver1.
	produce(t, rideAcceptedTestTopic, contractsKafka.RideAcceptedEvent{
		RideID:     rideID,
		DriverID:   driver2,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})

	// No positive event to wait on for a no-op, so a control event (different ride) is
	// produced and awaited afterward to prove the consumer is still alive and draining.
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

// TestRideAcceptedConsumer_HandlerFailureIsRetriedUntilItSucceeds proves a genuine handler
// failure (an FK violation) is retried in place, not silently dropped like pre-fix auto-commit.
func TestRideAcceptedConsumer_HandlerFailureIsRetriedUntilItSucceeds(t *testing.T) {
	topic := fmt.Sprintf("ride.accepted.redelivery.test.%d", nextSeq())
	if err := createTopicNoTest(topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Requested")
	missingDriverID := uuid.NewString() // doesn't exist yet -> FK violation on every attempt until seeded

	rideRepo := persistence.NewPostgresRideRepository(testDB)
	application := app.Application{
		Commands: app.Commands{
			MarkRideMatched: command.NewMarkRideMatchedHandler(rideRepo, testLogger(), metrics.NewNoopMetricsClient()),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := NewRideAcceptedConsumer(application, kafkaBroker, testLogger())
	go consumer.Run(ctx, topic)

	produce(t, topic, contractsKafka.RideAcceptedEvent{
		RideID:     rideID,
		DriverID:   missingDriverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})

	// Give the consumer time for at least one failed attempt (retryBackoff is 2s)
	// against the still-missing driver.
	time.Sleep(3 * time.Second)
	ride, err := rideRepo.GetRideByID(context.Background(), rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.Status != "Requested" {
		t.Fatalf("expected ride to still be unmatched while the FK violation persists, got status=%s", ride.Status)
	}

	// Make the SAME message's driver id valid — the consumer, still retrying it in
	// place, must pick this up on its next attempt without any external trigger.
	seedDriverWithID(t, testDB, missingDriverID)

	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), rideID)
		return err == nil && ride.Status == "Matched" && ride.DriverID != nil && *ride.DriverID == missingDriverID
	})
}
