package consumers

import (
	"billing-service/internal/domain"
	"billing-service/internal/persistence"
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideCompletedConsumer_CreatesInvoice(t *testing.T) {
	clientID := seedClient(t, testDB)
	driverID := seedDriver(t, testDB)
	rideID := seedRide(t, testDB, clientID, driverID)

	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID:      rideID,
		ClientID:    clientID,
		DriverID:    driverID,
		AmountMinor: 1000,
		Currency:    "EUR",
		FinishedAt:  time.Now().UTC().Format(time.RFC3339),
	})

	invoiceRepo := persistence.NewPostgresInvoiceRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		inv, err := invoiceRepo.GetByRideID(context.Background(), rideID, domain.InvoiceTypeRideFare)
		return err == nil && inv.AmountMinor == 1000
	})
}

func TestRideCompletedConsumer_RedeliveryIsIdempotent(t *testing.T) {
	clientID := seedClient(t, testDB)
	driverID := seedDriver(t, testDB)
	rideID := seedRide(t, testDB, clientID, driverID)

	event := contractsKafka.RideCompletedEvent{
		RideID: rideID, ClientID: clientID, DriverID: driverID,
		AmountMinor: 1000, Currency: "EUR", FinishedAt: time.Now().UTC().Format(time.RFC3339),
	}

	invoiceRepo := persistence.NewPostgresInvoiceRepository(testDB)
	produce(t, rideCompletedTestTopic, event)
	waitFor(t, 20*time.Second, func() bool {
		_, err := invoiceRepo.GetByRideID(context.Background(), rideID, domain.InvoiceTypeRideFare)
		return err == nil
	})

	// Redelivery of the same event must hit the UNIQUE(ride_id, type) guard
	// and be treated as a no-op — no second invoice, no error. A control
	// event afterward (a different ride) proves the consumer is still alive
	// and draining the topic, so a silently-undelivered redelivery can't
	// make this test pass for the wrong reason.
	produce(t, rideCompletedTestTopic, event)

	controlRideID := seedRide(t, testDB, clientID, driverID)
	produce(t, rideCompletedTestTopic, contractsKafka.RideCompletedEvent{
		RideID: controlRideID, ClientID: clientID, DriverID: driverID,
		AmountMinor: 1000, Currency: "EUR", FinishedAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		_, err := invoiceRepo.GetByRideID(context.Background(), controlRideID, domain.InvoiceTypeRideFare)
		return err == nil
	})

	if got := countInvoicesForRide(t, rideID); got != 1 {
		t.Fatalf("expected exactly 1 invoice for ride %s after redelivery, got %d", rideID, got)
	}
}
