package consumers

import (
	"context"
	"ride-service/internal/persistence"
	"testing"
	"time"

	"github.com/google/uuid"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestPaymentCompletedConsumer_SetsBillID(t *testing.T) {
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Completed")
	invoiceID := uuid.NewString()

	produce(t, paymentCompletedTestTopic, contractsKafka.PaymentCompletedEvent{
		RideID:    rideID,
		InvoiceID: invoiceID,
		ClientID:  clientID,
		Currency:  "EUR",
	})

	rideRepo := persistence.NewPostgresRideRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), rideID)
		if err != nil {
			return false
		}
		return ride.BillID != nil && *ride.BillID == invoiceID
	})
}

func TestPaymentCompletedConsumer_RedeliveryIsIdempotent(t *testing.T) {
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "Completed")
	firstInvoiceID := uuid.NewString()
	secondInvoiceID := uuid.NewString()

	rideRepo := persistence.NewPostgresRideRepository(testDB)

	produce(t, paymentCompletedTestTopic, contractsKafka.PaymentCompletedEvent{RideID: rideID, InvoiceID: firstInvoiceID, ClientID: clientID, Currency: "EUR"})
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), rideID)
		return err == nil && ride.BillID != nil && *ride.BillID == firstInvoiceID
	})

	// Redelivery with a different invoice id must be a no-op (repository
	// guard: bill_id IS NULL) — bill_id must stay pinned to the first one.
	// A control event afterward (a different ride) proves the consumer is
	// still alive and draining the topic, so a silently-undelivered
	// redelivery can't make this test pass for the wrong reason.
	produce(t, paymentCompletedTestTopic, contractsKafka.PaymentCompletedEvent{RideID: rideID, InvoiceID: secondInvoiceID, ClientID: clientID, Currency: "EUR"})

	controlRideID := seedRide(t, testDB, clientID, "Completed")
	controlInvoiceID := uuid.NewString()
	produce(t, paymentCompletedTestTopic, contractsKafka.PaymentCompletedEvent{RideID: controlRideID, InvoiceID: controlInvoiceID, ClientID: clientID, Currency: "EUR"})
	waitFor(t, 20*time.Second, func() bool {
		ride, err := rideRepo.GetRideByID(context.Background(), controlRideID)
		return err == nil && ride.BillID != nil && *ride.BillID == controlInvoiceID
	})

	ride, err := rideRepo.GetRideByID(context.Background(), rideID)
	if err != nil {
		t.Fatalf("GetRideByID: %v", err)
	}
	if ride.BillID == nil || *ride.BillID != firstInvoiceID {
		t.Fatalf("expected bill_id to remain %s after redelivery, got %v", firstInvoiceID, ride.BillID)
	}
}
