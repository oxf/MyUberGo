package consumers

import (
	"billing-service/internal/domain"
	"billing-service/internal/persistence"
	"context"
	"testing"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
)

func TestRideCancelledConsumer_ZeroFeeCreatesNoInvoice(t *testing.T) {
	clientID := seedClient(t, testDB)
	zeroFeeRideID := seedRide(t, testDB, clientID, "")

	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      zeroFeeRideID,
		ClientID:    clientID,
		FeeMinor:    0,
		Currency:    "EUR",
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	// A zero-fee event has no positive side effect to wait on directly, so
	// prove the consumer is actually alive on this topic by also producing a
	// non-zero-fee event (a different ride) right behind it and waiting for
	// *that* invoice — otherwise a silently-undelivered first message would
	// make this test pass for the wrong reason.
	controlRideID := seedRide(t, testDB, clientID, "")
	invoiceRepo := persistence.NewPostgresInvoiceRepository(testDB)
	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      controlRideID,
		ClientID:    clientID,
		FeeMinor:    250,
		Currency:    "EUR",
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})
	waitFor(t, 20*time.Second, func() bool {
		inv, err := invoiceRepo.GetByRideID(context.Background(), controlRideID, domain.InvoiceTypeCancellationFee)
		return err == nil && inv.AmountMinor == 250
	})

	if got := countInvoicesForRide(t, zeroFeeRideID); got != 0 {
		t.Fatalf("expected no invoice for a zero-fee cancellation, got %d", got)
	}
}

func TestRideCancelledConsumer_NonZeroFeeCreatesCancellationInvoice(t *testing.T) {
	clientID := seedClient(t, testDB)
	driverID := seedDriver(t, testDB)
	rideID := seedRide(t, testDB, clientID, driverID)

	produce(t, rideCancelledTestTopic, contractsKafka.RideCancelledEvent{
		RideID:      rideID,
		ClientID:    clientID,
		DriverID:    &driverID,
		FeeMinor:    500,
		Currency:    "EUR",
		CancelledAt: time.Now().UTC().Format(time.RFC3339),
	})

	invoiceRepo := persistence.NewPostgresInvoiceRepository(testDB)
	waitFor(t, 20*time.Second, func() bool {
		inv, err := invoiceRepo.GetByRideID(context.Background(), rideID, domain.InvoiceTypeCancellationFee)
		return err == nil && inv.AmountMinor == 500
	})
}
