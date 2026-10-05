package command

import (
	"context"
	"io"
	"testing"

	"billing-service/internal/domain"
	"billing-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

type fakeInvoiceRepoForActuals struct {
	domain.InvoiceRepository
	recorded  bool
	rideID    string
	distanceM int64
	durationS int
	err       error
}

func (f *fakeInvoiceRepoForActuals) RecordActuals(ctx context.Context, rideID string, distanceM int64, durationS int) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	f.rideID, f.distanceM, f.durationS = rideID, distanceM, durationS
	return f.recorded, nil
}

func TestRecordRideActualsHandler_ForwardsToRepo(t *testing.T) {
	repo := &fakeInvoiceRepoForActuals{recorded: true}
	h := &RecordRideActualsHandler{invoiceRepo: repo, metrics: metrics.NewNoopMetricsClient(), logger: testLogger()}

	err := h.Handle(context.Background(), RecordRideActuals{RideID: "ride-1", DistanceM: 1500, DurationS: 600})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.rideID != "ride-1" || repo.distanceM != 1500 || repo.durationS != 600 {
		t.Fatalf("got rideID=%s distanceM=%d durationS=%d, want ride-1/1500/600", repo.rideID, repo.distanceM, repo.durationS)
	}
}

// TestRecordRideActualsHandler_NoInvoiceIsNotAnError guards the race
// between ride.completed (invoice creation) and ride.summary.ready — no
// cross-topic ordering guarantee means the invoice may not exist yet, or
// ever (a pre-fee cancellation). Returning an error would retry in place
// and block this consumer's partition forever for such a ride.
func TestRecordRideActualsHandler_NoInvoiceIsNotAnError(t *testing.T) {
	repo := &fakeInvoiceRepoForActuals{recorded: false}
	h := &RecordRideActualsHandler{invoiceRepo: repo, metrics: metrics.NewNoopMetricsClient(), logger: testLogger()}

	err := h.Handle(context.Background(), RecordRideActuals{RideID: "ride-1", DistanceM: 1500, DurationS: 600})
	if err != nil {
		t.Fatalf("expected no-op success, got error: %v", err)
	}
}
