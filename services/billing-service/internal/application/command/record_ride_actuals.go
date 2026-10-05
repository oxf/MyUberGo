package command

import (
	"context"

	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"billing-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

// RecordRideActuals stores the actual distance/duration location-service
// measured for a ride (from ride.summary.ready) alongside the quoted price
// already on the invoice — record only, never re-price
// (LOCATION_SPEC.md §2.4).
type RecordRideActuals struct {
	RideID    string
	DistanceM int64
	DurationS int
}

type RecordRideActualsHandler struct {
	invoiceRepo domain.InvoiceRepository
	logger      *logrus.Entry
	metrics     decorator.MetricsClient
}

func NewRecordRideActualsHandler(
	invoiceRepo domain.InvoiceRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[RecordRideActuals] {
	if invoiceRepo == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &RecordRideActualsHandler{invoiceRepo: invoiceRepo, logger: logger, metrics: metricsClient}
	return decorator.ApplyCommandDecoratorsNoResult[RecordRideActuals](handler, logger, metricsClient)
}

// Handle is a no-op, not an error, when no invoice exists for this ride yet
// (or ever, e.g. a pre-fee cancellation) — ride.completed/ride.summary.ready
// arrive on separate topics with no cross-topic ordering guarantee, so this
// race is expected, not exceptional. Returning an error here would retry in
// place and block the consumer's partition forever for any ride that never
// gets an invoice.
func (h *RecordRideActualsHandler) Handle(ctx context.Context, cmd RecordRideActuals) error {
	recorded, err := h.invoiceRepo.RecordActuals(ctx, cmd.RideID, cmd.DistanceM, cmd.DurationS)
	if err != nil {
		return err
	}
	if !recorded {
		h.metrics.IncCounter(ctx, "myubergo.billing.summary_no_invoice")
		h.logger.WithField("ride_id", cmd.RideID).Info("no invoice found for ride summary, skipping")
		return nil
	}
	return nil
}
