package workers

import (
	"context"
	"time"

	"matching-service/internal/application/command"
	"matching-service/internal/common/decorator"
	"matching-service/internal/domain"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Gives the retry sweep its own span so ListPending/AcceptedBy/BroadcastOffers join one
// trace instead of each becoming a disconnected root, and the sweep's cost is visible.
var tracer = otel.Tracer("matching-service/match-retry-worker")

// MatchRetryWorker sweeps pending_ride:* on a ticker, re-broadcasting with a widened pool
// (attempt+1) for rides whose offer window lapsed — the stand-in for geo expanding-radius retry.
type MatchRetryWorker struct {
	offers    domain.OfferRepository
	broadcast decorator.CommandHandlerNoResult[command.BroadcastOffers]
	logger    *logrus.Entry
	interval  time.Duration
}

func NewMatchRetryWorker(
	offers domain.OfferRepository,
	broadcast decorator.CommandHandlerNoResult[command.BroadcastOffers],
	logger *logrus.Entry,
	interval time.Duration,
) *MatchRetryWorker {
	return &MatchRetryWorker{offers: offers, broadcast: broadcast, logger: logger, interval: interval}
}

func (w *MatchRetryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.logger.Info("match retry worker started")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("match retry worker stopped")
			return
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

func (w *MatchRetryWorker) sweep(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "match retry sweep")
	defer span.End()

	pending, err := w.offers.ListPending(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.WithError(err).Error("failed to list pending rides")
		return
	}
	span.SetAttributes(attribute.Int("matching.pending_count", len(pending)))

	now := time.Now()
	for _, p := range pending {
		w.retryOne(ctx, p, now)
	}
}

// retryOne is scoped to a single pending ride so its span ends via defer even on panic —
// a bare span.End() at the bottom of the sweep loop would never run in that case.
func (w *MatchRetryWorker) retryOne(ctx context.Context, p domain.PendingRide, now time.Time) {
	ctx, span := tracer.Start(ctx, "match retry candidate", trace.WithAttributes(
		attribute.String("matching.ride_id", p.RideID),
		attribute.Int("matching.attempt", p.Attempt),
	))
	defer span.End()

	acceptedBy, err := w.offers.AcceptedBy(ctx, p.RideID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.WithError(err).Errorf("failed to check accepted_by for ride %s", p.RideID)
		return
	}
	if acceptedBy != "" {
		// Matched between sweeps; the accept handler normally cleans this
		// up — this is just belt-and-braces.
		span.SetAttributes(attribute.String("matching.retry_outcome", "already_matched"))
		_ = w.offers.DeletePending(ctx, p.RideID)
		return
	}
	if now.Before(p.Deadline) {
		span.SetAttributes(attribute.String("matching.retry_outcome", "not_due"))
		return
	}
	span.SetAttributes(attribute.String("matching.retry_outcome", "rebroadcast"))
	if err := w.broadcast.Handle(ctx, command.BroadcastOffers{RideID: p.RideID, Attempt: p.Attempt + 1}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.WithError(err).Errorf("retry broadcast failed for ride %s", p.RideID)
	}
}
