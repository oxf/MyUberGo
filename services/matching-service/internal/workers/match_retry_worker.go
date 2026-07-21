package workers

import (
	"context"
	"time"

	"matching-service/internal/application/command"
	"matching-service/internal/common/decorator"
	"matching-service/internal/domain"

	"github.com/sirupsen/logrus"
)

// MatchRetryWorker sweeps pending_ride:* on a ticker. Rides whose offer
// window lapsed without an accept get another BroadcastOffers round with a
// widened pool (attempt+1); BroadcastOffersHandler itself gives up past
// MaxAttempts. This is the simplified stand-in for the README's
// expanding-radius retry — no geo data exists yet.
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
	pending, err := w.offers.ListPending(ctx)
	if err != nil {
		w.logger.WithError(err).Error("failed to list pending rides")
		return
	}

	now := time.Now()
	for _, p := range pending {
		acceptedBy, err := w.offers.AcceptedBy(ctx, p.RideID)
		if err != nil {
			w.logger.WithError(err).Errorf("failed to check accepted_by for ride %s", p.RideID)
			continue
		}
		if acceptedBy != "" {
			// Matched between sweeps; the accept handler normally cleans this
			// up — this is just belt-and-braces.
			_ = w.offers.DeletePending(ctx, p.RideID)
			continue
		}
		if now.Before(p.Deadline) {
			continue
		}
		if err := w.broadcast.Handle(ctx, command.BroadcastOffers{RideID: p.RideID, Attempt: p.Attempt + 1}); err != nil {
			w.logger.WithError(err).Errorf("retry broadcast failed for ride %s", p.RideID)
		}
	}
}
