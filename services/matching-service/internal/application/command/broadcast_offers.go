package command

import (
	"context"
	"errors"
	"time"

	"matching-service/internal/common/decorator"
	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"

	"github.com/sirupsen/logrus"
)

// Simplified matching parameters (see README "Matching Algorithm" for the
// target design; geo radius is replaced by a widening rating-ranked pool
// until the Location service exists).
const (
	BroadcastSize       = 5
	PoolWidthPerAttempt = 5
	MaxAttempts         = 5
	OfferTTL            = 30 * time.Second
	RateLimitPerMinute  = 3
	AcceptClaimTTL      = time.Hour
)

// BroadcastOffers runs one BROADCAST round for a searching ride: pick the
// top-BroadcastSize eligible drivers from a pool that widens with each
// attempt, record offers in Redis, and (re)arm the retry deadline.
type BroadcastOffers struct {
	RideID  string
	Attempt int
}

type BroadcastOffersHandler struct {
	rides   domain.RideRepository
	drivers domain.DriverRepository
	offers  domain.OfferRepository
	logger  *logrus.Entry
}

func NewBroadcastOffersHandler(
	rides domain.RideRepository,
	drivers domain.DriverRepository,
	offers domain.OfferRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[BroadcastOffers] {
	if rides == nil || drivers == nil || offers == nil {
		panic("nil repo")
	}
	handler := &BroadcastOffersHandler{rides: rides, drivers: drivers, offers: offers, logger: logger}
	return decorator.ApplyCommandDecoratorsNoResult[BroadcastOffers](handler, logger, metricsClient)
}

func (h *BroadcastOffersHandler) Handle(ctx context.Context, cmd BroadcastOffers) error {
	ride, err := h.rides.GetRide(ctx, cmd.RideID)
	if errors.Is(err, cmnerrors.ErrNotFound) {
		return h.offers.DeletePending(ctx, cmd.RideID)
	}
	if err != nil {
		return err
	}
	if ride.Status != domain.RideStatusSearching {
		return h.offers.DeletePending(ctx, cmd.RideID)
	}

	if cmd.Attempt > MaxAttempts {
		// Target design would notify the client here; no Notification
		// service exists yet, so failing the ride + logging is the whole story.
		if err := h.rides.MarkFailed(ctx, cmd.RideID); err != nil {
			return err
		}
		h.log().Warnf("giving up on ride %s after %d attempts", cmd.RideID, MaxAttempts)
		return h.offers.DeletePending(ctx, cmd.RideID)
	}

	candidates, err := h.drivers.TopOnlineDrivers(ctx, cmd.Attempt*PoolWidthPerAttempt)
	if err != nil {
		return err
	}
	alreadyOffered, err := h.offers.OfferedDrivers(ctx, cmd.RideID)
	if err != nil {
		return err
	}

	excluded := map[string]bool{}
	for _, c := range candidates {
		if alreadyOffered[c.DriverID] {
			continue
		}
		busy, err := h.offers.HasCurrentOffer(ctx, c.DriverID)
		if err != nil {
			return err
		}
		if busy {
			excluded[c.DriverID] = true
			continue
		}
		n, err := h.offers.OfferCount(ctx, c.DriverID)
		if err != nil {
			return err
		}
		if n >= RateLimitPerMinute {
			excluded[c.DriverID] = true
		}
	}

	targets := domain.SelectOfferTargets(candidates, alreadyOffered, excluded, BroadcastSize)

	offered := 0
	for _, t := range targets {
		ok, err := h.offers.TryOffer(ctx, cmd.RideID, t.DriverID, OfferTTL)
		if err != nil {
			return err
		}
		if ok {
			offered++
			if err := h.offers.IncrOfferCount(ctx, t.DriverID); err != nil {
				return err
			}
		}
	}

	h.log().Infof("ride %s attempt %d: offered to %d driver(s) (pool %d, excluded %d)",
		cmd.RideID, cmd.Attempt, offered, len(candidates), len(excluded))

	// Arm (or re-arm) the retry deadline even when nobody was offered —
	// drivers may come online before the next sweep.
	return h.offers.SetPending(ctx, domain.PendingRide{
		RideID:   cmd.RideID,
		Attempt:  cmd.Attempt,
		Deadline: time.Now().Add(OfferTTL),
	})
}

func (h *BroadcastOffersHandler) log() *logrus.Entry {
	if h.logger != nil {
		return h.logger
	}
	return logrus.NewEntry(logrus.StandardLogger())
}
