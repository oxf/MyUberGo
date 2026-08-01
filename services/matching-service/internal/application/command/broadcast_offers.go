package command

import (
	"context"
	"errors"
	"sync"
	"time"

	"matching-service/internal/common/decorator"
	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

	// Candidates not already offered to are the only ones worth a busy/rate
	// check — fetched in two pipelined round-trips (one EXISTS per driver,
	// then one GET per still-eligible driver) instead of up to 2 sequential
	// Redis calls per candidate.
	var toCheck []string
	for _, c := range candidates {
		if !alreadyOffered[c.DriverID] {
			toCheck = append(toCheck, c.DriverID)
		}
	}
	busyByDriver, err := h.offers.HasCurrentOffers(ctx, toCheck)
	if err != nil {
		return err
	}

	var toRateCheck []string
	for _, id := range toCheck {
		if !busyByDriver[id] {
			toRateCheck = append(toRateCheck, id)
		}
	}
	countByDriver, err := h.offers.OfferCounts(ctx, toRateCheck)
	if err != nil {
		return err
	}

	excluded := map[string]bool{}
	for _, id := range toCheck {
		if busyByDriver[id] {
			excluded[id] = true
			continue
		}
		if countByDriver[id] >= RateLimitPerMinute {
			excluded[id] = true
		}
	}

	targets := domain.SelectOfferTargets(candidates, alreadyOffered, excluded, BroadcastSize)

	// Each target's try-offer-then-maybe-increment sequence is independent
	// of every other target's, so fan them out instead of running up to 5
	// targets' worth of sequential Redis round-trips one at a time.
	var offeredMu sync.Mutex
	offered := 0
	g, gCtx := errgroup.WithContext(ctx)
	for _, t := range targets {
		driverID := t.DriverID
		g.Go(func() error {
			ok, err := h.offers.TryOffer(gCtx, cmd.RideID, driverID, OfferTTL)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if err := h.offers.IncrOfferCount(gCtx, driverID); err != nil {
				return err
			}
			offeredMu.Lock()
			offered++
			offeredMu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
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
