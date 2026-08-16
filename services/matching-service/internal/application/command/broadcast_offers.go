package command

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"matching-service/internal/application/services"
	"matching-service/internal/common/decorator"
	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"
	"matching-service/internal/infrastructure/metrics"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

const (
	BroadcastSize       = 5
	PoolWidthPerAttempt = 5
	MaxAttempts         = 5
	OfferTTL            = 30 * time.Second
	RateLimitPerMinute  = 3
	AcceptClaimTTL      = time.Hour

	// LocationOversampleFactor cushions the geo-discovery query: location-service returns
	// geographic candidates only (no availability filter), so an un-cushioned attempt-1 query
	// for exactly PoolWidthPerAttempt drivers can be starved down to far fewer once intersected
	// against drivers:online. The rating-only fallback pool doesn't need this — TopOnlineDrivers
	// already returns only online drivers (docs/AUDIT_2026-08-15.md #3).
	LocationOversampleFactor = 5

	// baseRadiusKm/radiusStepKm/maxRadiusKm implement the radius-expanding retry (5→7→9→11→13km,
	// cap 15); the 15km cap is never reached since MaxAttempts=5, but is kept as documented behavior.
	baseRadiusKm = 5.0
	radiusStepKm = 2.0
	maxRadiusKm  = 15.0
)

func radiusForAttempt(attempt int) float64 {
	r := baseRadiusKm + float64(attempt-1)*radiusStepKm
	if r > maxRadiusKm {
		return maxRadiusKm
	}
	return r
}

// BroadcastOffers runs one BROADCAST round: pick top-BroadcastSize eligible drivers from
// a widening pool, record offers in Redis, and (re)arm the retry deadline.
type BroadcastOffers struct {
	RideID  string
	Attempt int
}

type BroadcastOffersHandler struct {
	rides     domain.RideRepository
	drivers   domain.DriverRepository
	offers    domain.OfferRepository
	location  services.LocationClient
	publisher services.EventPublisher
	logger    *logrus.Entry
	metrics   decorator.MetricsClient
}

// NewBroadcastOffersHandler takes location second-to-last (with publisher last) so a nil or
// erroring LocationClient are both handled by the same fallback path in selectCandidates
// (LOCATION_SPEC.md §2.2).
func NewBroadcastOffersHandler(
	rides domain.RideRepository,
	drivers domain.DriverRepository,
	offers domain.OfferRepository,
	location services.LocationClient,
	publisher services.EventPublisher,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[BroadcastOffers] {
	if rides == nil || drivers == nil || offers == nil || publisher == nil {
		panic("nil dependency")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &BroadcastOffersHandler{rides: rides, drivers: drivers, offers: offers, location: location, publisher: publisher, logger: logger, metrics: metricsClient}
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
		h.metrics.IncCounter(ctx, "myubergo.matching.rides_failed")
		h.publishMatchingFailed(ctx, cmd.RideID)
		h.metrics.RecordValue(ctx, "myubergo.matching.broadcast_rounds", float64(MaxAttempts))
		return h.offers.DeletePending(ctx, cmd.RideID)
	}

	candidates, err := h.selectCandidates(ctx, ride, cmd.Attempt)
	if err != nil {
		return err
	}
	alreadyOffered, err := h.offers.OfferedDrivers(ctx, cmd.RideID)
	if err != nil {
		return err
	}

	// Only not-yet-offered candidates need a busy/rate check — fetched in two pipelined
	// round-trips (EXISTS then GET per driver) instead of sequential per-candidate calls.
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
	rateLimited := 0
	for _, id := range toCheck {
		if busyByDriver[id] {
			excluded[id] = true
			continue
		}
		if countByDriver[id] >= RateLimitPerMinute {
			excluded[id] = true
			rateLimited++
		}
	}
	// IncCounter once per rate-limited driver, not once per round — a round-level
	// increment used to understate this by up to PoolWidthPerAttempt×.
	for i := 0; i < rateLimited; i++ {
		h.metrics.IncCounter(ctx, "myubergo.matching.rate_limited")
	}

	targets := domain.SelectOfferTargets(candidates, alreadyOffered, excluded, BroadcastSize)

	// Each target's try-offer-then-increment sequence is independent of the others',
	// so fan them out instead of running up to 5 sequential Redis round-trips.
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

	h.metrics.IncCounter(ctx, "myubergo.matching.offers_broadcast",
		attribute.Int("attempt", cmd.Attempt),
	)

	// Arm (or re-arm) the retry deadline even when nobody was offered —
	// drivers may come online before the next sweep.
	return h.offers.SetPending(ctx, domain.PendingRide{
		RideID:   cmd.RideID,
		Attempt:  cmd.Attempt,
		Deadline: time.Now().Add(OfferTTL),
	})
}

// selectCandidates tries geo discovery first, then intersects with availability (Location
// doesn't know shift state); any failure falls back to the rating-only pool (LOCATION_SPEC.md §2.2).
func (h *BroadcastOffersHandler) selectCandidates(ctx context.Context, ride *domain.Ride, attempt int) ([]domain.Candidate, error) {
	limit := attempt * PoolWidthPerAttempt

	if h.location != nil {
		locationLimit := limit * LocationOversampleFactor
		if candidates, ok := h.tryLocationDiscovery(ctx, ride, attempt, locationLimit); ok {
			return candidates, nil
		}
		h.metrics.IncCounter(ctx, "myubergo.matching.location_fallbacks")
	}

	candidates, err := h.drivers.TopOnlineDrivers(ctx, limit)
	if err != nil {
		return nil, err
	}
	return domain.RankCandidates(candidates, false), nil
}

// tryLocationDiscovery is the geo-first path. ok is false on any failure,
// signalling the caller to fall back — never treated as a ride failure.
func (h *BroadcastOffersHandler) tryLocationDiscovery(ctx context.Context, ride *domain.Ride, attempt, limit int) ([]domain.Candidate, bool) {
	nearby, err := h.location.Nearby(ctx, ride.PickupLat, ride.PickupLng, radiusForAttempt(attempt), limit)
	if err != nil {
		h.log().WithError(err).Warn("location discovery unavailable, falling back to rating-only pool")
		return nil, false
	}

	ids := make([]string, len(nearby))
	distanceByID := make(map[string]int64, len(nearby))
	for i, n := range nearby {
		ids[i] = n.DriverID
		distanceByID[n.DriverID] = n.DistanceM
	}

	ratings, err := h.drivers.OnlineRatings(ctx, ids)
	if err != nil {
		h.log().WithError(err).Warn("location discovery: online-ratings intersect failed, falling back to rating-only pool")
		return nil, false
	}

	var candidates []domain.Candidate
	for _, id := range ids {
		// Location has no availability data — rating 0 means "not in drivers:online" (same
		// convention as DriverRepository.Rating), the "intersect locally" half of §2.2's design.
		if rating := ratings[id]; rating != 0 {
			candidates = append(candidates, domain.Candidate{DriverID: id, Rating: rating, DistanceM: distanceByID[id]})
		}
	}
	return domain.RankCandidates(candidates, true), true
}

// publishMatchingFailed tells ride-service its Postgres row would otherwise stay
// 'Requested' forever with no signal matching ever gave up (docs/AUDIT_2026-08-15.md #11).
// Direct publish, no outbox — same at-most-once tradeoff as AcceptRideHandler's
// ride.accepted publish: Redis has no transaction to hide a dual write behind, and
// MarkFailed above is already durable regardless of whether this publish succeeds.
func (h *BroadcastOffersHandler) publishMatchingFailed(ctx context.Context, rideID string) {
	payload, err := json.Marshal(contracts.RideMatchingFailedEvent{RideID: rideID})
	if err != nil {
		h.log().WithError(err).Errorf("failed to marshal ride.matching_failed for ride %s", rideID)
		return
	}
	if err := h.publisher.Publish(ctx, "ride.matching_failed", payload); err != nil {
		h.log().WithError(err).Errorf("failed to publish ride.matching_failed for ride %s", rideID)
	}
}

func (h *BroadcastOffersHandler) log() *logrus.Entry {
	if h.logger != nil {
		return h.logger
	}
	return logrus.NewEntry(logrus.StandardLogger())
}
