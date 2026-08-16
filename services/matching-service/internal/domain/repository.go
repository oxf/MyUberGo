package domain

import (
	"context"
	"time"

	contracts "github.com/oxf/MyUber/contracts/kafka"
)

type RideRepository interface {
	SaveRide(ctx context.Context, event contracts.RideRequestedEvent) error
	// GetRide returns cmnerrors.ErrNotFound when the ride hash doesn't exist.
	GetRide(ctx context.Context, rideID string) (*Ride, error)
	MarkMatched(ctx context.Context, rideID, driverID string, driverRating float64) error
	MarkFailed(ctx context.Context, rideID string) error
	MarkCancelled(ctx context.Context, rideID string) error
}

type DriverRepository interface {
	UpsertDriver(ctx context.Context, event contracts.ShiftUpdatedEvent) error
	// TopOnlineDrivers returns up to limit candidates, best rating first.
	TopOnlineDrivers(ctx context.Context, limit int) ([]Candidate, error)
	RemoveOnline(ctx context.Context, driverID string) error
	// Rating returns a driver's cached score from drivers:online, or 0 if
	// they're not currently a member (e.g. already removed).
	Rating(ctx context.Context, driverID string) (float64, error)
	// AddOnline re-adds a driver to drivers:online, e.g. after a matched
	// ride they were reserved for gets cancelled.
	AddOnline(ctx context.Context, driverID string, rating float64) error
	// GetUserID returns the driver's auth.user(id), cached from ShiftUpdatedEvent.UserID.
	// Empty string (not an error) if the driver has never had a shift.updated cached.
	GetUserID(ctx context.Context, driverID string) (string, error)
	// OnlineRatings returns each id's rating from drivers:online (0 if not a member) — used to
	// intersect location-service's geo candidates against actual availability, which Location can't see.
	OnlineRatings(ctx context.Context, driverIDs []string) (map[string]float64, error)
}

type OfferRepository interface {
	OfferedDrivers(ctx context.Context, rideID string) (map[string]bool, error)
	// TryOffer atomically claims the driver's current_offer slot (SET NX EX);
	// on success it also records the driver in the ride's offered set.
	TryOffer(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error)
	// CurrentOffer returns ("", zero, nil) when the driver has no live offer.
	CurrentOffer(ctx context.Context, driverID string) (rideID string, expiresAt time.Time, err error)
	HasCurrentOffer(ctx context.Context, driverID string) (bool, error)
	// HasCurrentOffers is HasCurrentOffer for many drivers in one pipelined
	// round-trip — used by BroadcastOffersHandler's candidate-filtering loop.
	HasCurrentOffers(ctx context.Context, driverIDs []string) (map[string]bool, error)
	ClearCurrentOffer(ctx context.Context, driverID string) error
	// TryAccept is the atomic claim: SET ride:{id}:accepted_by NX. false = lost the race.
	TryAccept(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error)
	// AcceptedBy returns "" when nobody has claimed the ride.
	AcceptedBy(ctx context.Context, rideID string) (string, error)
	IsCancelled(ctx context.Context, rideID string) (bool, error)
	// SetCancelled marks the ride as cancelled so any in-flight AcceptRide
	// racing the cancellation is rejected by IsCancelled.
	SetCancelled(ctx context.Context, rideID string) error
	OfferCount(ctx context.Context, driverID string) (int64, error)
	// OfferCounts is OfferCount for many drivers in one pipelined round-trip
	// — used by BroadcastOffersHandler's candidate-filtering loop.
	OfferCounts(ctx context.Context, driverIDs []string) (map[string]int64, error)
	IncrOfferCount(ctx context.Context, driverID string) error
	SetPending(ctx context.Context, p PendingRide) error
	DeletePending(ctx context.Context, rideID string) error
	ListPending(ctx context.Context) ([]PendingRide, error)
}
