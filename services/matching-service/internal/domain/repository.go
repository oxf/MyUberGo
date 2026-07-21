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
	MarkMatched(ctx context.Context, rideID, driverID string) error
	MarkFailed(ctx context.Context, rideID string) error
}

type DriverRepository interface {
	UpsertDriver(ctx context.Context, event contracts.ShiftUpdatedEvent) error
	// TopOnlineDrivers returns up to limit candidates, best rating first.
	TopOnlineDrivers(ctx context.Context, limit int) ([]Candidate, error)
	RemoveOnline(ctx context.Context, driverID string) error
}

type OfferRepository interface {
	OfferedDrivers(ctx context.Context, rideID string) (map[string]bool, error)
	// TryOffer atomically claims the driver's current_offer slot (SET NX EX);
	// on success it also records the driver in the ride's offered set.
	TryOffer(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error)
	// CurrentOffer returns ("", zero, nil) when the driver has no live offer.
	CurrentOffer(ctx context.Context, driverID string) (rideID string, expiresAt time.Time, err error)
	HasCurrentOffer(ctx context.Context, driverID string) (bool, error)
	ClearCurrentOffer(ctx context.Context, driverID string) error
	// TryAccept is the atomic claim: SET ride:{id}:accepted_by NX. false = lost the race.
	TryAccept(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error)
	// AcceptedBy returns "" when nobody has claimed the ride.
	AcceptedBy(ctx context.Context, rideID string) (string, error)
	IsCancelled(ctx context.Context, rideID string) (bool, error)
	OfferCount(ctx context.Context, driverID string) (int64, error)
	IncrOfferCount(ctx context.Context, driverID string) error
	SetPending(ctx context.Context, p PendingRide) error
	DeletePending(ctx context.Context, rideID string) error
	ListPending(ctx context.Context) ([]PendingRide, error)
}
