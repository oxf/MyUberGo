package domain

import (
	"context"
	"time"
)

type RideRepository interface {
	CreateRide(ctx context.Context, r *Ride) (string, error)
	GetRideList(ctx context.Context, req PageRequest) ([]*Ride, error)
	CountRides(ctx context.Context) (int, error)
	GetRideByID(ctx context.Context, id string) (*Ride, error)
	MarkRideMatched(ctx context.Context, rideID, driverID string, matchedAt time.Time) error
	// GetRideForUpdate locks the ride row (SELECT ... FOR UPDATE) so its
	// status/ownership can be checked and then mutated within the same
	// transaction without a race against a concurrent cancel/match.
	GetRideForUpdate(ctx context.Context, id string) (*Ride, error)
	CancelRide(ctx context.Context, id, reason string) error
}
