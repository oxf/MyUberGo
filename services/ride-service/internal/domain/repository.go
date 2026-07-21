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
}
