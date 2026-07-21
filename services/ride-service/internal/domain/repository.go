package domain

import "context"

type RideRepository interface {
	CreateRide(ctx context.Context, r *Ride) (string, error)
	GetRideList(ctx context.Context, req PageRequest) ([]*Ride, error)
	CountRides(ctx context.Context) (int, error)
	GetRideByID(ctx context.Context, id string) (*Ride, error)
}
