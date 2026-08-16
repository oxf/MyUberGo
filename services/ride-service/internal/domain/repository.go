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
	// MarkRideBilled sets bill_id once billing-service publishes
	// payment.completed. Idempotent against redelivery (WHERE bill_id IS
	// NULL) — same "AND status = 'Requested'"-style guard as MarkRideMatched.
	MarkRideBilled(ctx context.Context, rideID, billID string) error
	// GetRideForUpdate locks the ride row (SELECT ... FOR UPDATE) so its
	// status/ownership can be checked and then mutated within the same
	// transaction without a race against a concurrent cancel/match.
	GetRideForUpdate(ctx context.Context, id string) (*Ride, error)
	CancelRide(ctx context.Context, id, reason string) error
	MarkRideStarted(ctx context.Context, id string, startedAt time.Time) error
	CompleteRide(ctx context.Context, id string, finishedAt time.Time) error
	// FailRide flips a ride to Failed once matching-service gives up after exhausting
	// its retries. Idempotent against redelivery (WHERE status = 'Requested'), same
	// guard style as MarkRideMatched (docs/AUDIT_2026-08-15.md #11).
	FailRide(ctx context.Context, rideID string) error
}
