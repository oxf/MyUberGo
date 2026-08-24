package domain

import (
	"context"
	"time"
)

// SubjectType distinguishes which side of a ride's tracking window a
// position update belongs to.
type SubjectType string

const (
	SubjectDriver SubjectType = "driver"
	SubjectClient SubjectType = "client"
)

// PositionUpdate is a validated, accepted position for a subject inside an
// open tracking window, ready to fan out over Redis Pub/Sub (Slice 2).
type PositionUpdate struct {
	RideID   string
	Subject  SubjectType
	Position Position
}

// PositionPublisher fans an accepted position out to whichever instance
// holds the ride's counterparty WS connection (LOCATION_SPEC.md §7.3).
type PositionPublisher interface {
	Publish(ctx context.Context, update PositionUpdate) error
}

// Participants is one ride's open tracking window — the sole authorization
// scope for WS streaming and the counterparty endpoint (LOCATION_SPEC.md §9).
type Participants struct {
	RideID   string
	ClientID string
	DriverID string
	OpenedAt time.Time
}

// TrackingRepository manages a ride's tracking window: opened once both
// ride.requested/accepted are observed in either order, closed on completion/cancellation.
type TrackingRepository interface {
	// RecordRideRequested caches clientID pending a matching ride.accepted.
	RecordRideRequested(ctx context.Context, rideID, clientID string) error
	// RecordRideAccepted caches driverID pending a matching ride.requested.
	RecordRideAccepted(ctx context.Context, rideID, driverID string, acceptedAt time.Time) error
	// CloseWindow is idempotent — safe on a window never completed or already closed.
	CloseWindow(ctx context.Context, rideID string) error
	// Participants returns the ride's open window, or ok=false if none is open.
	Participants(ctx context.Context, rideID string) (participants Participants, ok bool, err error)
	// ActiveRideForDriver returns the driver's open rideId, or "" if none.
	ActiveRideForDriver(ctx context.Context, driverID string) (string, error)
	// ActiveRideForClient returns the client's open rideId, or "" if none.
	ActiveRideForClient(ctx context.Context, clientID string) (string, error)
	// ActiveRideIDs returns every ride id with a currently-open tracking
	// window — the fleet-wide read the admin live-positions map uses to find
	// which clients currently have a position worth showing.
	ActiveRideIDs(ctx context.Context) ([]string, error)
}
