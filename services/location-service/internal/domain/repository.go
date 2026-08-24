package domain

import (
	"context"
	"time"
)

// DriverLocationRepository persists live driver positions in the Redis geo
// index (loc:drivers:geo / loc:drivers:lastseen / loc:driver:{id}).
type DriverLocationRepository interface {
	// LastPosition returns the driver's last stored position, or nil (not an
	// error) if none — feeds ValidatePing's ordering/teleport guard.
	LastPosition(ctx context.Context, driverID string) (*Position, error)
	// UpsertPosition writes the geo index, lastseen score, and detail hash
	// for one driver in a single pipeline.
	UpsertPosition(ctx context.Context, driverID string, pos Position) error
	// Nearby returns up to limit geographic candidates within radiusKm of
	// center, nearest first — no availability filtering.
	Nearby(ctx context.Context, center Coordinate, radiusKm float64, limit int) ([]NearbyDriver, error)
	// StaleDriverIDs returns driver ids whose lastseen score is older than
	// olderThan — used by StalenessWorker's sweep.
	StaleDriverIDs(ctx context.Context, olderThan time.Time) ([]string, error)
	// Evict removes driverIDs from both the geo index and lastseen set,
	// pipelined.
	Evict(ctx context.Context, driverIDs []string) error
	// AllPositions returns every driver currently within the repository's
	// staleness threshold of loc:drivers:lastseen, each with its full
	// position — the fleet-wide read the admin live-positions map uses.
	AllPositions(ctx context.Context) ([]DriverPosition, error)
}

// NearbyDriver is one geographic candidate returned by Nearby.
type NearbyDriver struct {
	DriverID  string
	DistanceM int64
}

// DriverPosition pairs a driver id with its full position, returned by
// AllPositions.
type DriverPosition struct {
	DriverID string
	Position Position
}

// OwnerRepository caches the driver<->user mapping from shift.updated —
// ingest resolves driverId from X-User-Id rather than a self-asserted field.
type OwnerRepository interface {
	// SetOwner writes both directions of the mapping — idempotent, safe
	// against shift.updated redelivery.
	SetOwner(ctx context.Context, driverID, userID string) error
	// DriverIDForUser returns ("", nil) if the user has no cached driver
	// mapping yet — not an error, the caller has simply never opened a shift.
	DriverIDForUser(ctx context.Context, userID string) (string, error)
}

// ClientLocationRepository persists live client (rider) positions —
// populated only while a ride's tracking window is open (LOCATION_SPEC.md §17 decision 5).
type ClientLocationRepository interface {
	// LastPosition returns the client's last stored position, or nil (not an
	// error) if none.
	LastPosition(ctx context.Context, clientID string) (*Position, error)
	// UpsertPosition writes the client's position hash.
	UpsertPosition(ctx context.Context, clientID string, pos Position) error
}
