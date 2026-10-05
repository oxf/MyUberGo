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

// HistoryEntry is one archived raw ping, shaped for the DynamoDB adapter
// (PK=SubjectID, SK=timestamp_ms) with a GSI on RideID.
type HistoryEntry struct {
	SubjectID   string
	SubjectType SubjectType
	RideID      string
	Position    Position
}

// LocationHistoryRepository archives raw pings to the long-retention,
// eventually-consistent history store (DynamoDB Local in this repo — see
// LOCATION_SPEC.md §6.2). This tier is an audit/verification input, never a
// system of record for money: it may be lossy under load, and callers must
// never let ingest latency depend on it being healthy.
type LocationHistoryRepository interface {
	// PutBatch archives entries in bulk — called by ArchiveWorker on a
	// ticker, never from the synchronous ingest path.
	PutBatch(ctx context.Context, entries []HistoryEntry) error
}

// RideSummaryRepository persists the one-row-per-ride summary
// (location.ride_summary).
type RideSummaryRepository interface {
	// Insert is idempotent against ride.completed/ride.cancelled
	// redelivery (ON CONFLICT (ride_id) DO NOTHING) — same idiom as
	// billing.invoice's UNIQUE(ride_id, type) guard. inserted=false means a
	// summary already existed for this ride: the caller must not also
	// re-publish ride.summary.ready in that case, or a redelivered event
	// republishes forever.
	Insert(ctx context.Context, summary RideSummary) (inserted bool, err error)
	// GetByRideID returns ErrNotFound if no summary exists yet.
	GetByRideID(ctx context.Context, rideID string) (RideSummary, error)
}
