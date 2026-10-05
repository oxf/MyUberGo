package domain

import "context"

// TrackEntry is one archived position read back from a ride's Redis Stream,
// ordered by stream id.
type TrackEntry struct {
	StreamID string
	Subject  SubjectType
	Position Position
}

// RideTrackRepository buffers a ride's raw pings in a capped Redis Stream
// (loc:ride:{id}:track, MAXLEN ~5000, 24h TTL) — the buffer ArchiveWorker
// drains to the history store and BuildRideSummary reads to build the
// polyline. Ingest latency must never depend on this being healthy: a
// failed Append is logged and counted by the caller, never returned as an
// ingest failure (LOCATION_SPEC.md §6.2).
type RideTrackRepository interface {
	// Append adds one accepted position to the ride's track stream.
	Append(ctx context.Context, rideID string, subject SubjectType, pos Position) error
	// Range reads a ride's track in stream order, starting strictly after
	// afterID (empty string reads from the beginning).
	Range(ctx context.Context, rideID string, afterID string) ([]TrackEntry, error)
	// Trim deletes the ride's track stream — called once a summary has been
	// durably built and the raw track archived.
	Trim(ctx context.Context, rideID string) error
	// LastArchivedID returns the stream id ArchiveWorker last successfully
	// archived for this ride ("" if none yet).
	LastArchivedID(ctx context.Context, rideID string) (string, error)
	// SetLastArchivedID advances the archive checkpoint after a successful
	// batch write to the history store.
	SetLastArchivedID(ctx context.Context, rideID string, streamID string) error
}
