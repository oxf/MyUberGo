package domain

import "context"

// MapMatchingProvider snaps a raw GPS track to the road network, returning a
// road-snapped polyline and road distance — the source=MapMatched path.
// Implemented by the Slice-4 Geoapify adapter; a nil provider means "not
// configured," in which case the caller falls back to BuildSummaryFallback
// (LOCATION_SPEC.md §2.5).
type MapMatchingProvider interface {
	MatchRoute(ctx context.Context, track []Coordinate) (polyline string, distanceM int64, err error)
}
