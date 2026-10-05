package domain

import (
	"errors"
	"math"
	"time"
)

var ErrEmptyTrack = errors.New("empty track")

// SummarySource records how a ride summary's polyline/distance were
// derived — MapMatched once the Slice-4 Geoapify adapter exists,
// Simplified (RDP + Haversine) until then. Mirrors location.ride_summary's
// source column.
type SummarySource string

const (
	SourceMapMatched SummarySource = "MapMatched"
	SourceSimplified SummarySource = "Simplified"
)

// RideSummary is one ride's map-matched (or simplified-fallback) track —
// the row persisted to location.ride_summary and published as
// ride.summary.ready.
type RideSummary struct {
	RideID     string
	ClientID   string
	DriverID   string
	StartedAt  time.Time
	EndedAt    time.Time
	Start      Coordinate
	End        Coordinate
	Polyline   string
	DistanceM  int64
	DurationS  int
	PointCount int
	Source     SummarySource
}

// BuildSummaryFallback simplifies a raw track via RDP and sums distance via
// Haversine — the source=Simplified path used whenever no
// MapMatchingProvider is configured, or the provider call fails
// (LOCATION_SPEC.md §2.5). epsilonM controls how aggressively RDP drops
// points (LOCATION_RDP_EPSILON_M). Distance is summed in float64 and
// rounded exactly once via math.Round before the int64 cast — the
// repo-wide money-rounding discipline applied to metres (never persist an
// intermediate float).
func BuildSummaryFallback(rideID, clientID, driverID string, startedAt, endedAt time.Time, positions []Position, epsilonM float64) (RideSummary, error) {
	if len(positions) == 0 {
		return RideSummary{}, ErrEmptyTrack
	}

	coords := make([]Coordinate, len(positions))
	for i, p := range positions {
		coords[i] = p.Coordinate
	}

	simplified := SimplifyRDP(coords, epsilonM)

	var distance float64
	for i := 1; i < len(coords); i++ {
		distance += HaversineMeters(coords[i-1], coords[i])
	}

	return RideSummary{
		RideID:     rideID,
		ClientID:   clientID,
		DriverID:   driverID,
		StartedAt:  startedAt,
		EndedAt:    endedAt,
		Start:      coords[0],
		End:        coords[len(coords)-1],
		Polyline:   EncodePolyline(simplified),
		DistanceM:  int64(math.Round(distance)),
		DurationS:  int(endedAt.Sub(startedAt).Seconds()),
		PointCount: len(positions),
		Source:     SourceSimplified,
	}, nil
}
