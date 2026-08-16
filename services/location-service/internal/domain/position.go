package domain

import (
	"errors"
	"math"
	"time"
)

var ErrInvalidCoordinate = errors.New("invalid coordinate")

// Coordinate is a validated lat/lon pair. Always float64 — float32's ~1m of
// representational error silently corrupts short-distance comparisons.
type Coordinate struct {
	Lat float64
	Lon float64
}

// NewCoordinate validates lat/lon at construction so an invalid value can
// never reach a Position, instead of every consumer re-checking bounds.
func NewCoordinate(lat, lon float64) (Coordinate, error) {
	if math.IsNaN(lat) || math.IsNaN(lon) || math.IsInf(lat, 0) || math.IsInf(lon, 0) {
		return Coordinate{}, ErrInvalidCoordinate
	}
	if lat < -90 || lat > 90 {
		return Coordinate{}, ErrInvalidCoordinate
	}
	if lon < -180 || lon > 180 {
		return Coordinate{}, ErrInvalidCoordinate
	}
	return Coordinate{Lat: lat, Lon: lon}, nil
}

const earthRadiusM = 6371000.0

// HaversineMeters is great-circle distance in metres. Spherical, not
// ellipsoidal (~0.5% error) — fine for dispatch, never used for money.
func HaversineMeters(a, b Coordinate) float64 {
	lat1 := a.Lat * math.Pi / 180
	lat2 := b.Lat * math.Pi / 180
	dLat := (b.Lat - a.Lat) * math.Pi / 180
	dLon := (b.Lon - a.Lon) * math.Pi / 180

	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(h), math.Sqrt(1-h))
	return earthRadiusM * c
}

// Position is one accepted GPS fix.
type Position struct {
	Coordinate Coordinate
	AccuracyM  float64
	HeadingDeg float64
	SpeedMps   float64
	// DeviceTs is when the device captured the fix — used for ordering and
	// the teleport/speed guard. ServerTs is when this service accepted it.
	DeviceTs time.Time
	ServerTs time.Time
}
