package actors

import (
	"math"
	"math/rand"
	"time"
)

const (
	metersPerDegreeLat = 111320.0

	// minSpeedMps/maxSpeedMps span roughly stopped-in-traffic to city-driving —
	// the spec's own worked example uses 50km/h (~13.9 m/s).
	minSpeedMps = 3.0
	maxSpeedMps = 14.0

	// maxTurnDeg bounds bearing drift per turning tick (turnProbability chance/tick) —
	// small enough to look like a plausible driving path, not a random scatter.
	maxTurnDeg      = 20.0
	turnProbability = 0.3
)

// driverPosition is bearing+speed advanced tick by tick, not a per-axis random walk — a random
// walk's √n growth would leave a "driving" driver only ~40m out after ten minutes (LOCATION_SPEC.md §14).
type driverPosition struct {
	lat, lon   float64
	bearingDeg float64 // 0 = north, 90 = east
	speedMps   float64
	// lastAdvance anchors advanceTo's real-elapsed-time calc, so reported displacement always
	// matches claimed speed — a mismatch is exactly what the teleport guard (LOCATION_SPEC.md §5.5) catches.
	lastAdvance time.Time
}

// newDriverPosition starts a driver at a random point in the Prague box with a random
// bearing/speed; now is passed in (not read internally) so construction and the first
// advance share one timestamp.
func newDriverPosition(rnd *rand.Rand, now time.Time) *driverPosition {
	lat, lon := randomBoxPoint(rnd)
	return &driverPosition{
		lat:         lat,
		lon:         lon,
		bearingDeg:  rnd.Float64() * 360,
		speedMps:    minSpeedMps + rnd.Float64()*(maxSpeedMps-minSpeedMps),
		lastAdvance: now,
	}
}

// advanceTo moves the position forward based on real elapsed time, applying an occasional random turn.
// The longitude step divides by cos(latitude), or eastbound drivers would look faster than northbound (LOCATION_SPEC.md §14).
// A driver that would step outside the Prague box instead bounces off that edge (clamped position,
// bearing mirrored across the edge it hit) — without this a long-running driver's random walk would
// eventually wander out of Prague entirely, since nothing else bounds advanceTo's displacement.
func (p *driverPosition) advanceTo(now time.Time, rnd *rand.Rand) (lat, lon float64) {
	dt := now.Sub(p.lastAdvance)
	p.lastAdvance = now

	if rnd.Float64() < turnProbability {
		p.bearingDeg += (rnd.Float64()*2 - 1) * maxTurnDeg
	}

	distanceM := p.speedMps * dt.Seconds()
	bearingRad := p.bearingDeg * math.Pi / 180

	metersPerDegreeLon := metersPerDegreeLat * math.Cos(p.lat*math.Pi/180)

	newLat := p.lat + (distanceM*math.Cos(bearingRad))/metersPerDegreeLat
	newLon := p.lon + (distanceM*math.Sin(bearingRad))/metersPerDegreeLon

	if newLat > pragueBoxMaxLat || newLat < pragueBoxMinLat {
		newLat = clamp(newLat, pragueBoxMinLat, pragueBoxMaxLat)
		p.bearingDeg = 180 - p.bearingDeg // mirror the north-south component
	}
	if newLon > pragueBoxMaxLon || newLon < pragueBoxMinLon {
		newLon = clamp(newLon, pragueBoxMinLon, pragueBoxMaxLon)
		p.bearingDeg = -p.bearingDeg // mirror the east-west component
	}

	p.lat, p.lon = newLat, newLon
	return p.lat, p.lon
}
