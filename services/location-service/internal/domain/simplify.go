package domain

import "math"

// SimplifyRDP reduces track to the points whose perpendicular deviation from
// the straight line between their neighbors exceeds epsilonM
// (Ramer-Douglas-Peucker). Distance is computed in a local flat projection
// (accurate at ride-track scale) rather than repeated Haversine calls. This
// is what makes a real turn survive simplification instead of being smoothed
// away the way every-Nth sampling would (LOCATION_SPEC.md §2.5).
func SimplifyRDP(track []Coordinate, epsilonM float64) []Coordinate {
	if len(track) < 3 {
		return track
	}

	pts := projectFlat(track)

	keep := make([]bool, len(track))
	keep[0] = true
	keep[len(track)-1] = true
	rdpRange(pts, 0, len(pts)-1, epsilonM, keep)

	out := make([]Coordinate, 0, len(track))
	for i, k := range keep {
		if k {
			out = append(out, track[i])
		}
	}
	return out
}

type flatPoint struct{ x, y float64 }

// projectFlat converts coordinates to local metre offsets using an
// equirectangular approximation referenced at track[0]'s latitude —
// accurate enough at ride-track scale (a few km), far cheaper than true
// cross-track spherical distance.
func projectFlat(track []Coordinate) []flatPoint {
	lat0 := track[0].Lat * math.Pi / 180
	pts := make([]flatPoint, len(track))
	for i, c := range track {
		pts[i] = flatPoint{
			x: earthRadiusM * (c.Lon * math.Pi / 180) * math.Cos(lat0),
			y: earthRadiusM * (c.Lat * math.Pi / 180),
		}
	}
	return pts
}

func rdpRange(pts []flatPoint, first, last int, epsilonM float64, keep []bool) {
	if last <= first+1 {
		return
	}

	maxDist := -1.0
	maxIdx := -1
	for i := first + 1; i < last; i++ {
		d := perpendicularDistance(pts[i], pts[first], pts[last])
		if d > maxDist {
			maxDist = d
			maxIdx = i
		}
	}

	if maxDist > epsilonM {
		keep[maxIdx] = true
		rdpRange(pts, first, maxIdx, epsilonM, keep)
		rdpRange(pts, maxIdx, last, epsilonM, keep)
	}
}

func perpendicularDistance(p, a, b flatPoint) float64 {
	dx := b.x - a.x
	dy := b.y - a.y
	if dx == 0 && dy == 0 {
		return math.Hypot(p.x-a.x, p.y-a.y)
	}
	num := math.Abs(dy*p.x - dx*p.y + b.x*a.y - b.y*a.x)
	den := math.Hypot(dx, dy)
	return num / den
}
