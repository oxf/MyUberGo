package actors

import "math/rand"

// pragueBoxMinLat/MaxLat/MinLon/MaxLon bound the ~12km x 17km area rides and
// drivers are drawn from — sized near the search radius so in/out-of-radius
// mixes happen realistically (LOCATION_SPEC.md §14). Corners: top-left (NW)
// 50°07'17.19"N 14°18'53.90"E to bottom-right (SE) 50°00'48.78"N 14°33'28.83"E.
const (
	pragueBoxMinLat = 50.01355 // south edge (bottom-right corner's latitude)
	pragueBoxMaxLat = 50.12144 // north edge (top-left corner's latitude)
	pragueBoxMinLon = 14.31497 // west edge (top-left corner's longitude)
	pragueBoxMaxLon = 14.55801 // east edge (bottom-right corner's longitude)
)

// randomBoxPoint returns a uniformly random point inside the Prague box.
func randomBoxPoint(rnd *rand.Rand) (lat, lon float64) {
	lat = pragueBoxMinLat + rnd.Float64()*(pragueBoxMaxLat-pragueBoxMinLat)
	lon = pragueBoxMinLon + rnd.Float64()*(pragueBoxMaxLon-pragueBoxMinLon)
	return lat, lon
}

func clamp(v, lo, hi float64) float64 {
	return min(max(v, lo), hi)
}
