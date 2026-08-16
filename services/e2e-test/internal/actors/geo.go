package actors

// rideBoxLat/rideBoxLon/rideBoxSpanDeg define the ~11km x ~7km area rides and drivers are
// drawn from — sized near the search radius so in/out-of-radius mixes happen realistically (LOCATION_SPEC.md §14).
const (
	rideBoxLat     = 50.40
	rideBoxLon     = 30.50
	rideBoxSpanDeg = 0.1
)
