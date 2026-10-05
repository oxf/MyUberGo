package contracts

// RideSummaryReadyEvent is location-service's summary-tier output, built
// from a ride's archived track once it ends (LOCATION_SPEC.md §8.2).
// Distance follows the repo's money-convention discipline: integer metres,
// never a bare float.
type RideSummaryReadyEvent struct {
	RideID    string `json:"rideId"`
	ClientID  string `json:"clientId"`
	DriverID  string `json:"driverId"`
	DistanceM int64  `json:"distanceM"`
	DurationS int    `json:"durationS"`
	Polyline  string `json:"polyline"`
	StartedAt string `json:"startedAt"`
	EndedAt   string `json:"endedAt"`
	// Source is "MapMatched" or "Simplified" — mirrors
	// location.ride_summary.source, so a consumer can see a degraded summary.
	Source string `json:"source"`
}
