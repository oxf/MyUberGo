package contracts

type RideAcceptedEvent struct {
	RideID      string `json:"rideId"`
	DriverID    string `json:"driverId"`
	AcceptedAt  string `json:"acceptedAt"`
	// RequestedAt is the ride's original requested_at (RFC3339), threaded through
	// so ride-service can compute time_to_match without an extra DB read.
	RequestedAt string `json:"requestedAt,omitempty"`
}

// RideMatchingFailedEvent is published when matching-service gives up on a ride after
// MaxAttempts retries — closes the gap where ride-service's Postgres row otherwise stays
// 'Requested' forever with no signal that matching ever gave up (docs/AUDIT_2026-08-15.md #11).
type RideMatchingFailedEvent struct {
	RideID string `json:"rideId"`
}
