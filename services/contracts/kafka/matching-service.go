package contracts

type RideAcceptedEvent struct {
	RideID      string `json:"rideId"`
	DriverID    string `json:"driverId"`
	AcceptedAt  string `json:"acceptedAt"`
	// RequestedAt is the ride's original requested_at (RFC3339), threaded through
	// so ride-service can compute time_to_match without an extra DB read.
	RequestedAt string `json:"requestedAt,omitempty"`
}
