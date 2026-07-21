package contracts

type RideAcceptedEvent struct {
	RideID     string `json:"rideId"`
	DriverID   string `json:"driverId"`
	AcceptedAt string `json:"acceptedAt"`
}
