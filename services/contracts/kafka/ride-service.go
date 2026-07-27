package contracts

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type LocationWithAddress struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Address   string  `json:"address"`
}

type RideRequestedEvent struct {
	RideID              string              `json:"rideId"`
	ClientID            string              `json:"clientId"`
	ClientName          string              `json:"clientName"`
	ClientPhone         string              `json:"clientPhone"`
	PickupLocation      LocationWithAddress `json:"pickupLocation"`
	DestinationLocation LocationWithAddress `json:"destinationLocation"`
	DistanceKm          float64             `json:"distanceKm"`
	PriceMinor          int64               `json:"priceMinor"`
	Currency            string              `json:"currency"`
	CreatedAt           string              `json:"createdAt"`
}

type RideCancelledEvent struct {
	RideID      string  `json:"rideId"`
	ClientID    string  `json:"clientId"`
	DriverID    *string `json:"driverId,omitempty"`
	FeeMinor    int64   `json:"feeMinor"`
	Currency    string  `json:"currency"`
	Reason      string  `json:"reason,omitempty"`
	CancelledAt string  `json:"cancelledAt"`
}

type RideStartedEvent struct {
	RideID    string `json:"rideId"`
	DriverID  string `json:"driverId"`
	StartedAt string `json:"startedAt"`
}

type RideCompletedEvent struct {
	RideID      string `json:"rideId"`
	ClientID    string `json:"clientId"`
	DriverID    string `json:"driverId"`
	AmountMinor int64  `json:"amountMinor"`
	Currency    string `json:"currency"`
	FinishedAt  string `json:"finishedAt"`
}
