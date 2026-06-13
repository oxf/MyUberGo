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
	Price               float64             `json:"price"`
	CreatedAt           string              `json:"createdAt"`
}
