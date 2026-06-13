package contracts

type LocationRequest struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Address   string  `json:"address"`
}

type CreateRideRequest struct {
	PickupLat     float64 `json:"pickupLat"`
	PickupLng     float64 `json:"pickupLng"`
	DestLat       float64 `json:"destLat"`
	DestLng       float64 `json:"destLng"`
	PickupAddress string  `json:"pickupAddress"`
	DestAddress   string  `json:"destAddress"`
}

type CreateRideResponse struct {
	RideID              string  `json:"rideId"`
	ClientID            string  `json:"clientId"`
	Status              string  `json:"status"`
	EstimatedPrice      float64 `json:"estimatedPrice"`
	EstimatedDistanceKm float64 `json:"estimatedDistanceKm"`
	CreatedAt           string  `json:"createdAt"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
