package contracts

type AcceptRideRequest struct {
	DriverId string `json:"driverId"`
}

type AcceptRideResponse struct {
	RideId   string `json:"rideId"`
	DriverId string `json:"driverId"`
	Status   string `json:"status"`
}

type DriverOfferDto struct {
	RideId             string  `json:"rideId"`
	PickupLat          float64 `json:"pickupLat"`
	PickupLng          float64 `json:"pickupLng"`
	PickupAddress      string  `json:"pickupAddress"`
	DestinationLat     float64 `json:"destinationLat"`
	DestinationLng     float64 `json:"destinationLng"`
	DestinationAddress string  `json:"destinationAddress"`
	DistanceKm         float64 `json:"distanceKm"`
	Price              float64 `json:"price"`
	ExpiresAt          string  `json:"expiresAt"`
}
