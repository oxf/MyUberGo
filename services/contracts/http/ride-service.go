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
	// TariffName selects the fare table (ride.tariff.name); defaults to
	// "Standard" when omitted.
	TariffName string `json:"tariffName,omitempty"`
}

type CreateRideResponse struct {
	RideID              string  `json:"rideId"`
	ClientID            string  `json:"clientId"`
	Status              string  `json:"status"`
	EstimatedPriceMinor int64   `json:"estimatedPriceMinor"`
	Currency            string  `json:"currency"`
	EstimatedDistanceKm float64 `json:"estimatedDistanceKm"`
	CreatedAt           string  `json:"createdAt"`
}

type RideDto struct {
	ID                  string          `json:"id"`
	ClientID            string          `json:"clientId"`
	DriverID            *string         `json:"driverId,omitempty"`
	Status              string          `json:"status"`
	Pickup              LocationRequest `json:"pickup"`
	Destination         LocationRequest `json:"destination"`
	EstimatedPriceMinor int64           `json:"estimatedPriceMinor"`
	Currency            string          `json:"currency"`
	EstimatedDistanceKm float64         `json:"estimatedDistanceKm"`
	CreatedAt           string          `json:"createdAt"`
}

type CancelRideRequest struct {
	Reason string `json:"reason,omitempty"`
}

type CancelRideResponse struct {
	Status   string `json:"status"`
	FeeMinor int64  `json:"feeMinor"`
	Currency string `json:"currency"`
}

type StartRideRequest struct {
	DriverId string `json:"driverId"`
}

type StartRideResponse struct {
	Status    string `json:"status"`
	StartedAt string `json:"startedAt"`
}

type CompleteRideRequest struct {
	DriverId string `json:"driverId"`
}

type CompleteRideResponse struct {
	Status     string `json:"status"`
	FinishedAt string `json:"finishedAt"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
