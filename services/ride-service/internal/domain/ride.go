package domain

type Ride struct {
	ID                  string
	ClientID            string
	DriverID            *string // nil until matched
	Status              string
	PickupLat           float64
	PickupLng           float64
	PickupAddress       string
	DestLat             float64
	DestLng             float64
	DestAddress         string
	EstimatedPrice      float64
	EstimatedDistanceKm float64
	CreatedAt           string // RFC3339
}
