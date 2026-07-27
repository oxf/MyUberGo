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
	EstimatedPriceMinor int64
	Currency            string
	EstimatedDistanceKm float64
	BillID              *string // nil until billing-service publishes payment.completed
	CreatedAt           string  // RFC3339
}
