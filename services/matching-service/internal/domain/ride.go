package domain

import "time"

const (
	RideStatusSearching = "searching"
	RideStatusMatched   = "matched"
	RideStatusFailed    = "failed"
)

type Ride struct {
	RideID             string
	ClientID           string
	Status             string
	PickupLat          float64
	PickupLng          float64
	PickupAddress      string
	DestinationLat     float64
	DestinationLng     float64
	DestinationAddress string
	DistanceKm         float64
	Price              float64
}

// Candidate is one entry of the drivers:online pool, best-rated first.
type Candidate struct {
	DriverID string
	Rating   float64
}

// PendingRide is the retry state for a ride still searching for a driver.
type PendingRide struct {
	RideID   string
	Attempt  int
	Deadline time.Time
}

// DriverOffer is what a polling driver sees: the ride plus when the offer lapses.
type DriverOffer struct {
	Ride      Ride
	ExpiresAt time.Time
}
