package domain

import "time"

const (
	RideStatusSearching = "searching"
	RideStatusMatched   = "matched"
	RideStatusFailed    = "failed"
	RideStatusCancelled = "cancelled"
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
	PriceMinor         int64
	Currency           string
	// DriverRating is cached at MarkMatched time so a cancellation can restore the driver to
	// drivers:online without a cross-service lookup. Zero until matched.
	DriverRating float64
	// CreatedAt is the ride's original requested_at (RFC3339), carried from
	// RideRequestedEvent so AcceptRideHandler can thread it into RideAcceptedEvent.
	CreatedAt string
}

// Candidate is one driver eligible for an offer. DriverID/Rating come from drivers:online;
// DistanceM is populated only when this round used location-service's geo discovery (0 on fallback).
type Candidate struct {
	DriverID  string
	Rating    float64
	DistanceM int64
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
