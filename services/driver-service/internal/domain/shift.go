package domain

import "errors"

type Shift struct {
	ID            string
	DriverID      string
	StartedAt     string
	EndedAt       *string
	Status        string
	TotalRides    int
	TotalEarnings float64
}

func NewShift(driverID string) (*Shift, error) {
	if driverID == "" {
		return nil, errors.New("driverID is required")
	}
	return &Shift{DriverID: driverID}, nil
}
