package domain

import "errors"

type Driver struct {
	ID                  string
	UserID              string
	Rating              float64
	VehicleType         string
	LicencePlate        string
	Status              string
	TotalRidesCompleted int
	CreatedAt           string
}

func NewDriver(userID, vehicleType, licencePlate string) (*Driver, error) {
	if userID == "" {
		return nil, errors.New("userId is required")
	}
	return &Driver{
		UserID:       userID,
		VehicleType:  vehicleType,
		LicencePlate: licencePlate,
	}, nil
}
