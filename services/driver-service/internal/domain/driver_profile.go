package domain

import "errors"

type DriverProfile struct {
	ID                  string
	UserID              string
	DriverName          string
	Phone               string
	Rating              float64
	VehicleType         string
	LicencePlate        string
	Status              string
	TotalRidesCompleted int
	CreatedAt           string
}

func NewDriverProfile(userID, name, phone, vehicleType, licencePlate string) (*DriverProfile, error) {
	if userID == "" {
		return nil, errors.New("userId is required")
	}
	if name == "" {
		return nil, errors.New("driverName is required")
	}
	if phone == "" {
		return nil, errors.New("phone is required")
	}
	return &DriverProfile{
		UserID:       userID,
		DriverName:   name,
		Phone:        phone,
		VehicleType:  vehicleType,
		LicencePlate: licencePlate,
	}, nil
}
