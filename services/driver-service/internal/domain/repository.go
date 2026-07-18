package domain

import "context"

type DriverProfileRepository interface {
	CreateDriverProfile(ctx context.Context, profile *DriverProfile) (string, error)
	UpdateDriverProfile(ctx context.Context, id, name, phone, vehicleType, licencePlate string) error
	GetDriverProfileList(ctx context.Context, req PageRequest) ([]*DriverProfile, error)
	CountDriverProfiles(ctx context.Context) (int, error)
	GetDriverProfileByID(ctx context.Context, id string) (*DriverProfile, error)
}

type ShiftRepository interface {
	CreateShift(ctx context.Context, shift *Shift) (string, error)
	UpdateShift(ctx context.Context, shift *Shift) error
	HasActiveShift(ctx context.Context, driverID string) (bool, error)
	EndShift(ctx context.Context, id string) error
	GetShiftList(ctx context.Context, req PageRequest) ([]*Shift, error)
	CountShifts(ctx context.Context) (int, error)
	GetShiftByID(ctx context.Context, id string) (*Shift, error)
}
