package domain

import "context"

type DriverProfileRepository interface {
	CreateDriverProfile(ctx context.Context, profile *DriverProfile) (string, error)
	UpdateDriverProfile(ctx context.Context, id, name, phone, vehicleType, licencePlate string) error
	GetDriverProfileList(ctx context.Context, req PageRequest) ([]*DriverProfile, error)
	CountDriverProfiles(ctx context.Context) (int, error)
	GetDriverProfileByID(ctx context.Context, id string) (*DriverProfile, error)
	// UpdateDriverStatus flips status from fromStatus to toStatus, guarded by
	// the expected current status. Returns false (not an error) if the guard
	// didn't match, e.g. a redelivered/duplicate event.
	UpdateDriverStatus(ctx context.Context, id, fromStatus, toStatus string) (bool, error)
	// IncrementRidesCompleted bumps total_rides_completed by one.
	// Unconditional write - idempotency for redelivery is handled one layer
	// up, by only calling this when UpdateDriverStatus reports the OnRide ->
	// Online flip actually happened.
	IncrementRidesCompleted(ctx context.Context, id string) error
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
