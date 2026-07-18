package handler

import (
	"driver-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
)

func toDriverProfileDto(d *domain.DriverProfile) contracts.DriverProfileDto {
	return contracts.DriverProfileDto{
		Id:                  d.ID,
		UserId:              d.UserID,
		DriverName:          d.DriverName,
		Phone:               d.Phone,
		Rating:              d.Rating,
		VehicleType:         d.VehicleType,
		LicencePlate:        d.LicencePlate,
		Status:              d.Status,
		TotalRidesCompleted: d.TotalRidesCompleted,
		CreatedAt:           d.CreatedAt,
	}
}

func toShiftDto(s *domain.Shift) contracts.ShiftDto {
	return contracts.ShiftDto{
		Id:            s.ID,
		DriverId:      s.DriverID,
		StartedAt:     s.StartedAt,
		EndedAt:       s.EndedAt,
		TotalRides:    s.TotalRides,
		TotalEarnings: s.TotalEarnings,
	}
}
