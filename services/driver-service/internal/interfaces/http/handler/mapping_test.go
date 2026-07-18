package handler

import (
	"driver-service/internal/domain"
	"testing"
)

func TestToDriverProfileDto(t *testing.T) {
	dto := toDriverProfileDto(&domain.DriverProfile{
		ID: "p1", UserID: "u1", DriverName: "Ann", Phone: "+380501234567",
		Rating: 4.9, VehicleType: "Sedan", LicencePlate: "AA1234BB",
		Status: "Online", TotalRidesCompleted: 7, CreatedAt: "2026-07-18T10:00:00Z",
	})
	if dto.Id != "p1" || dto.UserId != "u1" || dto.DriverName != "Ann" || dto.TotalRidesCompleted != 7 {
		t.Fatalf("bad mapping: %+v", dto)
	}
}

func TestToShiftDto_OpenAndEnded(t *testing.T) {
	open := toShiftDto(&domain.Shift{ID: "s1", DriverID: "p1", StartedAt: "2026-07-18T10:00:00Z"})
	if open.EndedAt != nil {
		t.Fatalf("expected nil EndedAt for open shift, got %v", *open.EndedAt)
	}

	ended := "2026-07-18T12:00:00Z"
	done := toShiftDto(&domain.Shift{ID: "s2", DriverID: "p1", StartedAt: "2026-07-18T10:00:00Z", EndedAt: &ended})
	if done.EndedAt == nil || *done.EndedAt != ended {
		t.Fatalf("expected EndedAt %q, got %v", ended, done.EndedAt)
	}
}
