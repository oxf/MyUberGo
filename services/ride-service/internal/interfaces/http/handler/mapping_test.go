package handler

import (
	"ride-service/internal/domain"
	"testing"
)

func TestToRideDto_NoDriverAssigned(t *testing.T) {
	dto := toRideDto(&domain.Ride{
		ID: "r1", ClientID: "u1", Status: "Requested",
		PickupLat: 1, PickupLng: 2, PickupAddress: "A",
		DestLat: 3, DestLng: 4, DestAddress: "B",
		EstimatedPrice: 10, EstimatedDistanceKm: 10, CreatedAt: "2026-07-21T10:00:00Z",
	})
	if dto.ID != "r1" || dto.ClientID != "u1" || dto.DriverID != nil {
		t.Fatalf("bad mapping: %+v", dto)
	}
	if dto.Pickup.Latitude != 1 || dto.Pickup.Longitude != 2 || dto.Pickup.Address != "A" {
		t.Fatalf("bad pickup mapping: %+v", dto.Pickup)
	}
	if dto.Destination.Latitude != 3 || dto.Destination.Longitude != 4 || dto.Destination.Address != "B" {
		t.Fatalf("bad destination mapping: %+v", dto.Destination)
	}
}

func TestToRideDto_DriverAssigned(t *testing.T) {
	driverID := "d1"
	dto := toRideDto(&domain.Ride{ID: "r1", ClientID: "u1", DriverID: &driverID, Status: "Matched"})
	if dto.DriverID == nil || *dto.DriverID != "d1" {
		t.Fatalf("expected DriverID d1, got %v", dto.DriverID)
	}
}
