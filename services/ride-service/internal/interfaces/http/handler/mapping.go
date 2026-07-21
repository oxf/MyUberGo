package handler

import (
	"ride-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
)

func toRideDto(r *domain.Ride) contracts.RideDto {
	return contracts.RideDto{
		ID:       r.ID,
		ClientID: r.ClientID,
		DriverID: r.DriverID,
		Status:   r.Status,
		Pickup: contracts.LocationRequest{
			Latitude:  r.PickupLat,
			Longitude: r.PickupLng,
			Address:   r.PickupAddress,
		},
		Destination: contracts.LocationRequest{
			Latitude:  r.DestLat,
			Longitude: r.DestLng,
			Address:   r.DestAddress,
		},
		EstimatedPrice:      r.EstimatedPrice,
		EstimatedDistanceKm: r.EstimatedDistanceKm,
		CreatedAt:           r.CreatedAt,
	}
}
