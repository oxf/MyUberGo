package cache

import (
	"context"
	"fmt"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/redis/go-redis/v9"
)

type RideRepository struct {
	rdb *redis.Client
}

func NewRideRepository(rdb *redis.Client) *RideRepository {
	return &RideRepository{rdb: rdb}
}

func (r *RideRepository) SaveRide(
	ctx context.Context,
	event contracts.RideRequestedEvent,
) error {

	key := fmt.Sprintf("ride:%s", event.RideID)

	values := map[string]any{
		"rideId":     event.RideID,
		"clientId":   event.ClientID,
		"clientName": event.ClientName,
		//"clientEmail":        event.ClientEmail,
		"clientPhone": event.ClientPhone,
		//"clientRating":       event.ClientRating,
		"pickupLat":          event.PickupLocation.Latitude,
		"pickupLng":          event.PickupLocation.Longitude,
		"pickupAddress":      event.PickupLocation.Address,
		"destinationLat":     event.DestinationLocation.Latitude,
		"destinationLng":     event.DestinationLocation.Longitude,
		"destinationAddress": event.DestinationLocation.Address,
		"distanceKm":         event.DistanceKm,
		"price":              event.Price,
		//"tariffId":           event.TariffID,
		"createdAt": event.CreatedAt,
		"status":    "searching",
		"radius":    3000,
		"attempt":   1,
	}

	return r.rdb.HSet(ctx, key, values).Err()
}
