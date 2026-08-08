package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"

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
		"priceMinor":         event.PriceMinor,
		"currency":           event.Currency,
		//"tariffId":           event.TariffID,
		"createdAt": event.CreatedAt,
		"status":    "searching",
	}

	return r.rdb.HSet(ctx, key, values).Err()
}

func (r *RideRepository) GetRide(ctx context.Context, rideID string) (*domain.Ride, error) {
	m, err := r.rdb.HGetAll(ctx, "ride:"+rideID).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, cmnerrors.ErrNotFound
	}
	ride := &domain.Ride{
		RideID:             m["rideId"],
		ClientID:           m["clientId"],
		Status:             m["status"],
		PickupAddress:      m["pickupAddress"],
		DestinationAddress: m["destinationAddress"],
		CreatedAt:          m["createdAt"],
	}
	ride.PickupLat, _ = strconv.ParseFloat(m["pickupLat"], 64)
	ride.PickupLng, _ = strconv.ParseFloat(m["pickupLng"], 64)
	ride.DestinationLat, _ = strconv.ParseFloat(m["destinationLat"], 64)
	ride.DestinationLng, _ = strconv.ParseFloat(m["destinationLng"], 64)
	ride.DistanceKm, _ = strconv.ParseFloat(m["distanceKm"], 64)
	ride.PriceMinor, _ = strconv.ParseInt(m["priceMinor"], 10, 64)
	ride.Currency = m["currency"]
	ride.DriverRating, _ = strconv.ParseFloat(m["driverRating"], 64)
	return ride, nil
}

func (r *RideRepository) MarkMatched(ctx context.Context, rideID, driverID string, driverRating float64) error {
	return r.rdb.HSet(ctx, "ride:"+rideID, map[string]any{
		"status":       domain.RideStatusMatched,
		"driverId":     driverID,
		"driverRating": driverRating,
		"matchedAt":    time.Now().UTC().Format(time.RFC3339),
	}).Err()
}

func (r *RideRepository) MarkFailed(ctx context.Context, rideID string) error {
	return r.rdb.HSet(ctx, "ride:"+rideID, "status", domain.RideStatusFailed).Err()
}

func (r *RideRepository) MarkCancelled(ctx context.Context, rideID string) error {
	return r.rdb.HSet(ctx, "ride:"+rideID, "status", domain.RideStatusCancelled).Err()
}
