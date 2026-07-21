package command

import (
	"context"
	"encoding/json"
	"ride-service/internal/application/services"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type CreateRide struct {
	ClientID      string
	PickupLat     float64
	PickupLng     float64
	PickupAddress string
	DestLat       float64
	DestLng       float64
	DestAddress   string
}

type CreateRideResult struct {
	RideID              string
	Status              string
	EstimatedPrice      float64
	EstimatedDistanceKm float64
	CreatedAt           string
}

type CreateRideHandler struct {
	repo        domain.RideRepository
	outboxRepo  domain.OutboxRepository
	transaction services.TransactionManager
}

func NewCreateRideHandler(
	repo domain.RideRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[CreateRide, CreateRideResult] {

	handler := &CreateRideHandler{
		repo:        repo,
		outboxRepo:  outboxRepo,
		transaction: transaction,
	}

	return decorator.ApplyCommandDecorators[CreateRide, CreateRideResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *CreateRideHandler) Handle(ctx context.Context, cmd CreateRide) (CreateRideResult, error) {
	// Fare/distance are still hardcoded stubs, matching current behavior —
	// no real calculation exists yet (ride.tariff is unread).
	const distanceKm = 10.0
	const price = 10.0
	createdAt := time.Now().UTC().Format(time.RFC3339)

	var result CreateRideResult
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		ride := &domain.Ride{
			ClientID:            cmd.ClientID,
			Status:              "Requested",
			PickupLat:           cmd.PickupLat,
			PickupLng:           cmd.PickupLng,
			PickupAddress:       cmd.PickupAddress,
			DestLat:             cmd.DestLat,
			DestLng:             cmd.DestLng,
			DestAddress:         cmd.DestAddress,
			EstimatedPrice:      price,
			EstimatedDistanceKm: distanceKm,
		}

		id, err := h.repo.CreateRide(ctx, ride)
		if err != nil {
			return err
		}

		event := contractsKafka.RideRequestedEvent{
			RideID:     id,
			ClientID:   cmd.ClientID,
			DistanceKm: distanceKm,
			Price:      price,
			PickupLocation: contractsKafka.LocationWithAddress{
				Latitude:  cmd.PickupLat,
				Longitude: cmd.PickupLng,
				Address:   cmd.PickupAddress,
			},
			DestinationLocation: contractsKafka.LocationWithAddress{
				Latitude:  cmd.DestLat,
				Longitude: cmd.DestLng,
				Address:   cmd.DestAddress,
			},
			// ClientName/ClientPhone/CreatedAt left zero-valued — ride-service
			// has no access to the user's name/phone, and CreatedAt was never
			// populated by the pre-refactor handler either. Pre-existing
			// contract gap, not this refactor's job to fix.
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		if err := h.outboxRepo.Insert(ctx, &domain.OutboxMessage{
			Topic:     "ride.requested",
			EventType: "RideRequested",
			Payload:   payload,
		}); err != nil {
			return err
		}

		result = CreateRideResult{
			RideID:              id,
			Status:              "Requested",
			EstimatedPrice:      price,
			EstimatedDistanceKm: distanceKm,
			CreatedAt:           createdAt,
		}
		return nil
	})

	return result, err
}
