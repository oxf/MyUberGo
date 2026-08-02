package command

import (
	"context"
	"encoding/json"
	"math"
	"ride-service/internal/application/services"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

// ClientID is auth.client(id) (from Kong's X-Client-Id header), not
// auth.user(id) — see the role-table refactor notes in CLAUDE.md/PLAN.md.
type CreateRide struct {
	ClientID      string
	PickupLat     float64
	PickupLng     float64
	PickupAddress string
	DestLat       float64
	DestLng       float64
	DestAddress   string
	// TariffName defaults to "Standard" when empty.
	TariffName string
}

type CreateRideResult struct {
	RideID              string
	Status              string
	EstimatedPriceMinor int64
	Currency            string
	EstimatedDistanceKm float64
	CreatedAt           string
}

type CreateRideHandler struct {
	repo        domain.RideRepository
	tariffRepo  domain.TariffRepository
	outboxRepo  domain.OutboxRepository
	transaction services.TransactionManager
	metrics     decorator.MetricsClient
}

func NewCreateRideHandler(
	repo domain.RideRepository,
	tariffRepo domain.TariffRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[CreateRide, CreateRideResult] {

	handler := &CreateRideHandler{
		repo:        repo,
		tariffRepo:  tariffRepo,
		outboxRepo:  outboxRepo,
		transaction: transaction,
		metrics:     metricsClient,
	}

	return decorator.ApplyCommandDecorators[CreateRide, CreateRideResult](
		handler,
		logger,
		metricsClient,
	)
}

const avgSpeedKmh = 30.0 // stub: no traffic/routing data exists yet

func haversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusKm = 6371.0
	toRad := func(deg float64) float64 { return deg * math.Pi / 180 }

	dLat := toRad(lat2 - lat1)
	dLng := toRad(lng2 - lng1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*math.Sin(dLng/2)*math.Sin(dLng/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKm * c
}

func (h *CreateRideHandler) Handle(ctx context.Context, cmd CreateRide) (CreateRideResult, error) {
	tariffName := cmd.TariffName
	if tariffName == "" {
		tariffName = "Standard"
	}
	tariff, err := h.tariffRepo.GetByName(ctx, tariffName)
	if err != nil {
		return CreateRideResult{}, err
	}

	distanceKm := haversineKm(cmd.PickupLat, cmd.PickupLng, cmd.DestLat, cmd.DestLng)
	durationMin := distanceKm / avgSpeedKmh * 60

	// Rounding rule (CLAUDE.md §2): fractional multiplication in float64,
	// round once at the end, to int64. Never persist an intermediate float.
	fareMinor := tariff.BaseFareMinor +
		int64(math.Round(float64(tariff.PricePerKmMinor)*distanceKm)) +
		int64(math.Round(float64(tariff.PricePerMinMinor)*durationMin))

	createdAt := time.Now().UTC().Format(time.RFC3339)

	var result CreateRideResult
	err = h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		ride := &domain.Ride{
			ClientID:            cmd.ClientID,
			Status:              "Requested",
			PickupLat:           cmd.PickupLat,
			PickupLng:           cmd.PickupLng,
			PickupAddress:       cmd.PickupAddress,
			DestLat:             cmd.DestLat,
			DestLng:             cmd.DestLng,
			DestAddress:         cmd.DestAddress,
			EstimatedPriceMinor: fareMinor,
			Currency:            tariff.Currency,
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
			PriceMinor: fareMinor,
			Currency:   tariff.Currency,
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
			EstimatedPriceMinor: fareMinor,
			Currency:            tariff.Currency,
			EstimatedDistanceKm: distanceKm,
			CreatedAt:           createdAt,
		}
		return nil
	})

	if err == nil && h.metrics != nil {
		currencyAttr := attribute.String("currency", tariff.Currency)
		h.metrics.IncCounter(ctx, "myubergo.rides.requested", currencyAttr)
		h.metrics.RecordValue(ctx, "myubergo.ride.estimated_fare_minor", float64(fareMinor), currencyAttr)
	}

	return result, err
}
