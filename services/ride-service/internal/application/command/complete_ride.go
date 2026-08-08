package command

import (
	"context"
	"encoding/json"
	"ride-service/internal/application/services"
	"ride-service/internal/common/decorator"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"ride-service/internal/infrastructure/metrics"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type CompleteRide struct {
	RideID   string
	DriverID string
}

type CompleteRideResult struct {
	Status     string
	FinishedAt string
}

type CompleteRideHandler struct {
	repo        domain.RideRepository
	outboxRepo  domain.OutboxRepository
	transaction services.TransactionManager
	metrics     decorator.MetricsClient
}

func NewCompleteRideHandler(
	repo domain.RideRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[CompleteRide, CompleteRideResult] {
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}

	handler := &CompleteRideHandler{
		repo:        repo,
		outboxRepo:  outboxRepo,
		transaction: transaction,
		metrics:     metricsClient,
	}

	return decorator.ApplyCommandDecorators[CompleteRide, CompleteRideResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *CompleteRideHandler) Handle(ctx context.Context, cmd CompleteRide) (CompleteRideResult, error) {
	var result CompleteRideResult
	var currency string
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		ride, err := h.repo.GetRideForUpdate(ctx, cmd.RideID)
		if err != nil {
			return err
		}
		if ride.DriverID == nil || *ride.DriverID != cmd.DriverID {
			return commonerrors.ErrForbidden
		}
		if ride.Status != "InProgress" {
			return commonerrors.ErrConflict
		}

		finishedAt := time.Now().UTC()
		if err := h.repo.CompleteRide(ctx, cmd.RideID, finishedAt); err != nil {
			return err
		}

		event := contractsKafka.RideCompletedEvent{
			RideID:      cmd.RideID,
			ClientID:    ride.ClientID,
			DriverID:    cmd.DriverID,
			AmountMinor: ride.EstimatedPriceMinor,
			Currency:    ride.Currency,
			FinishedAt:  finishedAt.Format(time.RFC3339),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		if err := h.outboxRepo.Insert(ctx, &domain.OutboxMessage{
			Topic:     "ride.completed",
			EventType: "RideCompleted",
			Payload:   payload,
		}); err != nil {
			return err
		}

		currency = ride.Currency
		result = CompleteRideResult{Status: "Completed", FinishedAt: finishedAt.Format(time.RFC3339)}
		return nil
	})

	if err == nil {
		h.metrics.IncCounter(ctx, "myubergo.rides.completed", attribute.String("currency", currency))
	}

	return result, err
}
