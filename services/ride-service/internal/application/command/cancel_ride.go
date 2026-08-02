package command

import (
	"context"
	"encoding/json"
	"ride-service/internal/application/services"
	"ride-service/internal/common/decorator"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type CancelRide struct {
	RideID   string
	ClientID string
	Reason   string
}

type CancelRideResult struct {
	Status   string
	FeeMinor int64
	Currency string
}

type CancelRideHandler struct {
	repo          domain.RideRepository
	outboxRepo    domain.OutboxRepository
	transaction   services.TransactionManager
	feeCalculator services.CancellationFeeCalculator
	metrics       decorator.MetricsClient
}

func NewCancelRideHandler(
	repo domain.RideRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	feeCalculator services.CancellationFeeCalculator,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[CancelRide, CancelRideResult] {

	handler := &CancelRideHandler{
		repo:          repo,
		outboxRepo:    outboxRepo,
		transaction:   transaction,
		feeCalculator: feeCalculator,
		metrics:       metricsClient,
	}

	return decorator.ApplyCommandDecorators[CancelRide, CancelRideResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *CancelRideHandler) Handle(ctx context.Context, cmd CancelRide) (CancelRideResult, error) {
	var result CancelRideResult
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		ride, err := h.repo.GetRideForUpdate(ctx, cmd.RideID)
		if err != nil {
			return err
		}
		if ride.ClientID != cmd.ClientID {
			return commonerrors.ErrForbidden
		}
		if ride.Status == "InProgress" || ride.Status == "Completed" || ride.Status == "Cancelled" {
			return commonerrors.ErrConflict
		}

		if err := h.repo.CancelRide(ctx, cmd.RideID, cmd.Reason); err != nil {
			return err
		}

		var feeMinor int64
		feeCurrency := ride.Currency
		if ride.DriverID != nil {
			feeMinor, feeCurrency, err = h.feeCalculator.Calculate(ctx, ride)
			if err != nil {
				return err
			}
		}

		event := contractsKafka.RideCancelledEvent{
			RideID:      cmd.RideID,
			ClientID:    ride.ClientID,
			DriverID:    ride.DriverID,
			FeeMinor:    feeMinor,
			Currency:    feeCurrency,
			Reason:      cmd.Reason,
			CancelledAt: time.Now().UTC().Format(time.RFC3339),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		if err := h.outboxRepo.Insert(ctx, &domain.OutboxMessage{
			Topic:     "ride.cancelled",
			EventType: "RideCancelled",
			Payload:   payload,
		}); err != nil {
			return err
		}

		result = CancelRideResult{Status: "Cancelled", FeeMinor: feeMinor, Currency: feeCurrency}
		return nil
	})

	if err == nil && h.metrics != nil {
		h.metrics.IncCounter(ctx, "myubergo.rides.cancelled")
	}

	return result, err
}
