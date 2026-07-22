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
	Status string
	Fee    float64
}

type CancelRideHandler struct {
	repo          domain.RideRepository
	outboxRepo    domain.OutboxRepository
	transaction   services.TransactionManager
	feeCalculator services.CancellationFeeCalculator
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
		if ride.Status == "Completed" || ride.Status == "Cancelled" {
			return commonerrors.ErrConflict
		}

		if err := h.repo.CancelRide(ctx, cmd.RideID, cmd.Reason); err != nil {
			return err
		}

		var fee float64
		if ride.DriverID != nil {
			fee, err = h.feeCalculator.Calculate(ctx, ride)
			if err != nil {
				return err
			}
		}

		event := contractsKafka.RideCancelledEvent{
			RideID:      cmd.RideID,
			DriverID:    ride.DriverID,
			Fee:         fee,
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

		result = CancelRideResult{Status: "Cancelled", Fee: fee}
		return nil
	})

	return result, err
}
