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

type StartRide struct {
	RideID   string
	DriverID string
}

type StartRideResult struct {
	Status    string
	StartedAt string
}

type StartRideHandler struct {
	repo        domain.RideRepository
	outboxRepo  domain.OutboxRepository
	transaction services.TransactionManager
}

func NewStartRideHandler(
	repo domain.RideRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[StartRide, StartRideResult] {

	handler := &StartRideHandler{
		repo:        repo,
		outboxRepo:  outboxRepo,
		transaction: transaction,
	}

	return decorator.ApplyCommandDecorators[StartRide, StartRideResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *StartRideHandler) Handle(ctx context.Context, cmd StartRide) (StartRideResult, error) {
	var result StartRideResult
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		ride, err := h.repo.GetRideForUpdate(ctx, cmd.RideID)
		if err != nil {
			return err
		}
		if ride.DriverID == nil || *ride.DriverID != cmd.DriverID {
			return commonerrors.ErrForbidden
		}
		if ride.Status != "Matched" {
			return commonerrors.ErrConflict
		}

		startedAt := time.Now().UTC()
		if err := h.repo.MarkRideStarted(ctx, cmd.RideID, startedAt); err != nil {
			return err
		}

		event := contractsKafka.RideStartedEvent{
			RideID:    cmd.RideID,
			DriverID:  cmd.DriverID,
			StartedAt: startedAt.Format(time.RFC3339),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		if err := h.outboxRepo.Insert(ctx, &domain.OutboxMessage{
			Topic:     "ride.started",
			EventType: "RideStarted",
			Payload:   payload,
		}); err != nil {
			return err
		}

		result = StartRideResult{Status: "InProgress", StartedAt: startedAt.Format(time.RFC3339)}
		return nil
	})

	return result, err
}
