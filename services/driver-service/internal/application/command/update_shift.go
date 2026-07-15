package command

import (
	"context"
	"driver-service/internal/application/services"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"
	"encoding/json"
	"time"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type UpdateShift struct {
	ID     string
	Status string
}

type UpdateShiftHandler struct {
	repo        domain.ShiftRepository
	outboxRepo  domain.OutboxRepository
	transaction services.TransactionManager
}

func NewUpdateShiftHandler(
	repo domain.ShiftRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[UpdateShift] {

	handler := &UpdateShiftHandler{
		repo:        repo,
		outboxRepo:  outboxRepo,
		transaction: transaction,
	}

	return decorator.ApplyCommandDecoratorsNoResult[UpdateShift](
		handler,
		logger,
		metricsClient,
	)
}

func (h *UpdateShiftHandler) Handle(
	ctx context.Context,
	cmd UpdateShift,
) error {

	if cmd.Status == "Ended" {
		return h.repo.EndShift(ctx, cmd.ID)
	}

	return h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {

		shift, err := h.repo.GetShiftByID(ctx, cmd.ID)
		if err != nil {
			return err
		}

		// ideally:
		// shift.UpdateStatus(cmd.Status)
		shift.Status = cmd.Status

		if err := h.repo.UpdateShift(ctx, shift); err != nil {
			return err
		}

		event := &contracts.ShiftUpdatedEvent{
			ShiftID:   shift.ID,
			DriverID:  shift.DriverID,
			Status:    cmd.Status,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		return h.outboxRepo.Insert(
			ctx,
			&domain.OutboxMessage{
				Topic:     "shift.updated",
				EventType: "ShiftUpdatedEvent",
				Payload:   payload,
			},
		)
	})
}
