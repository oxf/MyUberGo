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
	"go.opentelemetry.io/otel/attribute"
)

type UpdateShift struct {
	ID     string
	Status string
}

type UpdateShiftHandler struct {
	repo        domain.ShiftRepository
	profileRepo domain.DriverRepository
	outboxRepo  domain.OutboxRepository
	transaction services.TransactionManager
	logger      *logrus.Entry
	metrics     decorator.MetricsClient
}

func NewUpdateShiftHandler(
	repo domain.ShiftRepository,
	profileRepo domain.DriverRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[UpdateShift] {

	handler := &UpdateShiftHandler{
		repo:        repo,
		profileRepo: profileRepo,
		outboxRepo:  outboxRepo,
		transaction: transaction,
		logger:      logger,
		metrics:     metricsClient,
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

	return h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {

		shift, err := h.repo.GetShiftByID(ctx, cmd.ID)
		if err != nil {
			return err
		}

		if cmd.Status == "Ended" {
			// Previously this returned early without an outbox row, so
			// matching-service never learned a driver went offline.
			if err := h.repo.EndShift(ctx, cmd.ID); err != nil {
				return err
			}
		} else {
			shift.Status = cmd.Status
			if err := h.repo.UpdateShift(ctx, shift); err != nil {
				return err
			}
		}

		profile, err := h.profileRepo.GetDriverByID(ctx, shift.DriverID)
		if err != nil {
			return err
		}

		switch cmd.Status {
		case "Online":
			if changed, err := h.profileRepo.UpdateDriverStatus(ctx, shift.DriverID, "Offline", "Online"); err != nil {
				return err
			} else if !changed {
				h.logger.Warnf("shift %s: driver %s not flipped Offline->Online (not currently Offline)", cmd.ID, shift.DriverID)
			} else if h.metrics != nil {
				h.metrics.IncCounter(ctx, "myubergo.shifts.started")
				h.metrics.IncCounter(ctx, "myubergo.driver.status_transitions",
					attribute.String("from", "Offline"), attribute.String("to", "Online"))
			}
		case "Ended":
			// Guarded so a driver who's OnRide when their shift ends stays
			// OnRide rather than being silently downgraded to Offline
			// mid-trip - same "don't force-touch a driver mid-ride"
			// philosophy already applied to cancellation.
			if changed, err := h.profileRepo.UpdateDriverStatus(ctx, shift.DriverID, "Online", "Offline"); err != nil {
				return err
			} else if !changed {
				h.logger.Warnf("shift %s: driver %s not flipped Online->Offline (not currently Online, e.g. still OnRide)", cmd.ID, shift.DriverID)
			} else if h.metrics != nil {
				h.metrics.IncCounter(ctx, "myubergo.shifts.ended")
				h.metrics.IncCounter(ctx, "myubergo.driver.status_transitions",
					attribute.String("from", "Online"), attribute.String("to", "Offline"))
			}
		}

		event := &contracts.ShiftUpdatedEvent{
			ShiftID:   shift.ID,
			DriverID:  shift.DriverID,
			Status:    cmd.Status,
			Rating:    profile.Rating,
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
