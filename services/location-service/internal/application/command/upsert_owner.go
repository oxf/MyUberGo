package command

import (
	"context"

	"location-service/internal/common/decorator"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

// UpsertOwner caches the driver<->user mapping from a shift.updated event, in
// both directions — idempotent (plain SET), safe against redelivery.
type UpsertOwner struct {
	DriverID string
	UserID   string
}

type UpsertOwnerHandler struct {
	owner domain.OwnerRepository
}

func NewUpsertOwnerHandler(
	owner domain.OwnerRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[UpsertOwner] {
	if owner == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &UpsertOwnerHandler{owner: owner}
	return decorator.ApplyCommandDecoratorsNoResult[UpsertOwner](handler, logger, metricsClient)
}

func (h *UpsertOwnerHandler) Handle(ctx context.Context, cmd UpsertOwner) error {
	// An empty UserID must never be cached: this repo also writes a *reverse*
	// key (loc:user:{userId}:driver), which empty UserID would collide.
	if cmd.DriverID == "" || cmd.UserID == "" {
		return nil
	}
	return h.owner.SetOwner(ctx, cmd.DriverID, cmd.UserID)
}
