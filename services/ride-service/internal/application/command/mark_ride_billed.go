package command

import (
	"context"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type MarkRideBilled struct {
	RideID    string
	InvoiceID string
}

type MarkRideBilledHandler struct {
	repo domain.RideRepository
}

func NewMarkRideBilledHandler(
	repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[MarkRideBilled] {

	handler := &MarkRideBilledHandler{repo: repo}

	return decorator.ApplyCommandDecoratorsNoResult[MarkRideBilled](
		handler,
		logger,
		metricsClient,
	)
}

func (h *MarkRideBilledHandler) Handle(ctx context.Context, cmd MarkRideBilled) error {
	return h.repo.MarkRideBilled(ctx, cmd.RideID, cmd.InvoiceID)
}
