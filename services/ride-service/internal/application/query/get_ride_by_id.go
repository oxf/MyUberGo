package query

import (
	"context"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetRideByID struct {
	ID string
}

type GetRideByIDHandler struct {
	repo domain.RideRepository
}

func NewGetRideByIDHandler(
	repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetRideByID, *domain.Ride] {

	handler := &GetRideByIDHandler{repo: repo}

	return decorator.ApplyQueryDecorators[GetRideByID, *domain.Ride](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetRideByIDHandler) Handle(ctx context.Context, q GetRideByID) (*domain.Ride, error) {
	return h.repo.GetRideByID(ctx, q.ID)
}
