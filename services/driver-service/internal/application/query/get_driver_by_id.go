package query

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetDriverByID struct {
	ID string
}

type GetDriverByIDHandler struct {
	repo domain.DriverProfileRepository
}

func NewGetDriverByIDHandler(repo domain.DriverProfileRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetDriverByID, *domain.DriverProfile] {
	handler := &GetDriverByIDHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetDriverByID, *domain.DriverProfile](handler, logger, metricsClient)
}

func (h *GetDriverByIDHandler) Handle(ctx context.Context, q GetDriverByID) (*domain.DriverProfile, error) {
	return h.repo.GetDriverProfileByID(ctx, q.ID)
}
