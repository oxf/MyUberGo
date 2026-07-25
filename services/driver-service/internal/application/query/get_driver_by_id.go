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
	repo domain.DriverRepository
}

func NewGetDriverByIDHandler(repo domain.DriverRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetDriverByID, *domain.Driver] {
	handler := &GetDriverByIDHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetDriverByID, *domain.Driver](handler, logger, metricsClient)
}

func (h *GetDriverByIDHandler) Handle(ctx context.Context, q GetDriverByID) (*domain.Driver, error) {
	return h.repo.GetDriverByID(ctx, q.ID)
}
