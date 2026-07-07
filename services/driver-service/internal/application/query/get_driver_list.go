package query

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetDriverList struct {
	Page     int
	PageSize int
}

type GetDriverListHandler struct {
	repo domain.DriverProfileRepository
}

func NewGetDriverListHandler(repo domain.DriverProfileRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetDriverList, []*domain.DriverProfile] {
	handler := &GetDriverListHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetDriverList, []*domain.DriverProfile](handler, logger, metricsClient)
}

func (h *GetDriverListHandler) Handle(ctx context.Context, q GetDriverList) ([]*domain.DriverProfile, error) {
	return h.repo.GetDriverProfileList(ctx, q.Page, q.PageSize)
}
