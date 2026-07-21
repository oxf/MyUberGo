package query

import (
	"context"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetRideList struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

type GetRideListHandler struct {
	repo domain.RideRepository
}

func NewGetRideListHandler(
	repo domain.RideRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetRideList, PagedResult[*domain.Ride]] {

	handler := &GetRideListHandler{repo: repo}

	return decorator.ApplyQueryDecorators[GetRideList, PagedResult[*domain.Ride]](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetRideListHandler) Handle(ctx context.Context, q GetRideList) (PagedResult[*domain.Ride], error) {
	total, err := h.repo.CountRides(ctx)
	if err != nil {
		return PagedResult[*domain.Ride]{}, err
	}

	items, err := h.repo.GetRideList(ctx, domain.PageRequest{
		Page:     q.Page,
		PageSize: q.PageSize,
		SortBy:   q.SortBy,
		SortDir:  q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.Ride]{}, err
	}

	return PagedResult[*domain.Ride]{Items: items, TotalCount: total}, nil
}
