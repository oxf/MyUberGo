package query

import (
	"context"

	"matching-service/internal/common/decorator"
	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetDriverOffer struct {
	DriverID string
}

type GetDriverOfferHandler struct {
	rides  domain.RideRepository
	offers domain.OfferRepository
}

func NewGetDriverOfferHandler(
	rides domain.RideRepository,
	offers domain.OfferRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetDriverOffer, *domain.DriverOffer] {
	if rides == nil || offers == nil {
		panic("nil repo")
	}
	handler := &GetDriverOfferHandler{rides: rides, offers: offers}
	return decorator.ApplyQueryDecorators[GetDriverOffer, *domain.DriverOffer](handler, logger, metricsClient)
}

func (h *GetDriverOfferHandler) Handle(ctx context.Context, q GetDriverOffer) (*domain.DriverOffer, error) {
	rideID, expiresAt, err := h.offers.CurrentOffer(ctx, q.DriverID)
	if err != nil {
		return nil, err
	}
	if rideID == "" {
		return nil, cmnerrors.ErrNotFound
	}
	ride, err := h.rides.GetRide(ctx, rideID)
	if err != nil {
		return nil, err
	}
	return &domain.DriverOffer{Ride: *ride, ExpiresAt: expiresAt}, nil
}
