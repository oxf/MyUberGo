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
	// CallerUserID is the caller's own auth.user(id) (Kong's injected
	// X-User-Id). Must match DriverID's cached userId or Handle returns
	// ErrForbidden — see CLAUDE.md's Kong route note for the rollout caveat.
	CallerUserID string
}

type GetDriverOfferHandler struct {
	rides   domain.RideRepository
	offers  domain.OfferRepository
	drivers domain.DriverRepository
}

func NewGetDriverOfferHandler(
	rides domain.RideRepository,
	offers domain.OfferRepository,
	drivers domain.DriverRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetDriverOffer, *domain.DriverOffer] {
	if rides == nil || offers == nil || drivers == nil {
		panic("nil repo")
	}
	handler := &GetDriverOfferHandler{rides: rides, offers: offers, drivers: drivers}
	return decorator.ApplyQueryDecorators[GetDriverOffer, *domain.DriverOffer](handler, logger, metricsClient)
}

func (h *GetDriverOfferHandler) Handle(ctx context.Context, q GetDriverOffer) (*domain.DriverOffer, error) {
	ownerUserID, err := h.drivers.GetUserID(ctx, q.DriverID)
	if err != nil {
		return nil, err
	}
	if ownerUserID == "" || ownerUserID != q.CallerUserID {
		return nil, cmnerrors.ErrForbidden
	}

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
