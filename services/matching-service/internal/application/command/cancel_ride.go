package command

import (
	"context"

	"matching-service/internal/common/decorator"
	"matching-service/internal/domain"

	"github.com/sirupsen/logrus"
)

// CancelRide reacts to ride-service's ride.cancelled event.
type CancelRide struct {
	RideID   string
	DriverID *string // non-nil when the ride had already been matched
}

type CancelRideHandler struct {
	rides   domain.RideRepository
	drivers domain.DriverRepository
	offers  domain.OfferRepository
	logger  *logrus.Entry
}

func NewCancelRideHandler(
	rides domain.RideRepository,
	drivers domain.DriverRepository,
	offers domain.OfferRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[CancelRide] {
	if rides == nil || drivers == nil || offers == nil {
		panic("nil dependency")
	}
	handler := &CancelRideHandler{rides: rides, drivers: drivers, offers: offers, logger: logger}
	return decorator.ApplyCommandDecoratorsNoResult[CancelRide](handler, logger, metricsClient)
}

func (h *CancelRideHandler) Handle(ctx context.Context, cmd CancelRide) error {
	// Blocks any in-flight AcceptRide racing this cancellation, regardless of
	// whether the ride was still searching or already matched.
	if err := h.offers.SetCancelled(ctx, cmd.RideID); err != nil {
		return err
	}
	if err := h.offers.DeletePending(ctx, cmd.RideID); err != nil {
		return err
	}

	// cmd.DriverID (from ride-service's event) is NOT used here: it reflects
	// ride-service's own Postgres row at cancel time, which can lag
	// matching-service's Redis state (updated the instant AcceptRideHandler
	// runs, well before ride-service consumes the matching ride.accepted
	// event). AcceptedBy is matching-service's own live source of truth, so
	// it's what decides whether a driver needs restoring — independent of
	// whatever ride-service happened to see.
	acceptedBy, err := h.offers.AcceptedBy(ctx, cmd.RideID)
	if err != nil {
		return err
	}
	if acceptedBy != "" {
		ride, err := h.rides.GetRide(ctx, cmd.RideID)
		if err != nil {
			if h.logger != nil {
				h.logger.WithError(err).Warnf("ride.cancelled: could not look up cached rating for driver %s on ride %s, skipping re-add to drivers:online", acceptedBy, cmd.RideID)
			}
		} else if err := h.drivers.AddOnline(ctx, acceptedBy, ride.DriverRating); err != nil {
			return err
		}
	}

	return h.rides.MarkCancelled(ctx, cmd.RideID)
}
