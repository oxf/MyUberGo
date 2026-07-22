package command

import (
	"context"
	"encoding/json"
	"time"

	"matching-service/internal/application/services"
	"matching-service/internal/common/decorator"
	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

// AcceptRide is a driver's claim on an offered ride. The claim itself is a
// single Redis SET NX — first writer wins, everyone else gets ErrRideTaken.
type AcceptRide struct {
	RideID   string
	DriverID string
}

type AcceptRideHandler struct {
	rides     domain.RideRepository
	drivers   domain.DriverRepository
	offers    domain.OfferRepository
	publisher services.EventPublisher
	logger    *logrus.Entry
}

func NewAcceptRideHandler(
	rides domain.RideRepository,
	drivers domain.DriverRepository,
	offers domain.OfferRepository,
	publisher services.EventPublisher,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[AcceptRide] {
	if rides == nil || drivers == nil || offers == nil || publisher == nil {
		panic("nil dependency")
	}
	handler := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: publisher, logger: logger}
	return decorator.ApplyCommandDecoratorsNoResult[AcceptRide](handler, logger, metricsClient)
}

func (h *AcceptRideHandler) Handle(ctx context.Context, cmd AcceptRide) error {
	if _, err := h.rides.GetRide(ctx, cmd.RideID); err != nil {
		return err // ErrNotFound → 404 at the HTTP layer
	}

	cancelled, err := h.offers.IsCancelled(ctx, cmd.RideID)
	if err != nil {
		return err
	}
	if cancelled {
		return cmnerrors.ErrOfferGone
	}

	// Checked before the offer-liveness check so a driver re-accepting a ride
	// they already won (their current_offer was cleared by the first accept)
	// gets ErrRideTaken (409), not ErrOfferGone (400) — "already taken" is the
	// more accurate outcome than "no live offer" once someone has won.
	acceptedBy, err := h.offers.AcceptedBy(ctx, cmd.RideID)
	if err != nil {
		return err
	}
	if acceptedBy != "" {
		return cmnerrors.ErrRideTaken
	}

	offeredRide, _, err := h.offers.CurrentOffer(ctx, cmd.DriverID)
	if err != nil {
		return err
	}
	if offeredRide != cmd.RideID {
		// Offer expired (TTL lapsed) or this ride was never offered to them.
		return cmnerrors.ErrOfferGone
	}

	won, err := h.offers.TryAccept(ctx, cmd.RideID, cmd.DriverID, AcceptClaimTTL)
	if err != nil {
		return err
	}
	if !won {
		return cmnerrors.ErrRideTaken
	}

	// Captured before RemoveOnline erases the ZSET score, and cached on the
	// ride so a later cancellation can restore the driver without a
	// cross-service lookup.
	rating, err := h.drivers.Rating(ctx, cmd.DriverID)
	if err != nil {
		return err
	}

	if err := h.rides.MarkMatched(ctx, cmd.RideID, cmd.DriverID, rating); err != nil {
		return err
	}
	if err := h.offers.DeletePending(ctx, cmd.RideID); err != nil {
		return err
	}
	if err := h.offers.ClearCurrentOffer(ctx, cmd.DriverID); err != nil {
		return err
	}
	// Matched drivers leave the pool; they re-enter on their next
	// shift.updated with status Online, or immediately if the ride gets
	// cancelled (see CancelRideHandler).
	if err := h.drivers.RemoveOnline(ctx, cmd.DriverID); err != nil {
		return err
	}

	payload, err := json.Marshal(contracts.RideAcceptedEvent{
		RideID:     cmd.RideID,
		DriverID:   cmd.DriverID,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	// Direct publish, no outbox: Redis has no transaction to hide the state
	// write and the publish behind, so a crash right here loses the event
	// (at-most-once). Acceptable for now; the match itself is durable in Redis.
	if err := h.publisher.Publish(ctx, "ride.accepted", payload); err != nil {
		if h.logger != nil {
			h.logger.WithError(err).Errorf("failed to publish ride.accepted for ride %s", cmd.RideID)
		}
	}
	return nil
}
