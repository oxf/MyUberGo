package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	app "matching-service/internal/application"
	"matching-service/internal/application/command"
	"matching-service/internal/application/query"
	cmnerrors "matching-service/internal/common/errors"

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/kongheaders"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type MatchingHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewMatchingHandler(app app.Application, logger *logrus.Entry) *MatchingHandler {
	return &MatchingHandler{app: app, logger: logger}
}

// AcceptRide handles POST /rides/{rideId}/accept — the atomic first-wins claim.
func (h *MatchingHandler) AcceptRide(w http.ResponseWriter, r *http.Request) {
	rideID := r.PathValue("rideId")

	callerUserID, ok := kongheaders.RequireUserID(w, r)
	if !ok {
		return
	}

	var req contracts.AcceptRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DriverId == "" {
		httpresponse.WriteError(w, "driverId is required", http.StatusBadRequest)
		return
	}

	err := h.app.Commands.AcceptRide.Handle(r.Context(), command.AcceptRide{
		RideID:       rideID,
		DriverID:     req.DriverId,
		CallerUserID: callerUserID,
	})
	switch {
	case errors.Is(err, cmnerrors.ErrForbidden):
		httpresponse.WriteError(w, "caller does not own this driver", http.StatusForbidden)
		return
	case errors.Is(err, cmnerrors.ErrNotFound):
		httpresponse.WriteError(w, "ride not found", http.StatusNotFound)
		return
	case errors.Is(err, cmnerrors.ErrOfferGone):
		httpresponse.WriteError(w, "offer expired, cancelled, or not offered to this driver", http.StatusBadRequest)
		return
	case errors.Is(err, cmnerrors.ErrRideTaken):
		httpresponse.WriteError(w, "ride already accepted by another driver", http.StatusConflict)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.AcceptRideResponse{
		RideId:   rideID,
		DriverId: req.DriverId,
		Status:   "matched",
	})
}

// GetDriverOffer handles GET /drivers/{driverId}/offer — polled by drivers
// (there is no push channel until the Notification service exists).
func (h *MatchingHandler) GetDriverOffer(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("driverId")

	callerUserID, ok := kongheaders.RequireUserID(w, r)
	if !ok {
		return
	}

	offer, err := h.app.Queries.GetDriverOffer.Handle(r.Context(), query.GetDriverOffer{
		DriverID:     driverID,
		CallerUserID: callerUserID,
	})
	switch {
	case errors.Is(err, cmnerrors.ErrForbidden):
		httpresponse.WriteError(w, "caller does not own this driver", http.StatusForbidden)
		return
	case errors.Is(err, cmnerrors.ErrNotFound):
		httpresponse.WriteError(w, "no current offer", http.StatusNotFound)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.DriverOfferDto{
		RideId:             offer.Ride.RideID,
		PickupLat:          offer.Ride.PickupLat,
		PickupLng:          offer.Ride.PickupLng,
		PickupAddress:      offer.Ride.PickupAddress,
		DestinationLat:     offer.Ride.DestinationLat,
		DestinationLng:     offer.Ride.DestinationLng,
		DestinationAddress: offer.Ride.DestinationAddress,
		DistanceKm:         offer.Ride.DistanceKm,
		PriceMinor:         offer.Ride.PriceMinor,
		Currency:           offer.Ride.Currency,
		ExpiresAt:          offer.ExpiresAt.UTC().Format(time.RFC3339),
	})
}
