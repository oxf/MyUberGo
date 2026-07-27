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

	contracts "github.com/oxf/MyUber/contracts/http"
)

type MatchingHandler struct {
	app app.Application
}

func NewMatchingHandler(app app.Application) *MatchingHandler {
	return &MatchingHandler{app: app}
}

// AcceptRide handles POST /rides/{rideId}/accept — the atomic first-wins claim.
func (h *MatchingHandler) AcceptRide(w http.ResponseWriter, r *http.Request) {
	rideID := r.PathValue("rideId")

	var req contracts.AcceptRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DriverId == "" {
		http.Error(w, "driverId is required", http.StatusBadRequest)
		return
	}

	err := h.app.Commands.AcceptRide.Handle(r.Context(), command.AcceptRide{
		RideID:   rideID,
		DriverID: req.DriverId,
	})
	switch {
	case errors.Is(err, cmnerrors.ErrNotFound):
		http.Error(w, "ride not found", http.StatusNotFound)
		return
	case errors.Is(err, cmnerrors.ErrOfferGone):
		http.Error(w, "offer expired, cancelled, or not offered to this driver", http.StatusBadRequest)
		return
	case errors.Is(err, cmnerrors.ErrRideTaken):
		http.Error(w, "ride already accepted by another driver", http.StatusConflict)
		return
	case err != nil:
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, contracts.AcceptRideResponse{
		RideId:   rideID,
		DriverId: req.DriverId,
		Status:   "matched",
	})
}

// GetDriverOffer handles GET /drivers/{driverId}/offer — polled by drivers
// (there is no push channel until the Notification service exists).
func (h *MatchingHandler) GetDriverOffer(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("driverId")

	offer, err := h.app.Queries.GetDriverOffer.Handle(r.Context(), query.GetDriverOffer{DriverID: driverID})
	switch {
	case errors.Is(err, cmnerrors.ErrNotFound):
		http.Error(w, "no current offer", http.StatusNotFound)
		return
	case err != nil:
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, contracts.DriverOfferDto{
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
