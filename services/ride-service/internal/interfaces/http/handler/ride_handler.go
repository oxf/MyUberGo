package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"
	"ride-service/internal/application/query"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type RideHandler struct {
	app app.Application
}

func NewRideHandler(app app.Application) *RideHandler {
	return &RideHandler{app: app}
}

func (h *RideHandler) Create(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-User-Id")
	if clientID == "" {
		writeError(w, "X-User-Id header is required", http.StatusBadRequest)
		return
	}

	var req contracts.CreateRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.CreateRide.Handle(r.Context(), command.CreateRide{
		ClientID:      clientID,
		PickupLat:     req.PickupLat,
		PickupLng:     req.PickupLng,
		PickupAddress: req.PickupAddress,
		DestLat:       req.DestLat,
		DestLng:       req.DestLng,
		DestAddress:   req.DestAddress,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, contracts.CreateRideResponse{
		RideID:              result.RideID,
		ClientID:            clientID,
		Status:              result.Status,
		EstimatedPrice:      result.EstimatedPrice,
		EstimatedDistanceKm: result.EstimatedDistanceKm,
		CreatedAt:           result.CreatedAt,
	})
}

func (h *RideHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-User-Id")
	if clientID == "" {
		writeError(w, "X-User-Id header is required", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")

	var req contracts.CancelRideRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req) // empty/absent body is fine
	}

	result, err := h.app.Commands.CancelRide.Handle(r.Context(), command.CancelRide{
		RideID:   id,
		ClientID: clientID,
		Reason:   req.Reason,
	})
	switch {
	case errors.Is(err, commonerrors.ErrNotFound):
		writeError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		writeError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		writeError(w, "ride is already in a terminal state", http.StatusConflict)
		return
	case err != nil:
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, contracts.CancelRideResponse{Status: result.Status, Fee: result.Fee})
}

func (h *RideHandler) Start(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req contracts.StartRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DriverId == "" {
		writeError(w, "driverId is required", http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.StartRide.Handle(r.Context(), command.StartRide{
		RideID:   id,
		DriverID: req.DriverId,
	})
	switch {
	case errors.Is(err, commonerrors.ErrNotFound):
		writeError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		writeError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		writeError(w, "ride is not in a startable state", http.StatusConflict)
		return
	case err != nil:
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, contracts.StartRideResponse{Status: result.Status, StartedAt: result.StartedAt})
}

func (h *RideHandler) Complete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req contracts.CompleteRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DriverId == "" {
		writeError(w, "driverId is required", http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.CompleteRide.Handle(r.Context(), command.CompleteRide{
		RideID:   id,
		DriverID: req.DriverId,
	})
	switch {
	case errors.Is(err, commonerrors.ErrNotFound):
		writeError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		writeError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		writeError(w, "ride is not in progress", http.StatusConflict)
		return
	case err != nil:
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, contracts.CompleteRideResponse{Status: result.Status, FinishedAt: result.FinishedAt})
}

func (h *RideHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, domain.RideSortColumns, "createdAt")
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetRideList.Handle(r.Context(), query.GetRideList{
		Page: params.page, PageSize: params.pageSize, SortBy: params.sortBy, SortDir: params.sortDir,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	items := make([]contracts.RideDto, 0, len(result.Items))
	for _, ride := range result.Items {
		items = append(items, toRideDto(ride))
	}
	writeJSON(w, contracts.PagedResponse[contracts.RideDto]{
		Items: items, Page: params.page, PageSize: params.pageSize, TotalCount: result.TotalCount,
	})
}

func (h *RideHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetRideByID.Handle(r.Context(), query.GetRideByID{ID: id})
	if errors.Is(err, commonerrors.ErrNotFound) || (err == nil && result == nil) {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, toRideDto(result))
}

func writeError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
