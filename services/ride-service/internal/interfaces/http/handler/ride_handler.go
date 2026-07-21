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
