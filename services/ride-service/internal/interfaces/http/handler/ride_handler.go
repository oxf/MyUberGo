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

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/kongheaders"
	"github.com/oxf/MyUber/common/paging"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type RideHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewRideHandler(app app.Application, logger *logrus.Entry) *RideHandler {
	return &RideHandler{app: app, logger: logger}
}

func (h *RideHandler) Create(w http.ResponseWriter, r *http.Request) {
	clientID, ok := kongheaders.RequireClientID(w, r)
	if !ok {
		return
	}

	var req contracts.CreateRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
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
		TariffName:    req.TariffName,
	})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.CreateRideResponse{
		RideID:              result.RideID,
		ClientID:            clientID,
		Status:              result.Status,
		EstimatedPriceMinor: result.EstimatedPriceMinor,
		Currency:            result.Currency,
		EstimatedDistanceKm: result.EstimatedDistanceKm,
		CreatedAt:           result.CreatedAt,
	})
}

func (h *RideHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	clientID, ok := kongheaders.RequireClientID(w, r)
	if !ok {
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
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		httpresponse.WriteError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		httpresponse.WriteError(w, "ride is already in a terminal state", http.StatusConflict)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.CancelRideResponse{Status: result.Status, FeeMinor: result.FeeMinor, Currency: result.Currency})
}

func (h *RideHandler) Start(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req contracts.StartRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DriverId == "" {
		httpresponse.WriteError(w, "driverId is required", http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.StartRide.Handle(r.Context(), command.StartRide{
		RideID:   id,
		DriverID: req.DriverId,
	})
	switch {
	case errors.Is(err, commonerrors.ErrNotFound):
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		httpresponse.WriteError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		httpresponse.WriteError(w, "ride is not in a startable state", http.StatusConflict)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.StartRideResponse{Status: result.Status, StartedAt: result.StartedAt})
}

func (h *RideHandler) Complete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req contracts.CompleteRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DriverId == "" {
		httpresponse.WriteError(w, "driverId is required", http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.CompleteRide.Handle(r.Context(), command.CompleteRide{
		RideID:   id,
		DriverID: req.DriverId,
	})
	switch {
	case errors.Is(err, commonerrors.ErrNotFound):
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		httpresponse.WriteError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		httpresponse.WriteError(w, "ride is not in progress", http.StatusConflict)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.CompleteRideResponse{Status: result.Status, FinishedAt: result.FinishedAt})
}

func (h *RideHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := paging.ParseListParams(r, domain.RideSortColumns, "createdAt")
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetRideList.Handle(r.Context(), query.GetRideList{
		Page: params.Page, PageSize: params.PageSize, SortBy: params.SortBy, SortDir: params.SortDir,
	})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.RideDto, 0, len(result.Items))
	for _, ride := range result.Items {
		items = append(items, toRideDto(ride))
	}
	httpresponse.WriteJSON(w, http.StatusOK, contracts.PagedResponse[contracts.RideDto]{
		Items: items, Page: params.Page, PageSize: params.PageSize, TotalCount: result.TotalCount,
	})
}

func (h *RideHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetRideByID.Handle(r.Context(), query.GetRideByID{ID: id})
	if errors.Is(err, commonerrors.ErrNotFound) || (err == nil && result == nil) {
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}
	httpresponse.WriteJSON(w, http.StatusOK, toRideDto(result))
}
