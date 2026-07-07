package handler

import (
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"driver-service/internal/application/query"
	"encoding/json"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type ShiftHandler struct {
	app app.Application
}

func NewShiftHandler(app app.Application) *ShiftHandler {
	return &ShiftHandler{app: app}
}

func (h *ShiftHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req contracts.CreateShiftRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.CreateShift.Handle(r.Context(), command.CreateShift{
		DriverID: req.DriverId,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, contracts.CreateShiftResponse{Id: result.ID})
}

func (h *ShiftHandler) Update(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req contracts.UpdateShiftRequest
	json.NewDecoder(r.Body).Decode(&req)

	err := h.app.Commands.UpdateShift.Handle(r.Context(), command.UpdateShift{
		ID: req.DriverId, Status: req.Status,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, map[string]string{"id": id})
}

func (h *ShiftHandler) GetList(w http.ResponseWriter, r *http.Request) {
	page, _ := parseIntQuery(r, "page", 0)
	pageSize, _ := parseIntQuery(r, "pageSize", 10)

	result, err := h.app.Queries.GetDriverList.Handle(r.Context(), query.GetDriverList{Page: page, PageSize: pageSize})
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

func (h *ShiftHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetDriverByID.Handle(r.Context(), query.GetDriverByID{ID: id})
	if err != nil || result == nil {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, result)
}
