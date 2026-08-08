package handler

import (
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"driver-service/internal/application/query"
	commonerrors "driver-service/internal/common/errors"
	"driver-service/internal/domain"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/paging"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type ShiftHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewShiftHandler(app app.Application, logger *logrus.Entry) *ShiftHandler {
	return &ShiftHandler{app: app, logger: logger}
}

func (h *ShiftHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req contracts.CreateShiftRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.CreateShift.Handle(r.Context(), command.CreateShift{
		DriverID: req.DriverId,
	})
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.CreateShiftResponse{Id: result.ID})
}

func (h *ShiftHandler) Update(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req contracts.UpdateShiftRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := h.app.Commands.UpdateShift.Handle(r.Context(), command.UpdateShift{
		ID: id, Status: req.Status,
	})
	if errors.Is(err, commonerrors.ErrNotFound) {
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (h *ShiftHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := paging.ParseListParams(r, domain.ShiftSortColumns, "startedAt")
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetShiftList.Handle(r.Context(), query.GetShiftList{
		Page: params.Page, PageSize: params.PageSize, SortBy: params.SortBy, SortDir: params.SortDir,
	})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.ShiftDto, 0, len(result.Items))
	for _, s := range result.Items {
		items = append(items, toShiftDto(s))
	}
	httpresponse.WriteJSON(w, http.StatusOK, contracts.PagedResponse[contracts.ShiftDto]{
		Items: items, Page: params.Page, PageSize: params.PageSize, TotalCount: result.TotalCount,
	})
}

func (h *ShiftHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetShiftByID.Handle(r.Context(), query.GetShiftByID{ID: id})
	if errors.Is(err, commonerrors.ErrNotFound) {
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}
	httpresponse.WriteJSON(w, http.StatusOK, toShiftDto(result))
}
