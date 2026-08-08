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

type DriverHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewDriverHandler(app app.Application, logger *logrus.Entry) *DriverHandler {
	return &DriverHandler{app: app, logger: logger}
}

func (h *DriverHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req contracts.CreateDriverDto
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.CreateDriver.Handle(r.Context(), command.CreateDriver{
		UserID: req.UserId, VehicleType: req.VehicleType, LicencePlate: req.LicencePlate,
	})
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.CreateDriverResponse{Id: result.ID})
}

func (h *DriverHandler) Update(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req contracts.UpdateDriverDto
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := h.app.Commands.UpdateDriver.Handle(r.Context(), command.UpdateDriver{
		ID: id, VehicleType: req.VehicleType, LicencePlate: req.LicencePlate,
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

func (h *DriverHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := paging.ParseListParams(r, domain.DriverSortColumns, "createdAt")
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetDriverList.Handle(r.Context(), query.GetDriverList{
		Page: params.Page, PageSize: params.PageSize, SortBy: params.SortBy, SortDir: params.SortDir,
	})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.DriverDto, 0, len(result.Items))
	for _, d := range result.Items {
		items = append(items, toDriverDto(d))
	}
	httpresponse.WriteJSON(w, http.StatusOK, contracts.PagedResponse[contracts.DriverDto]{
		Items: items, Page: params.Page, PageSize: params.PageSize, TotalCount: result.TotalCount,
	})
}

func (h *DriverHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetDriverByID.Handle(r.Context(), query.GetDriverByID{ID: id})
	if errors.Is(err, commonerrors.ErrNotFound) {
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}
	httpresponse.WriteJSON(w, http.StatusOK, toDriverDto(result))
}
