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
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.CreateDriver.Handle(r.Context(), command.CreateDriver{
		UserID: req.UserId, VehicleType: req.VehicleType, LicencePlate: req.LicencePlate,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, contracts.CreateDriverResponse{Id: result.ID})
}

func (h *DriverHandler) Update(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req contracts.UpdateDriverDto
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := h.app.Commands.UpdateDriver.Handle(r.Context(), command.UpdateDriver{
		ID: id, VehicleType: req.VehicleType, LicencePlate: req.LicencePlate,
	})
	if errors.Is(err, commonerrors.ErrNotFound) {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, map[string]string{"id": id})
}

func (h *DriverHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, domain.DriverSortColumns, "createdAt")
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetDriverList.Handle(r.Context(), query.GetDriverList{
		Page: params.page, PageSize: params.pageSize, SortBy: params.sortBy, SortDir: params.sortDir,
	})
	if err != nil {
		writeInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.DriverDto, 0, len(result.Items))
	for _, d := range result.Items {
		items = append(items, toDriverDto(d))
	}
	writeJSON(w, contracts.PagedResponse[contracts.DriverDto]{
		Items: items, Page: params.page, PageSize: params.pageSize, TotalCount: result.TotalCount,
	})
}

func (h *DriverHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetDriverByID.Handle(r.Context(), query.GetDriverByID{ID: id})
	if errors.Is(err, commonerrors.ErrNotFound) {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		writeInternalError(w, r, err, h.logger)
		return
	}
	writeJSON(w, toDriverDto(result))
}

func writeError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

// writeInternalError logs the real error server-side and returns a generic message to the client —
// raw error text must never reach an HTTP response. Shared by driver_handler.go and shift_handler.go.
func writeInternalError(w http.ResponseWriter, r *http.Request, err error, logger *logrus.Entry) {
	logger.WithContext(r.Context()).WithError(err).Error("internal error")
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
