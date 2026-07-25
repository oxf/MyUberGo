package handler

import (
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"driver-service/internal/application/query"
	commonerrors "driver-service/internal/common/errors"
	"driver-service/internal/domain"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type DriverHandler struct {
	app app.Application
}

func NewDriverHandler(app app.Application) *DriverHandler {
	return &DriverHandler{app: app}
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
	json.NewDecoder(r.Body).Decode(&req)

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
		writeError(w, err.Error(), http.StatusInternalServerError)
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
	if err != nil || result == nil {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, toDriverDto(result))
}

func writeError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func parseIntQuery(r *http.Request, key string, defaultValue int) (int, error) {
	valStr := r.URL.Query().Get(key)

	if valStr == "" {
		return defaultValue, nil
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, err
	}

	if val < 0 {
		return 0, fmt.Errorf("%s cannot be negative", key)
	}

	return val, nil
}
