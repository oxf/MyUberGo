package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/query"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"encoding/json"
	"errors"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type InvoiceHandler struct {
	app app.Application
}

func NewInvoiceHandler(app app.Application) *InvoiceHandler {
	return &InvoiceHandler{app: app}
}

// GetByID is caller-scoped: authorized in-service against X-Client-Id
// (Kong has no concept of invoice ownership) — the same pattern
// ride-service's cancel_ride.go uses for ride ownership.
func (h *InvoiceHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-Id")
	id := r.PathValue("id")

	inv, err := h.app.Queries.GetInvoice.Handle(r.Context(), query.GetInvoice{ID: id})
	if errors.Is(err, commonerrors.ErrNotFound) {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if clientID == "" || inv.ClientID != clientID {
		writeError(w, "forbidden", http.StatusForbidden)
		return
	}

	writeJSON(w, http.StatusOK, toInvoiceDto(inv))
}

// GetByRideID backs GET /rides/{rideId}/invoice — the endpoint e2e-test
// polls after completing a ride, since delivery is async over Kafka plus a
// ChargeWorker tick.
func (h *InvoiceHandler) GetByRideID(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-Id")
	rideID := r.PathValue("rideId")

	inv, err := h.app.Queries.GetInvoiceByRide.Handle(r.Context(), query.GetInvoiceByRide{
		RideID: rideID, Type: domain.InvoiceTypeRideFare,
	})
	if errors.Is(err, commonerrors.ErrNotFound) {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if clientID == "" || inv.ClientID != clientID {
		writeError(w, "forbidden", http.StatusForbidden)
		return
	}

	writeJSON(w, http.StatusOK, toInvoiceDto(inv))
}

// GetList is Admin-only at the Kong gateway (see gateway/kong.yml) — no
// additional role check here, matching every other admin-list endpoint in
// this repo (Kong is the sole enforcement point).
func (h *InvoiceHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, domain.InvoiceSortColumns, "createdAt")
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.ListInvoices.Handle(r.Context(), query.ListInvoices{
		Page: params.page, PageSize: params.pageSize, SortBy: params.sortBy, SortDir: params.sortDir,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	items := make([]contracts.InvoiceDto, 0, len(result.Items))
	for _, inv := range result.Items {
		items = append(items, toInvoiceDto(inv))
	}
	writeJSON(w, http.StatusOK, contracts.PagedResponse[contracts.InvoiceDto]{
		Items: items, Page: params.page, PageSize: params.pageSize, TotalCount: result.TotalCount,
	})
}

func writeError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
