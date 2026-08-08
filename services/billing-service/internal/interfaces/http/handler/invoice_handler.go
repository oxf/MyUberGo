package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/query"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"errors"
	"net/http"

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/kongheaders"
	"github.com/oxf/MyUber/common/paging"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type InvoiceHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewInvoiceHandler(app app.Application, logger *logrus.Entry) *InvoiceHandler {
	return &InvoiceHandler{app: app, logger: logger}
}

// GetByID is caller-scoped: authorized in-service against X-Client-Id, since
// Kong has no concept of invoice ownership (same pattern as cancel_ride.go).
func (h *InvoiceHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	clientID, _ := kongheaders.ClientID(r)
	id := r.PathValue("id")

	inv, err := h.app.Queries.GetInvoice.Handle(r.Context(), query.GetInvoice{ID: id})
	if errors.Is(err, commonerrors.ErrNotFound) {
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}
	if clientID == "" || inv.ClientID != clientID {
		httpresponse.WriteError(w, "forbidden", http.StatusForbidden)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, toInvoiceDto(inv))
}

// GetByRideID backs GET /rides/{rideId}/invoice, which e2e-test polls after
// completing a ride since delivery is async over Kafka plus a ChargeWorker tick.
func (h *InvoiceHandler) GetByRideID(w http.ResponseWriter, r *http.Request) {
	clientID, _ := kongheaders.ClientID(r)
	rideID := r.PathValue("rideId")

	inv, err := h.app.Queries.GetInvoiceByRide.Handle(r.Context(), query.GetInvoiceByRide{
		RideID: rideID, Type: domain.InvoiceTypeRideFare,
	})
	if errors.Is(err, commonerrors.ErrNotFound) {
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}
	if clientID == "" || inv.ClientID != clientID {
		httpresponse.WriteError(w, "forbidden", http.StatusForbidden)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, toInvoiceDto(inv))
}

// GetList is Admin-only at the Kong gateway (see gateway/kong.yml); no
// additional role check here, since Kong is the sole enforcement point.
func (h *InvoiceHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := paging.ParseListParams(r, domain.InvoiceSortColumns, "createdAt")
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.ListInvoices.Handle(r.Context(), query.ListInvoices{
		Page: params.Page, PageSize: params.PageSize, SortBy: params.SortBy, SortDir: params.SortDir,
	})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.InvoiceDto, 0, len(result.Items))
	for _, inv := range result.Items {
		items = append(items, toInvoiceDto(inv))
	}
	httpresponse.WriteJSON(w, http.StatusOK, contracts.PagedResponse[contracts.InvoiceDto]{
		Items: items, Page: params.Page, PageSize: params.PageSize, TotalCount: result.TotalCount,
	})
}
