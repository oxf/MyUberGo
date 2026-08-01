package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/query"
	commonerrors "billing-service/internal/common/errors"
	"encoding/json"
	"errors"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type PaymentMethodHandler struct {
	app app.Application
}

func NewPaymentMethodHandler(app app.Application) *PaymentMethodHandler {
	return &PaymentMethodHandler{app: app}
}

func (h *PaymentMethodHandler) Create(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-Id")
	if clientID == "" {
		writeError(w, "X-Client-Id header is required", http.StatusBadRequest)
		return
	}

	var req contracts.AddPaymentMethodRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ProviderPaymentMethodId == "" {
		writeError(w, "providerPaymentMethodId is required", http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.AddPaymentMethod.Handle(r.Context(), command.AddPaymentMethod{
		ClientID:                clientID,
		ProviderPaymentMethodID: req.ProviderPaymentMethodId,
		Brand:                   req.Brand,
		Last4:                   req.Last4,
		ExpMonth:                req.ExpMonth,
		ExpYear:                 req.ExpYear,
		SetDefault:              req.SetDefault,
	})
	if err != nil {
		writeInternalError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, contracts.AddPaymentMethodResponse{Id: result.ID})
}

func (h *PaymentMethodHandler) List(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-Id")
	if clientID == "" {
		writeError(w, "X-Client-Id header is required", http.StatusBadRequest)
		return
	}

	methods, err := h.app.Queries.ListPaymentMethods.Handle(r.Context(), query.ListPaymentMethods{ClientID: clientID})
	if err != nil {
		writeInternalError(w, err)
		return
	}

	items := make([]contracts.PaymentMethodDto, 0, len(methods))
	for _, m := range methods {
		items = append(items, toPaymentMethodDto(m))
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *PaymentMethodHandler) Delete(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-Id")
	if clientID == "" {
		writeError(w, "X-Client-Id header is required", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")

	err := h.app.Commands.RemovePaymentMethod.Handle(r.Context(), command.RemovePaymentMethod{
		ID: id, ClientID: clientID,
	})
	switch {
	case errors.Is(err, commonerrors.ErrNotFound):
		writeError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		writeError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		writeError(w, "cannot remove the only active default while invoices are open", http.StatusConflict)
		return
	case err != nil:
		writeInternalError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
