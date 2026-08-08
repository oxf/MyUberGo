package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/query"
	commonerrors "billing-service/internal/common/errors"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/kongheaders"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type PaymentMethodHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewPaymentMethodHandler(app app.Application, logger *logrus.Entry) *PaymentMethodHandler {
	return &PaymentMethodHandler{app: app, logger: logger}
}

func (h *PaymentMethodHandler) Create(w http.ResponseWriter, r *http.Request) {
	clientID, ok := kongheaders.RequireClientID(w, r)
	if !ok {
		return
	}

	var req contracts.AddPaymentMethodRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ProviderPaymentMethodId == "" {
		httpresponse.WriteError(w, "providerPaymentMethodId is required", http.StatusBadRequest)
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
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusCreated, contracts.AddPaymentMethodResponse{Id: result.ID})
}

func (h *PaymentMethodHandler) List(w http.ResponseWriter, r *http.Request) {
	clientID, ok := kongheaders.RequireClientID(w, r)
	if !ok {
		return
	}

	methods, err := h.app.Queries.ListPaymentMethods.Handle(r.Context(), query.ListPaymentMethods{ClientID: clientID})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.PaymentMethodDto, 0, len(methods))
	for _, m := range methods {
		items = append(items, toPaymentMethodDto(m))
	}
	httpresponse.WriteJSON(w, http.StatusOK, items)
}

func (h *PaymentMethodHandler) Delete(w http.ResponseWriter, r *http.Request) {
	clientID, ok := kongheaders.RequireClientID(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")

	err := h.app.Commands.RemovePaymentMethod.Handle(r.Context(), command.RemovePaymentMethod{
		ID: id, ClientID: clientID,
	})
	switch {
	case errors.Is(err, commonerrors.ErrNotFound):
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, commonerrors.ErrForbidden):
		httpresponse.WriteError(w, "forbidden", http.StatusForbidden)
		return
	case errors.Is(err, commonerrors.ErrConflict):
		httpresponse.WriteError(w, "cannot remove the only active default while invoices are open", http.StatusConflict)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
