package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/query"
	"net/http"

	"github.com/sirupsen/logrus"
)

type LedgerHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewLedgerHandler(app app.Application, logger *logrus.Entry) *LedgerHandler {
	return &LedgerHandler{app: app, logger: logger}
}

// GetBalance is Admin-only at the Kong gateway — the cheapest regression
// check for the double-entry invariants e2e-test asserts (BILLING_SPEC.md §10).
func (h *LedgerHandler) GetBalance(w http.ResponseWriter, r *http.Request) {
	accountType := r.URL.Query().Get("type")
	currency := r.URL.Query().Get("currency")
	ownerID := r.URL.Query().Get("ownerId")
	if accountType == "" || currency == "" {
		writeError(w, "type and currency query params are required", http.StatusBadRequest)
		return
	}

	balance, err := h.app.Queries.GetLedgerBalance.Handle(r.Context(), query.GetLedgerBalance{
		AccountType: accountType, OwnerID: ownerID, Currency: currency,
	})
	if err != nil {
		writeInternalError(w, r, err, h.logger)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"type": accountType, "ownerId": ownerID, "currency": currency, "balanceMinor": balance,
	})
}
