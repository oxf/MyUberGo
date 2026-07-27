package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/query"
	"net/http"
)

type LedgerHandler struct {
	app app.Application
}

func NewLedgerHandler(app app.Application) *LedgerHandler {
	return &LedgerHandler{app: app}
}

// GetBalance is Admin-only at the Kong gateway — the cheapest possible
// regression check for the double-entry invariants (BILLING_SPEC.md §10):
// e2e-test asserts client_receivable == fare after T1, psp_clearing ==
// fare after T2, and that a client with one EUR and one USD ride has two
// distinct balances.
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
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"type": accountType, "ownerId": ownerID, "currency": currency, "balanceMinor": balance,
	})
}
