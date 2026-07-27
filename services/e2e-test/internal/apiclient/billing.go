package apiclient

import (
	"context"
	"fmt"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// BillingClient calls billing-service through Kong. Same identity model as
// RideClient: Kong derives X-Client-Id from the bearer token, so ownership
// of a payment method/invoice never needs to be spoofed by the caller.
type BillingClient struct {
	baseClient
}

func NewBillingClient(baseURL string) *BillingClient {
	return &BillingClient{newBaseClient(baseURL)}
}

func (c *BillingClient) AddPaymentMethod(ctx context.Context, accessToken string, req contracts.AddPaymentMethodRequest) (contracts.AddPaymentMethodResponse, error) {
	var resp contracts.AddPaymentMethodResponse
	err := c.doJSON(ctx, http.MethodPost, "/payment-methods", bearerHeader(accessToken), req, &resp)
	return resp, err
}

func (c *BillingClient) ListPaymentMethods(ctx context.Context, accessToken string) ([]contracts.PaymentMethodDto, error) {
	var resp []contracts.PaymentMethodDto
	err := c.doJSON(ctx, http.MethodGet, "/payment-methods", bearerHeader(accessToken), nil, &resp)
	return resp, err
}

func (c *BillingClient) GetInvoiceByRide(ctx context.Context, accessToken, rideID string) (contracts.InvoiceDto, error) {
	var resp contracts.InvoiceDto
	err := c.doJSON(ctx, http.MethodGet, "/rides/"+rideID+"/invoice", bearerHeader(accessToken), nil, &resp)
	return resp, err
}

// GetLedgerBalance is Admin-only at the Kong gateway — the cheapest
// possible regression check for the double-entry ledger invariants.
func (c *BillingClient) GetLedgerBalance(ctx context.Context, adminAccessToken, accountType, ownerID, currency string) (int64, error) {
	var resp struct {
		BalanceMinor int64 `json:"balanceMinor"`
	}
	path := fmt.Sprintf("/ledger/balance?type=%s&ownerId=%s&currency=%s", accountType, ownerID, currency)
	err := c.doJSON(ctx, http.MethodGet, path, bearerHeader(adminAccessToken), nil, &resp)
	return resp.BalanceMinor, err
}
