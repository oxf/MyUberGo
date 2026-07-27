package domain

import "context"

// ProviderStub is the only PaymentProvider implementation for now (D5: an
// in-process stub, not a separate container) — kept as a plain constant
// rather than an enum type so a real "stripe" provider is additive.
const ProviderStub = "stub"

type Customer struct {
	ID                 string
	ClientID           string
	Provider           string
	ProviderCustomerID string
}

// CustomerRepository manages billing.customer — one provider-side customer
// per client per provider, created lazily on first payment-method add.
type CustomerRepository interface {
	GetByClientID(ctx context.Context, clientID, provider string) (*Customer, error)
	Create(ctx context.Context, c *Customer) (string, error)
}
