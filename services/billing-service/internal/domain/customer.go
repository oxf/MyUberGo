package domain

import (
	"context"
	"errors"
)

// ProviderStub is the in-process stub PaymentProvider (D5). ProviderStripe
// is the real adapter (sandbox/test-mode only). Both are plain constants
// rather than an enum type, by design — see BILLING_SPEC.md §9.
const (
	ProviderStub   = "stub"
	ProviderStripe = "stripe"
)

type Customer struct {
	ID                 string
	ClientID           string
	Provider           string
	ProviderCustomerID string
}

// ErrCustomerExists signals the billing.customer (client_id, provider)
// unique-violation — two concurrent first-time AddPaymentMethod calls both
// found no row and both created one with the provider. The loser re-reads
// instead of erroring; the DB constraint is the actual source of truth.
var ErrCustomerExists = errors.New("customer already exists for this client and provider")

// CustomerRepository manages billing.customer — one provider-side customer
// per client per provider, created lazily on first payment-method add.
type CustomerRepository interface {
	GetByClientID(ctx context.Context, clientID, provider string) (*Customer, error)
	// Create returns ErrCustomerExists on a (client_id, provider)
	// unique-violation instead of a raw driver error.
	Create(ctx context.Context, c *Customer) (string, error)
}
