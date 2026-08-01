package domain

import (
	"context"
	"errors"
)

const (
	PaymentMethodStatusActive  = "active"
	PaymentMethodStatusExpired = "expired"
	PaymentMethodStatusRemoved = "removed"
)

// ErrPaymentMethodExists signals the billing.payment_method
// (client_id, provider, provider_payment_method_id) unique-violation — a
// retried/double-submitted attach of the same underlying card. Callers
// treat this as a no-op success (re-read and return the existing row),
// the same idiom as ErrDuplicateInvoice/ErrCustomerExists.
var ErrPaymentMethodExists = errors.New("payment method already attached for this client and provider")

// PaymentMethod never carries a PAN/CVC/full expiry-plus-number — brand and
// last4 are display metadata sourced from the payment provider.
type PaymentMethod struct {
	ID                      string
	ClientID                string
	Provider                string
	ProviderPaymentMethodID string
	Brand                   string
	Last4                   string
	ExpMonth                int
	ExpYear                 int
	IsDefault               bool
	Status                  string
	CreatedAt               string
}

type PaymentMethodRepository interface {
	// Create returns ErrPaymentMethodExists on a
	// (client_id, provider, provider_payment_method_id) unique-violation
	// instead of a raw driver error.
	Create(ctx context.Context, m *PaymentMethod) (string, error)
	// ClearDefault unsets is_default on every active method for the client,
	// so setting a new default is a two-step clear-then-set inside one
	// transaction rather than relying on an upsert.
	ClearDefault(ctx context.Context, clientID string) error
	ListByClientID(ctx context.Context, clientID string) ([]*PaymentMethod, error)
	GetByID(ctx context.Context, id string) (*PaymentMethod, error)
	GetDefaultActive(ctx context.Context, clientID string) (*PaymentMethod, error)
	// GetActiveByProviderID re-reads the existing row after an
	// ErrPaymentMethodExists violation, so a retried attach returns the
	// same id instead of erroring.
	GetActiveByProviderID(ctx context.Context, clientID, provider, providerPaymentMethodID string) (*PaymentMethod, error)
	MarkRemoved(ctx context.Context, id string) error
}
