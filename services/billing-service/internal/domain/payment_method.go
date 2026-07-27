package domain

import "context"

const (
	PaymentMethodStatusActive  = "active"
	PaymentMethodStatusExpired = "expired"
	PaymentMethodStatusRemoved = "removed"
)

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
	Create(ctx context.Context, m *PaymentMethod) (string, error)
	// ClearDefault unsets is_default on every active method for the client,
	// so setting a new default is a two-step clear-then-set inside one
	// transaction rather than relying on an upsert.
	ClearDefault(ctx context.Context, clientID string) error
	ListByClientID(ctx context.Context, clientID string) ([]*PaymentMethod, error)
	GetByID(ctx context.Context, id string) (*PaymentMethod, error)
	GetDefaultActive(ctx context.Context, clientID string) (*PaymentMethod, error)
	MarkRemoved(ctx context.Context, id string) error
}
