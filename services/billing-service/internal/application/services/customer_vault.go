package services

import "context"

// PaymentMethodDetails is the display metadata read back from the provider
// after attaching a payment method — never trust what the caller claims in
// the request body (brand/last4 of a self-asserted token are unverifiable).
type PaymentMethodDetails struct {
	Brand    string
	Last4    string
	ExpMonth int
	ExpYear  int
}

// CustomerVault is deliberately its own port, separate from PaymentProvider
// (Interface Segregation): ChargeWorker only ever needs Charge, and
// AddPaymentMethodHandler only ever needs this — neither should depend on a
// method it never calls. StubProvider and StripeProvider each implement
// both ports from one adapter struct.
type CustomerVault interface {
	// EnsureCustomer get-or-creates the provider-side customer for a client,
	// idempotently, and returns its provider customer id.
	EnsureCustomer(ctx context.Context, clientID string) (providerCustomerID string, err error)
	// AttachPaymentMethod attaches a payment method to a provider customer
	// and returns the provider's own view of its display metadata.
	AttachPaymentMethod(ctx context.Context, providerCustomerID, providerPaymentMethodID string) (PaymentMethodDetails, error)
}
