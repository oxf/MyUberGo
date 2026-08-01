package services

import "context"

// ChargeStatus is defined in our own vocabulary, not any specific payment
// provider's — this is the whole reason a Stripe adapter stays additive
// later instead of forcing a domain refactor (BILLING_SPEC.md §9).
type ChargeStatus string

const (
	ChargeSucceeded      ChargeStatus = "succeeded"
	ChargeProcessing     ChargeStatus = "processing"
	ChargeRequiresAction ChargeStatus = "requires_action"
	ChargeFailed         ChargeStatus = "failed"
)

type ChargeRequest struct {
	// IdempotencyKey is invoice:{invoice_id}:attempt:{attempt_no} —
	// deterministic, so a crashed-and-retried attempt reuses the same key
	// and the provider (real or stub) never double-charges.
	IdempotencyKey string
	// ProviderCustomerID is required by a real off-session charge — it's
	// what evidences the customer's prior authorization to be charged
	// without their presence. The stub ignores it.
	ProviderCustomerID      string
	ProviderPaymentMethodID string
	AmountMinor             int64
	Currency                string
}

type ChargeResult struct {
	Status           ChargeStatus
	ProviderIntentID string
	FailureCode      string
	FailureMessage   string
}

// PaymentProvider is the boundary a real Stripe adapter will implement
// later. StubProvider (infrastructure/payment/stub) is the only
// implementation for now.
type PaymentProvider interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
}
