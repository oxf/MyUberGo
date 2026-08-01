package domain

import "context"

const (
	PaymentStatusPending    = "pending"
	PaymentStatusProcessing = "processing"
	PaymentStatusSucceeded  = "succeeded"
	PaymentStatusFailed     = "failed"
)

// Payment is one collection attempt row, never mutated in place except for
// status/failure fields — a new attempt is a new row (attempt_no+1), never
// an overwrite.
type Payment struct {
	ID                      string
	InvoiceID               string
	AttemptNo               int
	Provider                string
	ProviderPaymentIntentID *string
	PaymentMethodID         *string
	AmountMinor             int64
	Currency                string
	Status                  string
	FailureCode             *string
	FailureMessage          *string
	// IdempotencyKey is invoice:{invoice_id}:attempt:{attempt_no} —
	// deterministic, so a crashed-and-retried attempt reuses the same key
	// and cannot double-charge.
	IdempotencyKey string
	// ClaimedUntil is the in-flight claim lease (distinct from
	// invoice.next_attempt_at's retry schedule) — nil once the payment is
	// terminal, since nothing needs to claim it again.
	ClaimedUntil *string
}

type PaymentRepository interface {
	Create(ctx context.Context, p *Payment) (string, error)
	// GetNonTerminalByInvoiceID returns the invoice's most recent pending or
	// processing payment row, if any — used by ChargeWorker's claim step to
	// decide between skip (still leased by an in-flight attempt), resume
	// (lease expired, reuse the same idempotency key), or a fresh attempt
	// (no non-terminal row at all).
	GetNonTerminalByInvoiceID(ctx context.Context, invoiceID string) (*Payment, error)
	// GetByProviderIntentID is the webhook handler's lookup: a Stripe event
	// carries only the PaymentIntent id, never our own payment row id.
	GetByProviderIntentID(ctx context.Context, providerIntentID string) (*Payment, error)
	// SetClaimedUntil (re-)arms the claim lease — called at creation for a
	// fresh attempt, and again on resume to extend it.
	SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error
	// MarkProcessing/MarkSucceeded/MarkFailed are guarded to only affect a
	// row still in a non-terminal status, and report whether the guard
	// actually won (rows-affected > 0) — the same idiom as driver-service's
	// UpdateDriverStatus. This is what makes a ChargeWorker resolution and a
	// later webhook resolution of the same payment mutually safe: whoever
	// loses the race no-ops instead of double-posting the ledger.
	MarkProcessing(ctx context.Context, id string, providerPaymentIntentID string) (bool, error)
	MarkSucceeded(ctx context.Context, id string, providerPaymentIntentID string) (bool, error)
	MarkFailed(ctx context.Context, id, failureCode, failureMessage string) (bool, error)
}
