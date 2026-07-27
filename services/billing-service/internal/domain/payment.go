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
}

type PaymentRepository interface {
	Create(ctx context.Context, p *Payment) (string, error)
	MarkSucceeded(ctx context.Context, id string, providerPaymentIntentID string) error
	MarkFailed(ctx context.Context, id, failureCode, failureMessage string) error
}
