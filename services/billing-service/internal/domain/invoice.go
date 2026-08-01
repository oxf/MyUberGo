package domain

import (
	"context"
	"errors"
)

const (
	InvoiceTypeRideFare        = "ride_fare"
	InvoiceTypeCancellationFee = "cancellation_fee"
)

const (
	InvoiceStatusOpen          = "open"
	InvoiceStatusPaid          = "paid"
	InvoiceStatusUncollectible = "uncollectible"
	InvoiceStatusVoid          = "void"
)

const (
	InvoiceLineKindBaseFare        = "base_fare"
	InvoiceLineKindDistance        = "distance"
	InvoiceLineKindTime            = "time"
	InvoiceLineKindCancellationFee = "cancellation_fee"
	InvoiceLineKindPlatformFee     = "platform_fee"
)

type InvoiceLine struct {
	Kind        string
	AmountMinor int64
	Currency    string
	Description string
}

type Invoice struct {
	ID          string
	RideID      string
	ClientID    string
	DriverID    *string
	Type        string
	Status      string
	AmountMinor int64
	Currency    string
	// AttemptCount is derived at read time (a correlated subquery over
	// billing.payment, COUNT(*) WHERE invoice_id=...) — not a stored
	// column. Consistent with the ledger's own "never a stored column,
	// compute at query time" rule. Populated by every query method below;
	// there is deliberately no writer method on this interface.
	AttemptCount  int
	NextAttemptAt *string // RFC3339 — the retry SCHEDULE only; see billing.payment.ClaimedUntil for the claim lease
	CreatedAt     string
	PaidAt        *string
	Lines         []InvoiceLine
}

// ErrDuplicateInvoice signals the billing.invoice (ride_id, type)
// unique-violation — the idempotency guard against a redelivered
// ride.completed/ride.cancelled Kafka event. Callers treat this as a no-op
// success (ack the message), never a failure.
var ErrDuplicateInvoice = errors.New("invoice already exists for this ride and type")

type InvoiceRepository interface {
	// Create inserts the invoice and its lines in one call; callers run it
	// inside a transaction alongside the T1 ledger posting. Returns
	// ErrDuplicateInvoice on a (ride_id, type) violation instead of erroring
	// the caller into treating a redelivered event as a hard failure.
	Create(ctx context.Context, inv *Invoice) (string, error)
	GetByID(ctx context.Context, id string) (*Invoice, error)
	GetByRideID(ctx context.Context, rideID, invoiceType string) (*Invoice, error)
	GetList(ctx context.Context, req PageRequest) ([]*Invoice, error)
	CountInvoices(ctx context.Context) (int, error)
	CountOpenByClientID(ctx context.Context, clientID string) (int, error)
	// GetDueForCharge locks (FOR UPDATE SKIP LOCKED) open invoices whose
	// next_attempt_at has elapsed, for the ChargeWorker sweep.
	GetDueForCharge(ctx context.Context, limit int) ([]*Invoice, error)
	// MarkPaid is guarded to WHERE status='open' and reports whether the
	// guard won (rows-affected > 0) — see PaymentRepository.MarkSucceeded
	// for why this matters once a webhook can race the worker for the same
	// invoice.
	MarkPaid(ctx context.Context, id, paidAt string) (bool, error)
	// SetNextAttemptAt arms (non-nil) or clears (nil) the retry schedule —
	// used by the finalize step (to schedule the next retry, or to stop
	// sweeping while a webhook-only "processing" outcome is pending).
	SetNextAttemptAt(ctx context.Context, id string, nextAttemptAt *string) error
	MarkUncollectible(ctx context.Context, id string) (bool, error)
}
