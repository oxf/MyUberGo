package domain

import (
	"context"
	"errors"
)

// PspEvent is the webhook inbox row — one per Stripe event id, ever. See
// billing.psp_event in 0006_billing.up.sql for the full rationale.
type PspEvent struct {
	ID           string
	Type         string
	APIVersion   string
	Payload      []byte
	ReceivedAt   string
	ProcessedAt  *string
	ProcessError *string
}

// ErrDuplicatePspEvent signals the billing.psp_event id (Stripe's own event
// id) unique-violation — a redelivered webhook. Handle callers distinguish
// "already fully processed" (ProcessedAt set — true no-op) from "a prior
// delivery was interrupted before processing completed" (ProcessedAt nil —
// safe to retry the effect, since every effect downstream is itself
// idempotent/guarded).
var ErrDuplicatePspEvent = errors.New("psp event already recorded")

type PspEventRepository interface {
	// Insert returns ErrDuplicatePspEvent on a primary-key violation instead
	// of a raw driver error.
	Insert(ctx context.Context, e *PspEvent) error
	GetByID(ctx context.Context, id string) (*PspEvent, error)
	MarkProcessed(ctx context.Context, id string) error
}
