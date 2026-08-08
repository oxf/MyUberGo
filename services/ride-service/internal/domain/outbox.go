package domain

import (
	"context"

	"github.com/oxf/MyUber/common/outbox"
)

// OutboxMessage is an alias for the canonical row shape shared by every
// service's outbox table — see common/outbox.Message.
type OutboxMessage = outbox.Message

type OutboxRepository interface {
	Insert(ctx context.Context, message *OutboxMessage) error
	// GetUnprocessedBatch only returns rows not currently under an active claim
	// lease (see SetClaimedUntil), so a concurrent tick can't double-claim.
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkProcessed(ctx context.Context, id string) error
	IncrementRetries(ctx context.Context, id string) error
	// SetClaimedUntil (re-)arms the claim lease so the row is excluded from
	// GetUnprocessedBatch until the lease expires — same concept as
	// billing.payment.claimed_until.
	SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error
	// CountByRetries splits the outbox backlog by workers.MaxRetries: pending rows will
	// retry, parked rows exceeded the cap and need manual triage. Backs the pending/parked gauges.
	CountByRetries(ctx context.Context, maxRetries int) (pending int64, parked int64, err error)
}
