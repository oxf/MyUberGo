// Package outbox provides the transactional-outbox worker shared by
// ride/driver/billing-service: a ticker-driven loop that claims a batch of
// unprocessed rows, publishes each to Kafka, and marks it processed (or
// increments its retry count on failure).
package outbox

import "context"

// Message is the canonical outbox row shape — identical across every
// service's own domain.OutboxMessage.
type Message struct {
	ID        string
	Topic     string
	EventType string
	Payload   []byte
	Processed bool
	Retries   int
	// ClaimedUntil is the in-flight claim lease (see SetClaimedUntil), nil
	// until a worker claims this row.
	ClaimedUntil *string
	// TraceContext is the W3C trace context active at insert (set by each
	// service's PostgresOutboxRepository, not callers) — lets the worker's
	// later Kafka publish rejoin the originating request's trace.
	TraceContext []byte
}

// Repository is the narrow structural port Worker needs — a subset of each
// service's own, wider domain.OutboxRepository (which also has Insert and
// CountByRetries, used by command handlers / main.go's gauges, not by the
// worker). Each service's existing *PostgresOutboxRepository already
// satisfies this with zero changes, via Go's structural interface typing.
type Repository interface {
	// GetUnprocessedBatch only returns rows not currently under an active
	// claim lease (see SetClaimedUntil), so a concurrent tick can't
	// double-claim.
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*Message, error)
	MarkProcessed(ctx context.Context, id string) error
	IncrementRetries(ctx context.Context, id string) error
	// SetClaimedUntil (re-)arms the claim lease so the row is excluded from
	// GetUnprocessedBatch until the lease expires.
	SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error
}

// Publisher matches each service's application/services.EventPublisher.
type Publisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}

// TransactionManager matches each service's own
// application/services.TransactionManager (and common/dbexec.
// PostgresTransactionManager already satisfies it).
type TransactionManager interface {
	WithinTransaction(ctx context.Context, fn func(context.Context) error) error
}
