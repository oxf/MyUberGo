package domain

import "context"

type OutboxMessage struct {
	ID        string
	Topic     string
	EventType string
	Payload   []byte
	Processed bool
	Retries   int
	// TraceContext is the W3C trace context (and baggage) active when this
	// row was inserted, captured by PostgresOutboxRepository.Insert — not
	// set by callers. The outbox worker runs on its own background ticker
	// context with no link back to the HTTP request that produced this row,
	// so this is what lets the eventual Kafka publish still join that
	// request's trace instead of starting a disconnected one. See
	// github.com/oxf/MyUber/observability/obsoutbox.
	TraceContext []byte
}

type OutboxRepository interface {
	Insert(ctx context.Context, message *OutboxMessage) error
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkProcessed(ctx context.Context, id string) error
	IncrementRetries(ctx context.Context, id string) error
}

// Event interfaces for domain events
type DomainEvent interface {
	EventType() string
	Topic() string
	Payload() interface{}
}
