package domain

import "context"

type OutboxMessage struct {
	ID        string
	Topic     string
	EventType string
	Payload   []byte
	Processed bool
	Retries   int
	// TraceContext is the W3C trace context active when this row was inserted (set by the repository, not
	// callers) — lets the eventual Kafka publish join the original request's trace. See observability/obsoutbox.
	TraceContext []byte
}

type OutboxRepository interface {
	Insert(ctx context.Context, message *OutboxMessage) error
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkProcessed(ctx context.Context, id string) error
	IncrementRetries(ctx context.Context, id string) error
	// CountByRetries splits the outbox backlog by workers.MaxRetries: pending rows will still be retried, parked
	// ones exceeded the cap and need manual triage. Backs the myubergo.outbox.{pending,parked} gauges in cmd/main.go.
	CountByRetries(ctx context.Context, maxRetries int) (pending int64, parked int64, err error)
}
