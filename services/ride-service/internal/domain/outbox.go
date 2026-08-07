package domain

import "context"

type OutboxMessage struct {
	ID        string
	Topic     string
	EventType string
	Payload   []byte
	Processed bool
	Retries   int
	// TraceContext is the W3C trace context active when this row was inserted (captured
	// by Insert), so the worker's later Kafka publish can still join the originating request's trace.
	TraceContext []byte
}

type OutboxRepository interface {
	Insert(ctx context.Context, message *OutboxMessage) error
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkProcessed(ctx context.Context, id string) error
	IncrementRetries(ctx context.Context, id string) error
	// CountByRetries splits the outbox backlog by workers.MaxRetries: pending rows will
	// retry, parked rows exceeded the cap and need manual triage. Backs the pending/parked gauges.
	CountByRetries(ctx context.Context, maxRetries int) (pending int64, parked int64, err error)
}
