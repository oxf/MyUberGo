package domain

import "context"

type OutboxMessage struct {
	ID        string
	Topic     string
	EventType string
	Payload   []byte
	Processed bool
	Retries   int
	// TraceContext is the W3C trace context active at insert (set by PostgresOutboxRepository, not
	// callers) — lets the worker's later Kafka publish rejoin the originating request's trace.
	TraceContext []byte
}

type OutboxRepository interface {
	Insert(ctx context.Context, message *OutboxMessage) error
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkProcessed(ctx context.Context, id string) error
	IncrementRetries(ctx context.Context, id string) error
	// CountByRetries splits the outbox backlog by workers.MaxRetries into pending (still retried) vs.
	// parked (needs manual triage) — backs the myubergo.outbox.{pending,parked} gauges in cmd/main.go.
	CountByRetries(ctx context.Context, maxRetries int) (pending int64, parked int64, err error)
}

// Event interfaces for domain events
type DomainEvent interface {
	EventType() string
	Topic() string
	Payload() interface{}
}
