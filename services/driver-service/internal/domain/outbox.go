package domain

import "context"

type OutboxMessage struct {
	ID        string
	Topic     string
	EventType string
	Payload   []byte
	Processed bool
	Retries   int
}

type OutboxRepository interface {
	Insert(ctx context.Context, message *OutboxMessage) error
}

// Event interfaces for domain events
type DomainEvent interface {
	EventType() string
	Topic() string
	Payload() interface{}
}
