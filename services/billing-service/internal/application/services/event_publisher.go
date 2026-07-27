package services

import "context"

type EventPublisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}
