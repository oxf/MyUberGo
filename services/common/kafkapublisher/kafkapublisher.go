// Package kafkapublisher wraps a segmentio kafka-go Writer with trace-header
// injection — the EventPublisher every producing service (ride/driver/
// billing/matching) constructs identically in cmd/main.go.
package kafkapublisher

import (
	"context"
	"time"

	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
}

// Option configures the underlying kafka.Writer.
type Option func(*kafka.Writer)

// WithBatchTimeout overrides kafka-go's default 1s BatchTimeout — needed by
// callers that publish one message at a time synchronously (e.g.
// matching-service's AcceptRide, no outbox), which never fill a batch and
// would otherwise wait out the full default window before flushing.
func WithBatchTimeout(d time.Duration) Option {
	return func(w *kafka.Writer) { w.BatchTimeout = d }
}

func New(broker string, opts ...Option) *Publisher {
	w := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Balancer: &kafka.LeastBytes{},
	}
	for _, opt := range opts {
		opt(w)
	}
	return &Publisher{writer: w}
}

func (p *Publisher) Publish(ctx context.Context, topic string, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Value:   payload,
		Headers: obskafka.Inject(ctx),
	})
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
