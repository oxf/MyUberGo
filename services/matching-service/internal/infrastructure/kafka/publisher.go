package kafka

import (
	"context"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *segmentio.Writer
}

func NewPublisher(broker string) *Publisher {
	return &Publisher{
		writer: &segmentio.Writer{
			Addr:     segmentio.TCP(broker),
			Balancer: &segmentio.LeastBytes{},
			// AcceptRide publishes one ride.accepted message at a time in the
			// synchronous HTTP request path (no outbox) - it never fills a
			// batch, so kafka-go's default 1s BatchTimeout means every
			// publish reliably waits out the full second before flushing.
			// 500ms caps that worst case while still coalescing genuinely
			// concurrent publishes that land in the same window.
			BatchTimeout: 500 * time.Millisecond,
		},
	}
}

func (p *Publisher) Publish(ctx context.Context, topic string, payload []byte) error {
	return p.writer.WriteMessages(ctx, segmentio.Message{
		Topic: topic,
		Value: payload,
	})
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
