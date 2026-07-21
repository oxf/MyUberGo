package kafka

import (
	"context"

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
