package consumers

import (
	"context"
	"encoding/json"
	"log"
	app "matching-service/internal/application"
	"matching-service/internal/application/command"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/segmentio/kafka-go"
)

type ShiftUpdatedConsumer struct {
	app    app.Application
	broker string
}

func NewShiftUpdatedConsumer(app app.Application, broker string) *ShiftUpdatedConsumer {
	return &ShiftUpdatedConsumer{app: app, broker: broker}
}

func (c *ShiftUpdatedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "matching-service",
	})
	defer reader.Close()

	log.Printf("%s consumer started", topic)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("consumer error:", err)
			continue
		}

		var event contractsKafka.ShiftUpdatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf(
			"Shift updated received. ShiftID=%s DriverID=%s Status=%s",
			event.ShiftID,
			event.DriverID,
			event.Status,
		)

		if err := c.handleShiftUpdated(ctx, event); err != nil {
			log.Println("handle error:", err)
		}
	}
}

func (c *ShiftUpdatedConsumer) handleShiftUpdated(ctx context.Context, event contractsKafka.ShiftUpdatedEvent) error {
	return c.app.Commands.UpsertDriver.Handle(ctx, command.UpsertDriver{Event: event})
}
