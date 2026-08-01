package consumers

import (
	"context"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"encoding/json"
	"log"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/segmentio/kafka-go"
)

type RideCancelledConsumer struct {
	app    app.Application
	broker string
}

func NewRideCancelledConsumer(app app.Application, broker string) *RideCancelledConsumer {
	return &RideCancelledConsumer{app: app, broker: broker}
}

func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "driver-service",
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

		var event contractsKafka.RideCancelledEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		handleCtx, cancel := context.WithTimeout(ctx, handleTimeout)
		if err := c.app.Commands.ProcessRideCancelled.Handle(handleCtx, command.ProcessRideCancelled{
			RideID:      event.RideID,
			DriverID:    event.DriverID,
			CancelledAt: event.CancelledAt,
		}); err != nil {
			log.Println("handle error:", err)
		}
		cancel()
	}
}
