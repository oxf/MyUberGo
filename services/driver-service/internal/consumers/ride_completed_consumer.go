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

type RideCompletedConsumer struct {
	app    app.Application
	broker string
}

func NewRideCompletedConsumer(app app.Application, broker string) *RideCompletedConsumer {
	return &RideCompletedConsumer{app: app, broker: broker}
}

func (c *RideCompletedConsumer) Run(ctx context.Context, topic string) {
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

		var event contractsKafka.RideCompletedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		if err := c.app.Commands.ProcessRideCompleted.Handle(ctx, command.ProcessRideCompleted{
			RideID:     event.RideID,
			DriverID:   event.DriverID,
			FinishedAt: event.FinishedAt,
		}); err != nil {
			log.Println("handle error:", err)
		}
	}
}
