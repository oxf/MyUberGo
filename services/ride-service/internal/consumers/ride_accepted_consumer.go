package consumers

import (
	"context"
	"encoding/json"
	"log"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/segmentio/kafka-go"
)

type RideAcceptedConsumer struct {
	app    app.Application
	broker string
}

func NewRideAcceptedConsumer(app app.Application, broker string) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{app: app, broker: broker}
}

func (c *RideAcceptedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "ride-service",
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

		var event contractsKafka.RideAcceptedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf("Ride accepted received. RideID=%s DriverID=%s", event.RideID, event.DriverID)

		if err := c.app.Commands.MarkRideMatched.Handle(ctx, command.MarkRideMatched{
			RideID:     event.RideID,
			DriverID:   event.DriverID,
			AcceptedAt: event.AcceptedAt,
		}); err != nil {
			log.Println("handle error:", err)
		}
	}
}
