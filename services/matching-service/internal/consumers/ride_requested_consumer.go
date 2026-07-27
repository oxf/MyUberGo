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

type RideRequestedConsumer struct {
	app    app.Application
	broker string
}

func NewRideRequestedConsumer(app app.Application, broker string) *RideRequestedConsumer {
	return &RideRequestedConsumer{app: app, broker: broker}
}

func (c *RideRequestedConsumer) Run(ctx context.Context, topic string) {
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

		var event contractsKafka.RideRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf("Ride request received. RideID=%s ClientID=%s PriceMinor=%d %s",
			event.RideID, event.ClientID, event.PriceMinor, event.Currency)

		if err := c.handleRideRequested(ctx, event); err != nil {
			log.Println("handle error:", err)
		}
	}
}

func (c *RideRequestedConsumer) handleRideRequested(ctx context.Context, event contractsKafka.RideRequestedEvent) error {
	if err := c.app.Commands.CreateRide.Handle(ctx, command.CreateRide{Event: event}); err != nil {
		return err
	}
	return c.app.Commands.BroadcastOffers.Handle(ctx, command.BroadcastOffers{RideID: event.RideID, Attempt: 1})
}
