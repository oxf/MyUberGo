package consumers

import (
	"context"
	"encoding/json"
	"log"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/segmentio/kafka-go"
)

// handleTimeout bounds how long a single message's command handling can
// run, so a hung DB/dependency call can't block the consumer's read loop
// (and, by extension, every message behind it) indefinitely.
const handleTimeout = 10 * time.Second

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

		handleCtx, cancel := context.WithTimeout(ctx, handleTimeout)
		if err := c.app.Commands.MarkRideMatched.Handle(handleCtx, command.MarkRideMatched{
			RideID:     event.RideID,
			DriverID:   event.DriverID,
			AcceptedAt: event.AcceptedAt,
		}); err != nil {
			log.Println("handle error:", err)
		}
		cancel()
	}
}
