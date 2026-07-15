package consumers

import (
	"context"
	"encoding/json"
	"log"
	app "matching-service/internal/application"
	"matching-service/internal/application/command"
	"os"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/segmentio/kafka-go"
)

type RideAcceptedConsumer struct {
	app app.Application
}

func NewRideAcceptedConsumer(app app.Application) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{app: app}
}

func (c *RideAcceptedConsumer) Run(topic string) {

	var kafkaBroker = getenv("KAFKA_BROKER", "kafka:29092")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "ride.requested",
		GroupID: "matching-service",
	})

	defer reader.Close()

	log.Println("ride-requested consumer started")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("consumer error:", err)
			continue
		}

		var event contractsKafka.RideRequestedEvent

		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf(
			"Ride request received. RideID=%s ClientID=%s Price=%.2f",
			event.RideID,
			event.ClientID,
			event.Price,
		)

		if err := c.handleRideRequested(event); err != nil {
			log.Println("handle error:", err)
		}
	}
}

func (c *RideAcceptedConsumer) handleRideRequested(event contractsKafka.RideRequestedEvent) error {
	command := command.CreateRide{Event: event}
	err := c.app.Commands.CreateRide.Handle(context.Background(), command)
	if err != nil {
		return err
	}
	return nil
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
