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
	app app.Application
}

func NewShiftUpdatedConsumer(app app.Application) *ShiftUpdatedConsumer {
	return &ShiftUpdatedConsumer{app: app}
}

func (c *ShiftUpdatedConsumer) Run(topic string) {

	var kafkaBroker = getenv("KAFKA_BROKER", "kafka:29092")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "shift.updated",
		GroupID: "matching-service",
	})

	defer reader.Close()

	log.Println("shift-updated consumer started")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("consumer error:", err)
			continue
		}

		var event contractsKafka.ShiftUpdatedEvent

		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf(
			"Shift updated received. ShiftID=%s DriverID=%s Status=%s",
			event.ShiftID,
			event.DriverID,
			event.Status,
		)

		if err := c.handleShiftUpdated(event); err != nil {
			log.Println("handle error:", err)
		}
	}
}

func (c *ShiftUpdatedConsumer) handleShiftUpdated(event contractsKafka.ShiftUpdatedEvent) error {
	command := command.CreateDriver{Event: event}
	err := c.app.Commands.CreateDriver.Handle(context.Background(), command)
	if err != nil {
		return err
	}
	return nil
}
