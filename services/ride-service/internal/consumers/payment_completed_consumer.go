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

// PaymentCompletedConsumer closes the loop documented in the README: once
// billing-service collects payment, ride-service flips ride.ride.bill_id so
// it's visible on the ride itself (and in the admin dashboard).
type PaymentCompletedConsumer struct {
	app    app.Application
	broker string
}

func NewPaymentCompletedConsumer(app app.Application, broker string) *PaymentCompletedConsumer {
	return &PaymentCompletedConsumer{app: app, broker: broker}
}

func (c *PaymentCompletedConsumer) Run(ctx context.Context, topic string) {
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

		var event contractsKafka.PaymentCompletedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf("Payment completed received. RideID=%s InvoiceID=%s", event.RideID, event.InvoiceID)

		if err := c.app.Commands.MarkRideBilled.Handle(ctx, command.MarkRideBilled{
			RideID:    event.RideID,
			InvoiceID: event.InvoiceID,
		}); err != nil {
			log.Println("handle error:", err)
		}
	}
}
