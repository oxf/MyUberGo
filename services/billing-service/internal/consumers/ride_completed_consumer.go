package consumers

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/domain"
	"context"
	"encoding/json"
	"log"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/segmentio/kafka-go"
)

// handleTimeout bounds how long a single message's command handling can
// run, so a hung DB/dependency call can't block the consumer's read loop
// (and, by extension, every message behind it) indefinitely.
const handleTimeout = 10 * time.Second

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
		GroupID: "billing-service",
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

		log.Printf("Ride completed received. RideID=%s ClientID=%s DriverID=%s AmountMinor=%d %s",
			event.RideID, event.ClientID, event.DriverID, event.AmountMinor, event.Currency)

		driverID := event.DriverID
		handleCtx, cancel := context.WithTimeout(ctx, handleTimeout)
		if err := c.app.Commands.CreateInvoiceFromRide.Handle(handleCtx, command.CreateInvoiceFromRide{
			RideID:      event.RideID,
			ClientID:    event.ClientID,
			DriverID:    &driverID,
			Type:        domain.InvoiceTypeRideFare,
			AmountMinor: event.AmountMinor,
			Currency:    event.Currency,
		}); err != nil {
			log.Println("handle error:", err)
		}
		cancel()
	}
}
