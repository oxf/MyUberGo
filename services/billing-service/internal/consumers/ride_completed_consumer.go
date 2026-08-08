package consumers

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/domain"
	"context"
	"encoding/json"

	"github.com/oxf/MyUber/common/kafkaconsumer"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type RideCompletedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideCompletedEvent]
}

func NewRideCompletedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideCompletedConsumer {
	return &RideCompletedConsumer{
		runner: kafkaconsumer.New(broker, "billing-service", logger,
			func(b []byte) (contractsKafka.RideCompletedEvent, error) {
				var event contractsKafka.RideCompletedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideCompletedEvent) error {
				driverID := event.DriverID
				return app.Commands.CreateInvoiceFromRide.Handle(ctx, command.CreateInvoiceFromRide{
					RideID:      event.RideID,
					ClientID:    event.ClientID,
					DriverID:    &driverID,
					Type:        domain.InvoiceTypeRideFare,
					AmountMinor: event.AmountMinor,
					Currency:    event.Currency,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideCompletedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run fetches/commits offsets manually and, on failure, retries the SAME message in place rather than committing
// later ones ahead of it (Kafka's offset is one cumulative cursor, not per-message) — a poison message blocks its partition rather than being skipped. CreateInvoiceFromRide's UNIQUE(ride_id, type) guard makes redelivery safe.
func (c *RideCompletedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
