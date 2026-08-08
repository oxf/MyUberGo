package consumers

import (
	"context"
	"encoding/json"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"

	"github.com/oxf/MyUber/common/kafkaconsumer"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

// PaymentCompletedConsumer flips ride.ride.bill_id once billing-service collects payment,
// making it visible on the ride itself and in the admin dashboard.
type PaymentCompletedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.PaymentCompletedEvent]
}

func NewPaymentCompletedConsumer(app app.Application, broker string, logger *logrus.Entry) *PaymentCompletedConsumer {
	return &PaymentCompletedConsumer{
		runner: kafkaconsumer.New(broker, "ride-service", logger,
			func(b []byte) (contractsKafka.PaymentCompletedEvent, error) {
				var event contractsKafka.PaymentCompletedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.PaymentCompletedEvent) error {
				return app.Commands.MarkRideBilled.Handle(ctx, command.MarkRideBilled{
					RideID:    event.RideID,
					InvoiceID: event.InvoiceID,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.PaymentCompletedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID, "invoice_id": event.InvoiceID}
			}),
		),
	}
}

// Run commits offsets manually, retrying a failed message in place (see
// RideAcceptedConsumer.Run) — safe because MarkRideBilled guards on "AND bill_id IS NULL".
func (c *PaymentCompletedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
