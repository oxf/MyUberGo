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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type RideCancelledConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideCancelledEvent]
}

func NewRideCancelledConsumer(app app.Application, broker string, logger *logrus.Entry) *RideCancelledConsumer {
	return &RideCancelledConsumer{
		runner: kafkaconsumer.New(broker, "billing-service", logger,
			func(b []byte) (contractsKafka.RideCancelledEvent, error) {
				var event contractsKafka.RideCancelledEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideCancelledEvent) error {
				// BILLING_SPEC.md §8: skip invoice creation when feeMinor==0 — ride-service's fee calculator only
				// returns nonzero once a driver was assigned, so most cancellations never reach this path.
				if event.FeeMinor == 0 {
					trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("billing.skipped_zero_fee", true))
					return nil
				}
				return app.Commands.CreateInvoiceFromRide.Handle(ctx, command.CreateInvoiceFromRide{
					RideID:      event.RideID,
					ClientID:    event.ClientID,
					DriverID:    event.DriverID,
					Type:        domain.InvoiceTypeCancellationFee,
					AmountMinor: event.FeeMinor,
					Currency:    event.Currency,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideCancelledEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run fetches/commits offsets manually and retries a failed message in place — see RideCompletedConsumer.Run's
// doc comment for the full rationale. CreateInvoiceFromRide's `UNIQUE (ride_id, type)` guard makes redelivery safe here.
func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
