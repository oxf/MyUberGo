package consumers

import (
	"context"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"encoding/json"

	"github.com/oxf/MyUber/common/kafkaconsumer"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type RideCancelledConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideCancelledEvent]
}

func NewRideCancelledConsumer(app app.Application, broker string, logger *logrus.Entry) *RideCancelledConsumer {
	return &RideCancelledConsumer{
		runner: kafkaconsumer.New(broker, "driver-service", logger,
			func(b []byte) (contractsKafka.RideCancelledEvent, error) {
				var event contractsKafka.RideCancelledEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideCancelledEvent) error {
				return app.Commands.ProcessRideCancelled.Handle(ctx, command.ProcessRideCancelled{
					RideID:      event.RideID,
					DriverID:    event.DriverID,
					CancelledAt: event.CancelledAt,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideCancelledEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run retries a failed message in place rather than auto-committing — see RideAcceptedConsumer.Run.
// ProcessRideCancelled's guarded OnRide->Online transition is what makes redelivery safe here.
func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
