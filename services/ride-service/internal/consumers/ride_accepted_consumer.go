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

type RideAcceptedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideAcceptedEvent]
}

func NewRideAcceptedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{
		runner: kafkaconsumer.New(broker, "ride-service", logger,
			func(b []byte) (contractsKafka.RideAcceptedEvent, error) {
				var event contractsKafka.RideAcceptedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideAcceptedEvent) error {
				return app.Commands.MarkRideMatched.Handle(ctx, command.MarkRideMatched{
					RideID:      event.RideID,
					DriverID:    event.DriverID,
					AcceptedAt:  event.AcceptedAt,
					RequestedAt: event.RequestedAt,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideAcceptedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID, "driver_id": event.DriverID}
			}),
		),
	}
}

// Run commits offsets manually for at-least-once delivery, retrying a failed message in
// place — safe because MarkRideMatched's "AND status = 'Requested'" guard is idempotent.
func (c *RideAcceptedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
