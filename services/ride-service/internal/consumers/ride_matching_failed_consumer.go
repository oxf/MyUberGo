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

type RideMatchingFailedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideMatchingFailedEvent]
}

func NewRideMatchingFailedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideMatchingFailedConsumer {
	return &RideMatchingFailedConsumer{
		runner: kafkaconsumer.New(broker, "ride-service", logger,
			func(b []byte) (contractsKafka.RideMatchingFailedEvent, error) {
				var event contractsKafka.RideMatchingFailedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideMatchingFailedEvent) error {
				return app.Commands.FailRide.Handle(ctx, command.FailRide{
					RideID: event.RideID,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideMatchingFailedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run commits offsets manually for at-least-once delivery, retrying a failed message in
// place — safe because FailRide's "AND status = 'Requested'" guard is idempotent.
func (c *RideMatchingFailedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
