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

type RideAcceptedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideAcceptedEvent]
}

func NewRideAcceptedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{
		runner: kafkaconsumer.New(broker, "driver-service", logger,
			func(b []byte) (contractsKafka.RideAcceptedEvent, error) {
				var event contractsKafka.RideAcceptedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideAcceptedEvent) error {
				return app.Commands.ProcessRideAccepted.Handle(ctx, command.ProcessRideAccepted{
					RideID:     event.RideID,
					DriverID:   event.DriverID,
					AcceptedAt: event.AcceptedAt,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideAcceptedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID, "driver_id": event.DriverID}
			}),
		),
	}
}

// Run manually fetches/commits offsets and retries a failed message in place, since Kafka's
// committed offset is one cursor per partition; ProcessRideAccepted's guarded transition makes that safe.
func (c *RideAcceptedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
