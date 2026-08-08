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

type RideCompletedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideCompletedEvent]
}

func NewRideCompletedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideCompletedConsumer {
	return &RideCompletedConsumer{
		runner: kafkaconsumer.New(broker, "driver-service", logger,
			func(b []byte) (contractsKafka.RideCompletedEvent, error) {
				var event contractsKafka.RideCompletedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideCompletedEvent) error {
				return app.Commands.ProcessRideCompleted.Handle(ctx, command.ProcessRideCompleted{
					RideID:     event.RideID,
					DriverID:   event.DriverID,
					FinishedAt: event.FinishedAt,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideCompletedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run retries a failed message in place rather than auto-committing — see RideAcceptedConsumer.Run.
// ProcessRideCompleted's guarded transition (skipping the increment on redelivery) makes that safe.
func (c *RideCompletedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
