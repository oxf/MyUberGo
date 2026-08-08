package consumers

import (
	"context"
	"encoding/json"
	app "matching-service/internal/application"
	"matching-service/internal/application/command"

	"github.com/oxf/MyUber/common/kafkaconsumer"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

type RideCancelledConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideCancelledEvent]
}

func NewRideCancelledConsumer(app app.Application, broker string, logger *logrus.Entry) *RideCancelledConsumer {
	return &RideCancelledConsumer{
		runner: kafkaconsumer.New(broker, "matching-service", logger,
			func(b []byte) (contractsKafka.RideCancelledEvent, error) {
				var event contractsKafka.RideCancelledEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideCancelledEvent) error {
				return app.Commands.CancelRide.Handle(ctx, command.CancelRide{
					RideID:   event.RideID,
					DriverID: event.DriverID,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideCancelledEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run fetches/commits offsets manually rather than ReadMessage's auto-commit, retrying
// failures in place — see RideRequestedConsumer.Run's doc comment for the full rationale.
func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
