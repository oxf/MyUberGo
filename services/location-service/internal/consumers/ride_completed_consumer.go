package consumers

import (
	"context"
	"encoding/json"

	app "location-service/internal/application"
	"location-service/internal/application/command"

	"github.com/oxf/MyUber/common/kafkaconsumer"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

// RideCompletedConsumer closes a ride's tracking window and force-closes any
// live WS connections — CloseRide must run even on redelivery, no Redis record of "done".
type RideCompletedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideCompletedEvent]
}

func NewRideCompletedConsumer(app app.Application, closer SocketCloser, broker string, logger *logrus.Entry) *RideCompletedConsumer {
	return &RideCompletedConsumer{
		runner: kafkaconsumer.New(broker, "location-service", logger,
			func(b []byte) (contractsKafka.RideCompletedEvent, error) {
				var event contractsKafka.RideCompletedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideCompletedEvent) error {
				if err := app.Commands.CloseTrackingWindow.Handle(ctx, command.CloseTrackingWindow{RideID: event.RideID}); err != nil {
					return err
				}
				closer.CloseRide(event.RideID)
				return nil
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideCompletedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying in place on handler failure.
// CloseWindow + CloseRide are both idempotent, safe to redeliver.
func (c *RideCompletedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
