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

// RideCancelledConsumer closes a ride's tracking window and force-closes any
// live WS connections — see RideCompletedConsumer for why CloseRide always runs.
type RideCancelledConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideCancelledEvent]
}

func NewRideCancelledConsumer(app app.Application, closer SocketCloser, broker string, logger *logrus.Entry) *RideCancelledConsumer {
	return &RideCancelledConsumer{
		runner: kafkaconsumer.New(broker, "location-service", logger,
			func(b []byte) (contractsKafka.RideCancelledEvent, error) {
				var event contractsKafka.RideCancelledEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideCancelledEvent) error {
				if err := app.Commands.CloseTrackingWindow.Handle(ctx, command.CloseTrackingWindow{RideID: event.RideID}); err != nil {
					return err
				}
				closer.CloseRide(event.RideID)
				return nil
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideCancelledEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying in place on handler failure.
// CloseWindow + CloseRide are both idempotent, safe to redeliver.
func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
