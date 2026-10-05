package consumers

import (
	"context"
	"encoding/json"
	"time"

	app "location-service/internal/application"
	"location-service/internal/application/command"

	"github.com/oxf/MyUber/common/kafkaconsumer"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

// RideCompletedConsumer builds the ride's route summary, closes its
// tracking window, and force-closes any live WS connections — CloseRide
// must run even on redelivery, no Redis record of "done".
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
				finishedAt, err := time.Parse(time.RFC3339, event.FinishedAt)
				if err != nil {
					finishedAt = time.Now().UTC()
				}
				// Must run before CloseTrackingWindow: CloseWindow deletes the
				// participants hash BuildRideSummary reads StartedAt from.
				if err := app.Commands.BuildRideSummary.Handle(ctx, command.BuildRideSummary{RideID: event.RideID, EndedAt: finishedAt}); err != nil {
					return err
				}
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
// BuildRideSummary, CloseWindow, and CloseRide are all idempotent, safe to redeliver.
func (c *RideCompletedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
