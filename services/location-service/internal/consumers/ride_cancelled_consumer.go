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

// RideCancelledConsumer builds a route summary (only if the ride actually
// had a driver — LOCATION_SPEC.md §8.1), closes a ride's tracking window,
// and force-closes any live WS connections — see RideCompletedConsumer for
// why CloseRide always runs.
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
				// A summary only makes sense once the ride was matched — a
				// pre-match cancellation (DriverID nil) never opened a window.
				if event.DriverID != nil {
					cancelledAt, err := time.Parse(time.RFC3339, event.CancelledAt)
					if err != nil {
						cancelledAt = time.Now().UTC()
					}
					// Must run before CloseTrackingWindow — see RideCompletedConsumer.
					if err := app.Commands.BuildRideSummary.Handle(ctx, command.BuildRideSummary{RideID: event.RideID, EndedAt: cancelledAt}); err != nil {
						return err
					}
				}
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
