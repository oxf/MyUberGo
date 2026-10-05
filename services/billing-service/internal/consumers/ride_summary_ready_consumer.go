package consumers

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"context"
	"encoding/json"

	"github.com/oxf/MyUber/common/kafkaconsumer"
	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

// RideSummaryReadyConsumer records location-service's measured actual
// distance/duration onto the ride's invoice — record only, never re-price
// (LOCATION_SPEC.md §2.4).
type RideSummaryReadyConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideSummaryReadyEvent]
}

func NewRideSummaryReadyConsumer(app app.Application, broker string, logger *logrus.Entry) *RideSummaryReadyConsumer {
	return &RideSummaryReadyConsumer{
		runner: kafkaconsumer.New(broker, "billing-service", logger,
			func(b []byte) (contractsKafka.RideSummaryReadyEvent, error) {
				var event contractsKafka.RideSummaryReadyEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideSummaryReadyEvent) error {
				return app.Commands.RecordRideActuals.Handle(ctx, command.RecordRideActuals{
					RideID:    event.RideID,
					DistanceM: event.DistanceM,
					DurationS: event.DurationS,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideSummaryReadyEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying in place on handler
// failure. RecordRideActuals is a plain UPDATE by ride_id, safe to redeliver.
func (c *RideSummaryReadyConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
