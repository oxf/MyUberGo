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

// RideRequestedConsumer opens the client-side half of a ride's tracking
// window — a second, independent consumer group over the same topic.
type RideRequestedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideRequestedEvent]
}

func NewRideRequestedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideRequestedConsumer {
	return &RideRequestedConsumer{
		runner: kafkaconsumer.New(broker, "location-service", logger,
			func(b []byte) (contractsKafka.RideRequestedEvent, error) {
				var event contractsKafka.RideRequestedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideRequestedEvent) error {
				return app.Commands.RecordRideRequested.Handle(ctx, command.RecordRideRequested{
					RideID:   event.RideID,
					ClientID: event.ClientID,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideRequestedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID, "client_id": event.ClientID}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying in place on handler failure.
// RecordRideRequested is a plain SET, safe to redeliver.
func (c *RideRequestedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
