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

// RideAcceptedConsumer opens the driver-side half of a ride's tracking
// window — RideAcceptedEvent carries no ClientID, see domain.TrackingRepository.
type RideAcceptedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideAcceptedEvent]
}

func NewRideAcceptedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{
		runner: kafkaconsumer.New(broker, "location-service", logger,
			func(b []byte) (contractsKafka.RideAcceptedEvent, error) {
				var event contractsKafka.RideAcceptedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideAcceptedEvent) error {
				acceptedAt, err := time.Parse(time.RFC3339, event.AcceptedAt)
				if err != nil {
					acceptedAt = time.Now().UTC()
				}
				return app.Commands.RecordRideAccepted.Handle(ctx, command.RecordRideAccepted{
					RideID:     event.RideID,
					DriverID:   event.DriverID,
					AcceptedAt: acceptedAt,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideAcceptedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID, "driver_id": event.DriverID}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying in place on handler failure.
// RecordRideAccepted is a plain SET, safe to redeliver.
func (c *RideAcceptedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
