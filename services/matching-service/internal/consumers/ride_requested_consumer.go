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

type RideRequestedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.RideRequestedEvent]
}

func NewRideRequestedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideRequestedConsumer {
	return &RideRequestedConsumer{
		runner: kafkaconsumer.New(broker, "matching-service", logger,
			func(b []byte) (contractsKafka.RideRequestedEvent, error) {
				var event contractsKafka.RideRequestedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.RideRequestedEvent) error {
				if err := app.Commands.CreateRide.Handle(ctx, command.CreateRide{Event: event}); err != nil {
					return err
				}
				return app.Commands.BroadcastOffers.Handle(ctx, command.BroadcastOffers{RideID: event.RideID, Attempt: 1})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.RideRequestedEvent) logrus.Fields {
				return logrus.Fields{"ride_id": event.RideID, "client_id": event.ClientID}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying a failing message in place (no park/DLQ
// exists) — safe here since every Redis write is already a natural, idempotent upsert.
func (c *RideRequestedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
