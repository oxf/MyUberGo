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

type ShiftUpdatedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.ShiftUpdatedEvent]
}

func NewShiftUpdatedConsumer(app app.Application, broker string, logger *logrus.Entry) *ShiftUpdatedConsumer {
	return &ShiftUpdatedConsumer{
		runner: kafkaconsumer.New(broker, "matching-service", logger,
			func(b []byte) (contractsKafka.ShiftUpdatedEvent, error) {
				var event contractsKafka.ShiftUpdatedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.ShiftUpdatedEvent) error {
				return app.Commands.UpsertDriver.Handle(ctx, command.UpsertDriver{Event: event})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.ShiftUpdatedEvent) logrus.Fields {
				return logrus.Fields{"shift_id": event.ShiftID, "driver_id": event.DriverID, "status": event.Status}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying in place — see RideRequestedConsumer.Run's
// doc comment. UpsertDriver is a plain upsert, safe to redeliver by construction.
func (c *ShiftUpdatedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
