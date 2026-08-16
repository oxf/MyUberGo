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

// ShiftUpdatedConsumer caches the driver<->user identity mapping from the same
// shift.updated event matching-service consumes, via its own consumer group.
type ShiftUpdatedConsumer struct {
	runner *kafkaconsumer.Runner[contractsKafka.ShiftUpdatedEvent]
}

func NewShiftUpdatedConsumer(app app.Application, broker string, logger *logrus.Entry) *ShiftUpdatedConsumer {
	return &ShiftUpdatedConsumer{
		runner: kafkaconsumer.New(broker, "location-service", logger,
			func(b []byte) (contractsKafka.ShiftUpdatedEvent, error) {
				var event contractsKafka.ShiftUpdatedEvent
				err := json.Unmarshal(b, &event)
				return event, err
			},
			func(ctx context.Context, event contractsKafka.ShiftUpdatedEvent) error {
				return app.Commands.UpsertOwner.Handle(ctx, command.UpsertOwner{
					DriverID: event.DriverID,
					UserID:   event.UserID,
				})
			},
			kafkaconsumer.WithEventFields(func(event contractsKafka.ShiftUpdatedEvent) logrus.Fields {
				return logrus.Fields{"shift_id": event.ShiftID, "driver_id": event.DriverID, "status": event.Status}
			}),
		),
	}
}

// Run fetches/commits offsets manually, retrying in place on handler failure.
// UpsertOwner is a plain SET, safe to redeliver.
func (c *ShiftUpdatedConsumer) Run(ctx context.Context, topic string) {
	c.runner.Run(ctx, topic)
}
