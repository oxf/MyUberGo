package consumers

import (
	"context"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"encoding/json"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// handleTimeout bounds how long a single message's handling can run, so a hung
// DB/dependency call can't block the read loop and every message behind it.
const handleTimeout = 10 * time.Second

// maxPoisonPayloadLogBytes bounds how much of an undeserializable message's
// raw payload gets attached to the "poison message" log line/span.
const maxPoisonPayloadLogBytes = 500

// retryBackoff is the pause before Run retries a message whose handler
// failed, in place — see RideAcceptedConsumer.Run's doc comment.
const retryBackoff = 2 * time.Second

// consumerTracer is shared by all three consumers in this package (they're
// all `package consumers`) to avoid a duplicate package-level declaration.
var consumerTracer = otel.Tracer("driver-service/consumer")

type RideAcceptedConsumer struct {
	app    app.Application
	broker string
	logger *logrus.Entry
}

func NewRideAcceptedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{app: app, broker: broker, logger: logger}
}

// Run manually fetches/commits offsets and retries a failed message in place, since Kafka's
// committed offset is one cursor per partition; ProcessRideAccepted's guarded transition makes that safe.
func (c *RideAcceptedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "driver-service",
	})
	defer reader.Close()

	c.logger.WithField("topic", topic).Info("consumer started")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.WithError(err).Error("consumer error")
			continue
		}

		for {
			commit, _ := c.handleOne(ctx, topic, msg)
			if commit {
				if err := reader.CommitMessages(ctx, msg); err != nil {
					c.logger.WithError(err).Error("commit offset failed")
				}
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryBackoff):
			}
		}
	}
}

// handleOne is scoped to a single message so its span/timeout-cancel run via defer even on panic.
// The span starts before deserialization so a poison message still produces one and is committed.
func (c *RideAcceptedConsumer) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, consumerTracer, topic, "driver-service", msg)
	defer func() { obskafka.FinishSpan(span, err) }()
	defer obskafka.RecoverSpan(span)

	var event contractsKafka.RideAcceptedEvent
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		span.SetAttributes(attribute.Bool("messaging.kafka.poison_message", true))
		c.logger.WithContext(msgCtx).WithError(err).
			WithField("raw_payload", obskafka.TruncateForLog(msg.Value, maxPoisonPayloadLogBytes)).
			Error("failed to deserialize event; committing to skip poison message")
		return true, err
	}

	logEntry := c.logger.WithContext(msgCtx).WithField("ride_id", event.RideID).WithField("driver_id", event.DriverID)

	handleCtx, cancel := context.WithTimeout(msgCtx, handleTimeout)
	defer cancel()
	err = c.app.Commands.ProcessRideAccepted.Handle(handleCtx, command.ProcessRideAccepted{
		RideID:     event.RideID,
		DriverID:   event.DriverID,
		AcceptedAt: event.AcceptedAt,
	})
	if err != nil {
		logEntry.WithError(err).Error("handle error; leaving offset uncommitted for redelivery")
		return false, err
	}
	return true, nil
}
