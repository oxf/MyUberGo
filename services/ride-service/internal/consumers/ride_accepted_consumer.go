package consumers

import (
	"context"
	"encoding/json"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// maxPoisonPayloadLogBytes bounds how much of an undeserializable message's
// raw payload gets attached to the "poison message" log line/span.
const maxPoisonPayloadLogBytes = 500

// retryBackoff is the pause between re-attempts of a failed message before Run
// retries the SAME message again in place — see Run's doc comment.
const retryBackoff = 2 * time.Second

var rideAcceptedTracer = otel.Tracer("ride-service/consumer")

// handleTimeout bounds a single message's command handling so a hung DB call
// can't block the read loop, and every message behind it, indefinitely.
const handleTimeout = 10 * time.Second

type RideAcceptedConsumer struct {
	app    app.Application
	broker string
	logger *logrus.Entry
}

func NewRideAcceptedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{app: app, broker: broker, logger: logger}
}

// Run commits offsets manually for at-least-once delivery, retrying a failed message in
// place — safe because MarkRideMatched's "AND status = 'Requested'" guard is idempotent.
func (c *RideAcceptedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "ride-service",
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

// handleOne is scoped to a single message so its span/timeout-cancel run via defer even
// on panic. The span starts before deserialization so a poison message still produces one.
func (c *RideAcceptedConsumer) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, rideAcceptedTracer, topic, "ride-service", msg)
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
	err = c.app.Commands.MarkRideMatched.Handle(handleCtx, command.MarkRideMatched{
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
