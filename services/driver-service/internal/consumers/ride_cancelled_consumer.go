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
	"go.opentelemetry.io/otel/attribute"
)

type RideCancelledConsumer struct {
	app    app.Application
	broker string
	logger *logrus.Entry
}

func NewRideCancelledConsumer(app app.Application, broker string, logger *logrus.Entry) *RideCancelledConsumer {
	return &RideCancelledConsumer{app: app, broker: broker, logger: logger}
}

// Run retries a failed message in place rather than auto-committing — see RideAcceptedConsumer.Run.
// ProcessRideCancelled's guarded OnRide->Online transition is what makes redelivery safe here.
func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
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

// handleOne is scoped to a single message so its span/timeout-cancel run via defer even on panic —
// see RideAcceptedConsumer.handleOne for why the span starts before deserialization.
func (c *RideCancelledConsumer) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, consumerTracer, topic, "driver-service", msg)
	defer func() { obskafka.FinishSpan(span, err) }()
	defer obskafka.RecoverSpan(span)

	var event contractsKafka.RideCancelledEvent
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		span.SetAttributes(attribute.Bool("messaging.kafka.poison_message", true))
		c.logger.WithContext(msgCtx).WithError(err).
			WithField("raw_payload", obskafka.TruncateForLog(msg.Value, maxPoisonPayloadLogBytes)).
			Error("failed to deserialize event; committing to skip poison message")
		return true, err
	}

	logEntry := c.logger.WithContext(msgCtx).WithField("ride_id", event.RideID)

	handleCtx, cancel := context.WithTimeout(msgCtx, handleTimeout)
	defer cancel()
	err = c.app.Commands.ProcessRideCancelled.Handle(handleCtx, command.ProcessRideCancelled{
		RideID:      event.RideID,
		DriverID:    event.DriverID,
		CancelledAt: event.CancelledAt,
	})
	if err != nil {
		logEntry.WithError(err).Error("handle error; leaving offset uncommitted for redelivery")
		return false, err
	}
	return true, nil
}
