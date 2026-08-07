package consumers

import (
	"context"
	"encoding/json"
	app "matching-service/internal/application"
	"matching-service/internal/application/command"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// handleTimeout bounds how long a single message's handling can run, so a hung Redis
// call can't block the consumer's read loop (and every message behind it).
const handleTimeout = 10 * time.Second

// maxPoisonPayloadLogBytes bounds how much of an undeserializable message's
// raw payload gets attached to the "poison message" log line/span.
const maxPoisonPayloadLogBytes = 500

// retryBackoff is the pause before Run retries a message whose handler
// failed, in place — see Run's doc comment.
const retryBackoff = 2 * time.Second

// tracer is shared across every consumer in this package.
var tracer = otel.Tracer("matching-service/consumer")

type RideRequestedConsumer struct {
	app    app.Application
	broker string
	logger *logrus.Entry
}

func NewRideRequestedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideRequestedConsumer {
	return &RideRequestedConsumer{app: app, broker: broker, logger: logger}
}

// Run fetches/commits offsets manually, retrying a failing message in place (no park/DLQ
// exists) — safe here since every Redis write is already a natural, idempotent upsert.
func (c *RideRequestedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "matching-service",
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

// handleOne is scoped to a single message so its span ends via defer even on panic.
// A poison message still gets a span (committed, commit=true); a real failure returns commit=false.
func (c *RideRequestedConsumer) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, tracer, topic, "matching-service", msg)
	defer func() { obskafka.FinishSpan(span, err) }()
	defer obskafka.RecoverSpan(span)

	var event contractsKafka.RideRequestedEvent
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		span.SetAttributes(attribute.Bool("messaging.kafka.poison_message", true))
		c.logger.WithContext(msgCtx).WithError(err).
			WithField("raw_payload", obskafka.TruncateForLog(msg.Value, maxPoisonPayloadLogBytes)).
			Error("failed to deserialize event; committing to skip poison message")
		return true, err
	}

	logEntry := c.logger.WithContext(msgCtx).WithField("ride_id", event.RideID).WithField("client_id", event.ClientID)
	err = c.handleRideRequested(msgCtx, event)
	if err != nil {
		logEntry.WithError(err).Error("handle error; leaving offset uncommitted for redelivery")
		return false, err
	}
	return true, nil
}

func (c *RideRequestedConsumer) handleRideRequested(ctx context.Context, event contractsKafka.RideRequestedEvent) error {
	ctx, cancel := context.WithTimeout(ctx, handleTimeout)
	defer cancel()
	if err := c.app.Commands.CreateRide.Handle(ctx, command.CreateRide{Event: event}); err != nil {
		return err
	}
	return c.app.Commands.BroadcastOffers.Handle(ctx, command.BroadcastOffers{RideID: event.RideID, Attempt: 1})
}
