package consumers

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/domain"
	"context"
	"encoding/json"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// handleTimeout bounds how long a single message's command handling can run, so a hung DB/dependency call can't
// block the consumer's read loop (and every message behind it) indefinitely.
const handleTimeout = 10 * time.Second

// maxPoisonPayloadLogBytes bounds how much of an undeserializable message's
// raw payload gets attached to the "poison message" log line/span.
const maxPoisonPayloadLogBytes = 500

// retryBackoff is the pause before Run retries a message whose handler
// failed, in place — see Run's doc comment.
const retryBackoff = 2 * time.Second

var consumerTracer = otel.Tracer("billing-service/consumer")

type RideCompletedConsumer struct {
	app    app.Application
	broker string
	logger *logrus.Entry
}

func NewRideCompletedConsumer(app app.Application, broker string, logger *logrus.Entry) *RideCompletedConsumer {
	return &RideCompletedConsumer{app: app, broker: broker, logger: logger}
}

// Run fetches/commits offsets manually and, on failure, retries the SAME message in place rather than committing
// later ones ahead of it (Kafka's offset is one cumulative cursor, not per-message) — a poison message blocks its partition rather than being skipped. CreateInvoiceFromRide's UNIQUE(ride_id, type) guard makes redelivery safe.
func (c *RideCompletedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "billing-service",
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

// handleOne is scoped to a single message so its span/timeout-cancel run via defer even on panic. Span starts
// before deserialization so a poison message still produces one and is committed (commit=true); a genuine handler failure returns commit=false so Run retries it.
func (c *RideCompletedConsumer) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, consumerTracer, topic, "billing-service", msg)
	defer func() { obskafka.FinishSpan(span, err) }()
	defer obskafka.RecoverSpan(span)

	var event contractsKafka.RideCompletedEvent
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		span.SetAttributes(attribute.Bool("messaging.kafka.poison_message", true))
		c.logger.WithContext(msgCtx).WithError(err).
			WithField("raw_payload", obskafka.TruncateForLog(msg.Value, maxPoisonPayloadLogBytes)).
			Error("failed to deserialize event; committing to skip poison message")
		return true, err
	}

	logEntry := c.logger.WithContext(msgCtx).WithField("ride_id", event.RideID)

	driverID := event.DriverID
	handleCtx, cancel := context.WithTimeout(msgCtx, handleTimeout)
	defer cancel()
	err = c.app.Commands.CreateInvoiceFromRide.Handle(handleCtx, command.CreateInvoiceFromRide{
		RideID:      event.RideID,
		ClientID:    event.ClientID,
		DriverID:    &driverID,
		Type:        domain.InvoiceTypeRideFare,
		AmountMinor: event.AmountMinor,
		Currency:    event.Currency,
	})
	if err != nil {
		logEntry.WithError(err).Error("handle error; leaving offset uncommitted for redelivery")
		return false, err
	}
	return true, nil
}
