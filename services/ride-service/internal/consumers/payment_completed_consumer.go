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

var paymentCompletedTracer = otel.Tracer("ride-service/consumer")

// PaymentCompletedConsumer flips ride.ride.bill_id once billing-service collects payment,
// making it visible on the ride itself and in the admin dashboard.
type PaymentCompletedConsumer struct {
	app    app.Application
	broker string
	logger *logrus.Entry
}

func NewPaymentCompletedConsumer(app app.Application, broker string, logger *logrus.Entry) *PaymentCompletedConsumer {
	return &PaymentCompletedConsumer{app: app, broker: broker, logger: logger}
}

// Run commits offsets manually, retrying a failed message in place (see
// RideAcceptedConsumer.Run) — safe because MarkRideBilled guards on "AND bill_id IS NULL".
func (c *PaymentCompletedConsumer) Run(ctx context.Context, topic string) {
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
// on panic — see RideAcceptedConsumer.handleOne for the span-ordering rationale.
func (c *PaymentCompletedConsumer) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, paymentCompletedTracer, topic, "ride-service", msg)
	defer func() { obskafka.FinishSpan(span, err) }()
	defer obskafka.RecoverSpan(span)

	var event contractsKafka.PaymentCompletedEvent
	if err = json.Unmarshal(msg.Value, &event); err != nil {
		span.SetAttributes(attribute.Bool("messaging.kafka.poison_message", true))
		c.logger.WithContext(msgCtx).WithError(err).
			WithField("raw_payload", obskafka.TruncateForLog(msg.Value, maxPoisonPayloadLogBytes)).
			Error("failed to deserialize event; committing to skip poison message")
		return true, err
	}

	logEntry := c.logger.WithContext(msgCtx).WithField("ride_id", event.RideID).WithField("invoice_id", event.InvoiceID)

	handleCtx, cancel := context.WithTimeout(msgCtx, handleTimeout)
	defer cancel()
	err = c.app.Commands.MarkRideBilled.Handle(handleCtx, command.MarkRideBilled{
		RideID:    event.RideID,
		InvoiceID: event.InvoiceID,
	})
	if err != nil {
		logEntry.WithError(err).Error("handle error; leaving offset uncommitted for redelivery")
		return false, err
	}
	return true, nil
}
