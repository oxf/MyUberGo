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

// Run fetches/commits offsets manually and retries a failed message in place — see RideCompletedConsumer.Run's
// doc comment for the full rationale. CreateInvoiceFromRide's `UNIQUE (ride_id, type)` guard makes redelivery safe here.
func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
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

// handleOne is scoped to a single message so its span/timeout-cancel run via defer even on panic (a bare
// span.End() in Run's loop body would never fire). Span starts before deserialization so poison messages still produce one — see RideCompletedConsumer.handleOne.
func (c *RideCancelledConsumer) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, consumerTracer, topic, "billing-service", msg)
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

	// BILLING_SPEC.md §8: skip invoice creation when feeMinor==0 — ride-service's fee calculator only returns
	// nonzero once a driver was assigned, so most cancellations never reach this path, and that's expected.
	if event.FeeMinor == 0 {
		span.SetAttributes(attribute.Bool("billing.skipped_zero_fee", true))
		return true, nil
	}

	logEntry := c.logger.WithContext(msgCtx).WithField("ride_id", event.RideID)

	handleCtx, cancel := context.WithTimeout(msgCtx, handleTimeout)
	defer cancel()
	err = c.app.Commands.CreateInvoiceFromRide.Handle(handleCtx, command.CreateInvoiceFromRide{
		RideID:      event.RideID,
		ClientID:    event.ClientID,
		DriverID:    event.DriverID,
		Type:        domain.InvoiceTypeCancellationFee,
		AmountMinor: event.FeeMinor,
		Currency:    event.Currency,
	})
	if err != nil {
		logEntry.WithError(err).Error("handle error; leaving offset uncommitted for redelivery")
		return false, err
	}
	return true, nil
}
