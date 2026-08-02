package consumers

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/domain"
	"context"
	"encoding/json"
	"log"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type RideCancelledConsumer struct {
	app    app.Application
	broker string
}

func NewRideCancelledConsumer(app app.Application, broker string) *RideCancelledConsumer {
	return &RideCancelledConsumer{app: app, broker: broker}
}

func (c *RideCancelledConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "billing-service",
	})
	defer reader.Close()

	log.Printf("%s consumer started", topic)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("consumer error:", err)
			continue
		}

		var event contractsKafka.RideCancelledEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf("Ride cancelled received. RideID=%s ClientID=%s FeeMinor=%d %s",
			event.RideID, event.ClientID, event.FeeMinor, event.Currency)

		// BILLING_SPEC.md §8: skip invoice creation entirely when
		// feeMinor==0 — ride-service's cancellation-fee calculator only
		// returns a nonzero fee once a driver was assigned, so most
		// cancellations never reach this path, and that's expected.
		if event.FeeMinor == 0 {
			continue
		}

		msgCtx := obskafka.Extract(ctx, msg.Headers)
		msgCtx, span := consumerTracer.Start(msgCtx, topic+" process",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination.name", topic),
			),
		)

		handleCtx, cancel := context.WithTimeout(msgCtx, handleTimeout)
		if err := c.app.Commands.CreateInvoiceFromRide.Handle(handleCtx, command.CreateInvoiceFromRide{
			RideID:      event.RideID,
			ClientID:    event.ClientID,
			DriverID:    event.DriverID,
			Type:        domain.InvoiceTypeCancellationFee,
			AmountMinor: event.FeeMinor,
			Currency:    event.Currency,
		}); err != nil {
			log.Println("handle error:", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		cancel()
		span.End()
	}
}
