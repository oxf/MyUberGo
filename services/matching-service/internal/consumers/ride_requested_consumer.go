package consumers

import (
	"context"
	"encoding/json"
	"log"
	app "matching-service/internal/application"
	"matching-service/internal/application/command"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// handleTimeout bounds how long a single message's command handling can
// run, so a hung Redis call can't block the consumer's read loop (and, by
// extension, every message behind it) indefinitely.
const handleTimeout = 10 * time.Second

// tracer is shared across every consumer in this package.
var tracer = otel.Tracer("matching-service/consumer")

func consumerSpanAttrs(topic string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", topic),
	}
}

type RideRequestedConsumer struct {
	app    app.Application
	broker string
}

func NewRideRequestedConsumer(app app.Application, broker string) *RideRequestedConsumer {
	return &RideRequestedConsumer{app: app, broker: broker}
}

func (c *RideRequestedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "matching-service",
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

		var event contractsKafka.RideRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf("Ride request received. RideID=%s ClientID=%s PriceMinor=%d %s",
			event.RideID, event.ClientID, event.PriceMinor, event.Currency)

		msgCtx := obskafka.Extract(ctx, msg.Headers)
		msgCtx, span := tracer.Start(msgCtx, topic+" process",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(consumerSpanAttrs(topic)...),
		)
		if err := c.handleRideRequested(msgCtx, event); err != nil {
			log.Println("handle error:", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func (c *RideRequestedConsumer) handleRideRequested(ctx context.Context, event contractsKafka.RideRequestedEvent) error {
	ctx, cancel := context.WithTimeout(ctx, handleTimeout)
	defer cancel()
	if err := c.app.Commands.CreateRide.Handle(ctx, command.CreateRide{Event: event}); err != nil {
		return err
	}
	return c.app.Commands.BroadcastOffers.Handle(ctx, command.BroadcastOffers{RideID: event.RideID, Attempt: 1})
}
