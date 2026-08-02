package consumers

import (
	"context"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"encoding/json"
	"log"
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
// run, so a hung DB/dependency call can't block the consumer's read loop
// (and, by extension, every message behind it) indefinitely.
const handleTimeout = 10 * time.Second

// consumerTracer is shared by all three consumers in this package (they're
// all `package consumers`) to avoid a duplicate package-level declaration.
var consumerTracer = otel.Tracer("driver-service/consumer")

type RideAcceptedConsumer struct {
	app    app.Application
	broker string
}

func NewRideAcceptedConsumer(app app.Application, broker string) *RideAcceptedConsumer {
	return &RideAcceptedConsumer{app: app, broker: broker}
}

func (c *RideAcceptedConsumer) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.broker},
		Topic:   topic,
		GroupID: "driver-service",
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

		var event contractsKafka.RideAcceptedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
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
		if err := c.app.Commands.ProcessRideAccepted.Handle(handleCtx, command.ProcessRideAccepted{
			RideID:     event.RideID,
			DriverID:   event.DriverID,
			AcceptedAt: event.AcceptedAt,
		}); err != nil {
			log.Println("handle error:", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		cancel()
		span.End()
	}
}
