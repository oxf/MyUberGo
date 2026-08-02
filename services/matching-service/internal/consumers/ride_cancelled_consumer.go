package consumers

import (
	"context"
	"encoding/json"
	"log"
	app "matching-service/internal/application"
	"matching-service/internal/application/command"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
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

		var event contractsKafka.RideCancelledEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to deserialize event:", err)
			continue
		}

		log.Printf("Ride cancellation received. RideID=%s DriverID=%v", event.RideID, event.DriverID)

		msgCtx := obskafka.Extract(ctx, msg.Headers)
		msgCtx, span := tracer.Start(msgCtx, topic+" process",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(consumerSpanAttrs(topic)...),
		)

		handleCtx, cancel := context.WithTimeout(msgCtx, handleTimeout)
		if err := c.app.Commands.CancelRide.Handle(handleCtx, command.CancelRide{
			RideID:   event.RideID,
			DriverID: event.DriverID,
		}); err != nil {
			log.Println("handle error:", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		cancel()
		span.End()
	}
}
