// Package obskafka propagates OTel trace context through kafka-go message headers and centralizes producer/consumer span
// creation with OTel messaging semconv attributes, hand-written since kafka-go has no official OTel instrumentation — this is what lets a trace survive a Kafka hop and keeps the ~10 producer/consumer call sites across 5 services from duplicating the same attribute list.
package obskafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// headerCarrier adapts a []kafka.Header to propagation.TextMapCarrier.
type headerCarrier struct {
	headers *[]kafka.Header
}

// Get is case-insensitive: W3C mandates lowercase header names (e.g. "traceparent"), but a non-Go producer could write
// "Traceparent" — a case-sensitive lookup would silently fail to extract it even though the header is present.
func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if strings.EqualFold(h.Key, key) {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

// Inject returns a new []kafka.Header carrying ctx's trace context and baggage, ready to attach to an outgoing message.
// Prefer InjectInto when the caller already has its own headers to send — Inject always starts from an empty slice.
func Inject(ctx context.Context) []kafka.Header {
	return InjectInto(ctx, nil)
}

// InjectInto merges ctx's trace context and baggage into existing (may be nil), upserting into the same slice rather
// than blindly appending — safe to call on a caller-supplied header slice with unrelated headers already in it.
func InjectInto(ctx context.Context, existing []kafka.Header) []kafka.Header {
	headers := existing
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})
	return headers
}

// Extract returns a context carrying the trace context and baggage from an incoming kafka.Message's headers, so a
// consumer's handler context joins the producer's trace instead of starting a new one.
func Extract(ctx context.Context, headers []kafka.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &headers})
}

// StartProducerSpan starts a "publish <topic>" producer span tagged with the OTel messaging semconv attributes this
// repo's dashboards and the Collector's span_metrics/service_graph connectors key off. Callers must end the returned span exactly once per message via FinishSpan, in a per-message scope — never a bare `span.End()` in a for loop, which leaks on panic.
func StartProducerSpan(ctx context.Context, tracer trace.Tracer, topic string, extraAttrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs := append([]attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(topic),
		semconv.MessagingOperationName("publish"),
		semconv.MessagingOperationTypeSend,
	}, extraAttrs...)
	return tracer.Start(ctx, "publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
}

// StartConsumerSpan starts a "<topic> process" consumer span for msg, tagged with partition/offset/consumer-group so a
// slow or erroring span can be traced back to a specific lagging partition. Call obskafka.Extract on msg.Headers first and pass the resulting ctx in, so the new span joins the producer's trace.
func StartConsumerSpan(ctx context.Context, tracer trace.Tracer, topic, consumerGroup string, msg kafka.Message, extraAttrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs := append([]attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(topic),
		semconv.MessagingOperationName("process"),
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingConsumerGroupName(consumerGroup),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
		semconv.MessagingKafkaOffset(int(msg.Offset)),
	}, extraAttrs...)
	if len(msg.Key) > 0 {
		attrs = append(attrs, semconv.MessagingKafkaMessageKey(string(msg.Key)))
	}
	return tracer.Start(ctx, topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
}

// FinishSpan records err (if non-nil) on span and ends it. Deploy it and RecoverSpan as two SEPARATE, direct defers
// scoped to a single message — never a bare `span.End()` in a for loop (drops the span on panic), and never nested in one closure (breaks Go's recover() semantics — verified empirically, see RecoverSpan):
//
//	func handleOne(ctx context.Context, msg kafka.Message) (err error) {
//		ctx, span := obskafka.StartConsumerSpan(ctx, tracer, topic, group, msg)
//		defer func() { obskafka.FinishSpan(span, err) }()
//		defer obskafka.RecoverSpan(span)
//		...
//	}
//
// Order matters: FinishSpan must be deferred BEFORE RecoverSpan, so RecoverSpan (deferred later, running FIRST during a
// panic unwind) marks the exception while the span is still recording; FinishSpan's later span.End() then becomes a safe no-op.
func FinishSpan(span trace.Span, err error) {
	defer span.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// RecoverSpan must be deferred directly (not nested) by the same function that defers FinishSpan — see its doc comment
// for the required order. On a panic it marks span as an error, ends it, and re-panics so goSafe still handles it.
func RecoverSpan(span trace.Span) {
	if r := recover(); r != nil {
		span.RecordError(fmt.Errorf("panic: %v", r))
		span.SetStatus(codes.Error, "panic")
		span.End()
		panic(r)
	}
}

// TruncateForLog bounds a raw Kafka message payload to at most max bytes, safe to attach to a log line or span
// attribute for a "poison message" that failed to deserialize, without risking an unbounded or binary dump into the log/trace backend.
func TruncateForLog(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return fmt.Sprintf("%s... (%d more bytes)", string(b[:max]), len(b)-max)
}
