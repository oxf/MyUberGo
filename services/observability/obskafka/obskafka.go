// Package obskafka propagates OTel trace context through
// github.com/segmentio/kafka-go message headers. kafka-go has no official
// OTel contrib instrumentation, so this carrier is hand-written — it is the
// piece that lets a trace survive a Kafka hop (matching-service's direct
// `ride.accepted` publish, and every outbox-published event once its
// producing worker calls Inject with a context rebuilt from the outbox
// row's stored trace context — see obsoutbox).
package obskafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

// headerCarrier adapts a []kafka.Header to propagation.TextMapCarrier.
type headerCarrier struct {
	headers *[]kafka.Header
}

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
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

// Inject returns the []kafka.Header carrying ctx's trace context and
// baggage, ready to attach to an outgoing kafka.Message.Headers.
func Inject(ctx context.Context) []kafka.Header {
	var headers []kafka.Header
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})
	return headers
}

// Extract returns a context carrying the trace context and baggage
// propagated in an incoming kafka.Message's headers, so a consumer's
// handler context joins the producer's trace instead of starting a new one.
func Extract(ctx context.Context, headers []kafka.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &headers})
}
