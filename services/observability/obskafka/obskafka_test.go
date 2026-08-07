package obskafka

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func init() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

func TestHeaderCarrier_GetIsCaseInsensitive(t *testing.T) {
	headers := []kafka.Header{{Key: "Traceparent", Value: []byte("00-abc-def-01")}}
	c := headerCarrier{headers: &headers}

	if got := c.Get("traceparent"); got != "00-abc-def-01" {
		t.Errorf("Get(lowercase) = %q, want match against differently-cased header", got)
	}
	if got := c.Get("TRACEPARENT"); got != "00-abc-def-01" {
		t.Errorf("Get(uppercase) = %q, want match", got)
	}
}

func TestHeaderCarrier_SetUpsertsInPlace(t *testing.T) {
	headers := []kafka.Header{{Key: "traceparent", Value: []byte("old")}}
	c := headerCarrier{headers: &headers}

	c.Set("traceparent", "new")

	if len(headers) != 1 {
		t.Fatalf("expected Set to upsert in place, got %d headers: %v", len(headers), headers)
	}
	if string(headers[0].Value) != "new" {
		t.Errorf("expected updated value, got %q", headers[0].Value)
	}
}

func TestInjectExtractRoundTrip(t *testing.T) {
	// A traceparent-bearing context injected into headers, then extracted
	// back, must yield a context whose span context matches — this is the
	// mechanism that lets a trace survive a Kafka hop.
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := context.Background()
	carrier := propagation.MapCarrier{"traceparent": tp}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	headers := Inject(ctx)
	if len(headers) == 0 {
		t.Fatal("expected Inject to produce at least the traceparent header")
	}

	extracted := Extract(context.Background(), headers)
	roundTripped := Inject(extracted)

	if len(roundTripped) != len(headers) {
		t.Fatalf("round-tripped header count = %d, want %d", len(roundTripped), len(headers))
	}
	for i := range headers {
		if headers[i].Key != roundTripped[i].Key || string(headers[i].Value) != string(roundTripped[i].Value) {
			t.Errorf("header %d did not round-trip: got %+v, want %+v", i, roundTripped[i], headers[i])
		}
	}
}

func TestInjectIntoMergesWithExistingHeaders(t *testing.T) {
	ctx := context.Background()
	carrier := propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	existing := []kafka.Header{{Key: "outbox.event_type", Value: []byte("ride.requested")}}
	merged := InjectInto(ctx, existing)

	if len(merged) < 2 {
		t.Fatalf("expected InjectInto to keep the caller's header alongside the injected ones, got %v", merged)
	}
	found := false
	for _, h := range merged {
		if h.Key == "outbox.event_type" && string(h.Value) == "ride.requested" {
			found = true
		}
	}
	if !found {
		t.Errorf("caller-supplied header was lost/clobbered: %v", merged)
	}
}

func TestTruncateForLog(t *testing.T) {
	short := []byte("hello")
	if got := TruncateForLog(short, 10); got != "hello" {
		t.Errorf("short payload should pass through unchanged, got %q", got)
	}

	long := []byte("0123456789abcdef")
	got := TruncateForLog(long, 10)
	want := "0123456789... (6 more bytes)"
	if got != want {
		t.Errorf("TruncateForLog(long, 10) = %q, want %q", got, want)
	}
}
