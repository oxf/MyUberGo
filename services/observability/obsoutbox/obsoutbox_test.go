package obsoutbox

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

func TestMarshalTraceContext_NoActiveTraceReturnsNil(t *testing.T) {
	if got := MarshalTraceContext(context.Background()); got != nil {
		t.Errorf("expected nil for a context with no active trace, got %q", got)
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	carrier := propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

	data := MarshalTraceContext(ctx)
	if data == nil {
		t.Fatal("expected non-nil marshaled trace context")
	}

	rehydrated := UnmarshalTraceContext(context.Background(), data)
	sc := trace.SpanContextFromContext(rehydrated)
	if !sc.IsValid() {
		t.Fatal("expected the rehydrated context to carry a valid remote span context")
	}
	if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace ID did not round-trip: got %s", sc.TraceID())
	}
	if !sc.IsRemote() {
		t.Error("expected the rehydrated span context to be marked remote")
	}
}

type testMarkerKey struct{}

func TestUnmarshalTraceContext_EmptyReturnsCtxUnchanged(t *testing.T) {
	base := context.WithValue(context.Background(), testMarkerKey{}, "marker")
	got := UnmarshalTraceContext(base, nil)
	if got != base {
		t.Error("expected the same context back for empty data")
	}
}

func TestUnmarshalTraceContext_CorruptDataDegradesGracefully(t *testing.T) {
	base := context.Background()
	got := UnmarshalTraceContext(base, []byte("not json"))
	if got != base {
		t.Error("expected corrupt data to degrade to the input context unchanged")
	}
}
