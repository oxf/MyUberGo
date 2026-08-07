package obsdecorator

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newTestTracer swaps the package-level tracer for one backed by an
// in-memory synchronous exporter, so tests can assert on exactly what got
// recorded without any batching/export delay. It restores the original
// tracer on cleanup so tests don't leak state into each other.
func newTestTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	prevTracer := tracer
	tracer = tp.Tracer("test")
	t.Cleanup(func() { tracer = prevTracer })
	return exporter
}

type fakeCmd struct{ shouldFail, shouldPanic bool }

type fakeCmdHandler struct{}

func (fakeCmdHandler) Handle(_ context.Context, cmd fakeCmd) (string, error) {
	if cmd.shouldPanic {
		panic("boom")
	}
	if cmd.shouldFail {
		return "", errors.New("handler failed")
	}
	return "ok", nil
}

var errExpected = errors.New("expected domain error")

func TestTraceCommand_Success(t *testing.T) {
	exporter := newTestTracer(t)

	h := TraceCommand(fakeCmdHandler{}, nil)
	result, err := h.Handle(context.Background(), fakeCmd{})
	if err != nil || result != "ok" {
		t.Fatalf("unexpected result: %q, %v", result, err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "command fakeCmd" {
		t.Errorf("unexpected span name: %q", spans[0].Name)
	}
	if spans[0].Status.Code == codes.Error {
		t.Errorf("success should not mark span as error")
	}
}

func TestTraceCommand_UnclassifiedErrorMarksSpanError(t *testing.T) {
	exporter := newTestTracer(t)

	h := TraceCommand(fakeCmdHandler{}, nil)
	_, err := h.Handle(context.Background(), fakeCmd{shouldFail: true})
	if err == nil {
		t.Fatal("expected error")
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Status.Code != codes.Error {
		t.Errorf("expected span status Error, got %v", spans[0].Status.Code)
	}
	if len(spans[0].Events) == 0 {
		t.Errorf("expected the error to be recorded as a span event")
	}
}

func TestTraceCommand_ExpectedErrorDoesNotMarkSpanError(t *testing.T) {
	exporter := newTestTracer(t)

	failing := fakeCmdHandlerFunc(func(context.Context, fakeCmd) (string, error) {
		return "", errExpected
	})
	isExpected := func(err error) bool { return errors.Is(err, errExpected) }

	h := TraceCommand(failing, isExpected)
	_, err := h.Handle(context.Background(), fakeCmd{})
	if err == nil {
		t.Fatal("expected error to propagate to the caller")
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	// The error must still be visible on the span (RecordError) — only the
	// span STATUS should be spared, so an individual trace remains
	// diagnosable while the aggregate error-rate ratio isn't inflated by
	// business-as-usual outcomes.
	if len(spans[0].Events) == 0 {
		t.Errorf("expected the error to still be recorded as a span event")
	}
	if spans[0].Status.Code == codes.Error {
		t.Errorf("expected span status to NOT be Error for a classified-expected error")
	}
}

// TestTraceCommand_PanicIsRecordedAndPropagates locks in the fix for the
// bug where a panicking handler left its span permanently unended (the SDK
// drops spans that never call End()). It also pins the defer-ordering that
// makes recoverAndReraise's exception recording actually take effect —
// swapping the defer order in tracedCommand.Handle silently breaks this
// (RecordError/SetStatus become no-ops on an already-ended span) without
// making the code fail to compile, so this test is the only thing that
// would catch a regression there.
func TestTraceCommand_PanicIsRecordedAndPropagates(t *testing.T) {
	exporter := newTestTracer(t)

	h := TraceCommand(fakeCmdHandler{}, nil)

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected the panic to propagate to the caller")
			}
			if r != "boom" {
				t.Fatalf("unexpected panic value: %v", r)
			}
		}()
		_, _ = h.Handle(context.Background(), fakeCmd{shouldPanic: true})
	}()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span even though the handler panicked, got %d", len(spans))
	}
	if spans[0].EndTime.IsZero() {
		t.Fatal("expected the span to have been ended despite the panic")
	}
	if spans[0].Status.Code != codes.Error {
		t.Errorf("expected span status Error after a panic, got %v", spans[0].Status.Code)
	}
	if len(spans[0].Events) == 0 {
		t.Errorf("expected the panic to be recorded as a span exception event")
	}
}

type fakeCmdHandlerFunc func(context.Context, fakeCmd) (string, error)

func (f fakeCmdHandlerFunc) Handle(ctx context.Context, cmd fakeCmd) (string, error) {
	return f(ctx, cmd)
}

func TestActionName(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want string
	}{
		{"package-qualified struct", fakeCmd{}, "fakeCmd"},
		{"pointer to package-qualified struct", &fakeCmd{}, "fakeCmd"},
		{"bare type with no dot never panics", "a string command", "string"},
		{"map with no dot never panics", map[string]int{}, "map[string]int"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ActionName(tc.v)
			if got != tc.want {
				t.Errorf("ActionName(%v) = %q, want %q", tc.v, got, tc.want)
			}
		})
	}
}
