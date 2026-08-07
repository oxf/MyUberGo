package obslog

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceCorrelationHook_NoContextIsNoop(t *testing.T) {
	e := logrus.NewEntry(logrus.New())
	if err := (traceCorrelationHook{}).Fire(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := e.Data["trace_id"]; ok {
		t.Error("expected no trace_id field when the entry has no context")
	}
}

func TestTraceCorrelationHook_InvalidSpanContextIsNoop(t *testing.T) {
	e := logrus.NewEntry(logrus.New()).WithContext(context.Background())
	if err := (traceCorrelationHook{}).Fire(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := e.Data["trace_id"]; ok {
		t.Error("expected no trace_id field for a context with no active span")
	}
}

func TestTraceCorrelationHook_StampsTraceAndSpanID(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	e := logrus.NewEntry(logrus.New()).WithContext(ctx)

	if err := (traceCorrelationHook{}).Fire(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := e.Data["trace_id"]; got != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %v", got, sc.TraceID().String())
	}
	if got := e.Data["span_id"]; got != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %v", got, sc.SpanID().String())
	}
}

type fakeFlushableProvider struct {
	noop.LoggerProvider
	flushed bool
}

func (f *fakeFlushableProvider) ForceFlush(context.Context) error {
	f.flushed = true
	return nil
}

var _ log.LoggerProvider = (*fakeFlushableProvider)(nil)

func TestFlushLoggerProvider_FlushesWhenSupported(t *testing.T) {
	prev := global.GetLoggerProvider()
	defer global.SetLoggerProvider(prev)

	fake := &fakeFlushableProvider{}
	global.SetLoggerProvider(fake)

	flushLoggerProvider()

	if !fake.flushed {
		t.Error("expected flushLoggerProvider to call ForceFlush on a provider that supports it")
	}
}

func TestFlushLoggerProvider_NoopWhenUnsupported(t *testing.T) {
	prev := global.GetLoggerProvider()
	defer global.SetLoggerProvider(prev)

	global.SetLoggerProvider(noop.NewLoggerProvider())

	// Must not panic even though noop.LoggerProvider has no ForceFlush method.
	flushLoggerProvider()
}
