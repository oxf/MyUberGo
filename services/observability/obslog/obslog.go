// Package obslog configures logrus consistently across services: JSON to stdout, LOG_LEVEL-driven level,
// trace_id/span_id correlation, and a bridge shipping logs to the OTel Collector via otelinit.Setup's LoggerProvider.
package obslog

import (
	"context"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/bridges/otellogrus"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/trace"
)

// NewLogger returns a *logrus.Entry pre-configured for serviceName, a drop-in replacement for the
// unconfigured logrus.NewEntry. otelinit.Setup must run first, or the OTLP bridge falls back to a no-op provider.
func NewLogger(serviceName string) *logrus.Entry {
	base := logrus.New()
	base.SetFormatter(&logrus.JSONFormatter{})
	base.SetLevel(levelFromEnv())
	base.SetOutput(os.Stdout)
	base.AddHook(traceCorrelationHook{})
	base.AddHook(otellogrus.NewHook(serviceName, otellogrus.WithLevels(logrus.AllLevels)))

	registerFatalFlush()

	return logrus.NewEntry(base).WithField("service.name", serviceName)
}

// registerFatalFlushOnce guards logrus.RegisterExitHandler so a process constructing more than one
// obslog logger (unusual outside tests) doesn't stack up duplicate flush handlers.
var registerFatalFlushOnce = func() func() {
	done := false
	return func() {
		if done {
			return
		}
		done = true
		logrus.RegisterExitHandler(flushLoggerProvider)
	}
}()

func registerFatalFlush() { registerFatalFlushOnce() }

// flushLoggerProvider force-flushes the global LoggerProvider before logrus.Fatal's os.Exit(1) runs, so the
// fatal line itself reaches Loki instead of just stdout. 3s budget (matches other shutdown timeouts) bounds a wedged exporter.
func flushLoggerProvider() {
	provider := logglobal.GetLoggerProvider()
	flusher, ok := provider.(interface{ ForceFlush(context.Context) error })
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = flusher.ForceFlush(ctx)
}

func levelFromEnv() logrus.Level {
	raw := os.Getenv("LOG_LEVEL")
	if raw == "" {
		return logrus.InfoLevel
	}
	level, err := logrus.ParseLevel(raw)
	if err != nil {
		return logrus.InfoLevel
	}
	return level
}

// traceCorrelationHook stamps trace_id/span_id onto any entry created via (*logrus.Entry).WithContext(ctx)
// with an active span, making stdout JSON grep-able by trace ID; in Loki these arrive as structured metadata, not a body regex.
type traceCorrelationHook struct{}

func (traceCorrelationHook) Levels() []logrus.Level { return logrus.AllLevels }

func (traceCorrelationHook) Fire(e *logrus.Entry) error {
	if e.Context == nil {
		return nil
	}
	sc := trace.SpanContextFromContext(e.Context)
	if !sc.IsValid() {
		return nil
	}
	e.Data["trace_id"] = sc.TraceID().String()
	e.Data["span_id"] = sc.SpanID().String()
	return nil
}
