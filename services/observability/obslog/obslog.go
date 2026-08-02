// Package obslog configures logrus consistently across services: JSON to
// stdout (so `docker-compose logs` stays useful), a level read from
// LOG_LEVEL (unset today, which is why every decorator's `logger.Debug(...)`
// call has always been silently dropped), a trace_id/span_id correlation
// hook, and a bridge that also ships every log line to the OTel Collector
// (and from there to Loki) via the global LoggerProvider installed by
// otelinit.Setup.
package obslog

import (
	"os"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/bridges/otellogrus"
	"go.opentelemetry.io/otel/trace"
)

// NewLogger returns a *logrus.Entry pre-configured for serviceName. Pass it
// wherever the repo's per-service decorator/health/shutdown/worker code
// today takes a *logrus.Entry — this is a drop-in replacement for the
// unconfigured `logrus.NewEntry(logrus.New())` every service's main()
// currently constructs.
//
// otelinit.Setup must run before this for the OTLP log bridge to have a
// real LoggerProvider to attach to; if it hasn't, otellogrus falls back to
// the global no-op provider and log lines simply aren't exported (stdout
// output is unaffected either way).
func NewLogger(serviceName string) *logrus.Entry {
	base := logrus.New()
	base.SetFormatter(&logrus.JSONFormatter{})
	base.SetLevel(levelFromEnv())
	base.SetOutput(os.Stdout)
	base.AddHook(traceCorrelationHook{})
	base.AddHook(otellogrus.NewHook(serviceName, otellogrus.WithLevels(logrus.AllLevels)))

	return logrus.NewEntry(base).WithField("service.name", serviceName)
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

// traceCorrelationHook stamps trace_id/span_id onto any entry created via
// (*logrus.Entry).WithContext(ctx) that carries an active OTel span, so
// stdout JSON lines are grep-able by trace ID even without going through
// Loki, and Loki's derived-field regex (see
// observability/grafana/provisioning/datasources/datasources.yaml) has a
// field to match against.
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
