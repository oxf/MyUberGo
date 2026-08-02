package metrics

import (
	"context"
	"matching-service/internal/common/decorator"
	"time"

	"github.com/oxf/MyUber/observability/obsmetrics"
	"go.opentelemetry.io/otel/attribute"
)

// NewOtelMetricsClient returns an OTel-backed MetricsClient exporting via
// the OTLP pipeline configured by otelinit.Setup in cmd/main.go. Replaces
// the old logging-only LoggingMetricsClient stub. Returns the concrete
// *obsmetrics.Client (assignable to decorator.MetricsClient wherever that's
// expected) rather than the bare interface, so callers that also need
// obsmetrics.Client's Gauge method (observable gauges — e.g. drivers
// online) don't have to import services/observability/obsmetrics directly.
func NewOtelMetricsClient(serviceName string) *obsmetrics.Client {
	return obsmetrics.NewClient(serviceName)
}

// NoopMetricsClient discards all metrics — used by tests that don't care
// about metrics recording.
type NoopMetricsClient struct{}

func NewNoopMetricsClient() decorator.MetricsClient {
	return &NoopMetricsClient{}
}

func (n *NoopMetricsClient) IncCounter(ctx context.Context, name string, attrs ...attribute.KeyValue) {
}

func (n *NoopMetricsClient) RecordDuration(ctx context.Context, name string, d time.Duration, attrs ...attribute.KeyValue) {
}

func (n *NoopMetricsClient) RecordValue(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
}
