package metrics

import (
	"billing-service/internal/common/decorator"
	"context"
	"time"

	"github.com/oxf/MyUber/observability/obsmetrics"
	"go.opentelemetry.io/otel/attribute"
)

// NewOtelMetricsClient returns an OTel-backed MetricsClient exporting via
// the OTLP pipeline configured by otelinit.Setup in cmd/main.go. Replaces
// the old logging-only LoggingMetricsClient stub.
func NewOtelMetricsClient(serviceName string) decorator.MetricsClient {
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
