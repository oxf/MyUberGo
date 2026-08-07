package metrics

import (
	"billing-service/internal/common/decorator"
	"context"
	"time"

	"github.com/oxf/MyUber/observability/obsmetrics"
	"go.opentelemetry.io/otel/attribute"
)

// NewOtelMetricsClient returns an OTel-backed MetricsClient. Returns the concrete *obsmetrics.Client (not the
// decorator.MetricsClient interface) so cmd/main.go can also reach Gauge, used for the outbox pending/parked gauges.
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
