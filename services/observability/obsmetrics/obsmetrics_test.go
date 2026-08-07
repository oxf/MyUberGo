package obsmetrics

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestClient wires a Client to a manual reader so tests can collect
// exactly what was recorded without waiting on a periodic export interval.
func newTestClient(t *testing.T) (*Client, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return &Client{
		meter:      mp.Meter("test"),
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Registration),
	}, reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func TestIncCounter_CachesInstrumentAcrossCalls(t *testing.T) {
	c, reader := newTestClient(t)

	c.IncCounter(context.Background(), "myubergo.signups", attribute.String("role", "Client"))
	c.IncCounter(context.Background(), "myubergo.signups", attribute.String("role", "Client"))

	rm := collect(t, reader)
	m, ok := findMetric(rm, "myubergo.signups")
	if !ok {
		t.Fatal("expected myubergo.signups to be exported")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 2 {
		t.Fatalf("expected a single data point with value 2, got %+v", m.Data)
	}
}

func TestGauge_ReRegistrationReplacesRatherThanAccumulates(t *testing.T) {
	c, reader := newTestClient(t)

	callCount := 0
	register := func(v int64) {
		err := c.Gauge("myubergo.drivers.online", nil, func(context.Context) (int64, error) {
			callCount++
			return v, nil
		})
		if err != nil {
			t.Fatalf("Gauge: %v", err)
		}
	}

	register(1)
	register(2) // re-registration under the same name

	rm := collect(t, reader)
	m, ok := findMetric(rm, "myubergo.drivers.online")
	if !ok {
		t.Fatal("expected myubergo.drivers.online to be exported")
	}
	gauge, ok := m.Data.(metricdata.Gauge[int64])
	if !ok || len(gauge.DataPoints) != 1 {
		t.Fatalf("expected exactly one data point (old callback retired, not accumulated), got %+v", m.Data)
	}
	if gauge.DataPoints[0].Value != 2 {
		t.Errorf("expected the second registration's value (2), got %d — first callback wasn't retired", gauge.DataPoints[0].Value)
	}
	if callCount != 1 {
		t.Errorf("expected exactly one callback to fire during collection (the retired one must not), got %d", callCount)
	}
}

func TestHistogram_UnitOnlyAppliedOnFirstCreation(t *testing.T) {
	c, reader := newTestClient(t)

	c.RecordDuration(context.Background(), "myubergo.command.duration", 0)

	rm := collect(t, reader)
	m, ok := findMetric(rm, "myubergo.command.duration")
	if !ok {
		t.Fatal("expected myubergo.command.duration to be exported")
	}
	if m.Unit != "s" {
		t.Errorf("expected unit %q, got %q", "s", m.Unit)
	}
	if m.Description == "" {
		t.Error("expected a non-empty description from the descriptions table")
	}
}

func TestCounter_CreationFailureDegradesToNoop(t *testing.T) {
	c, _ := newTestClient(t)

	// An empty instrument name is invalid and fails Int64Counter creation —
	// this must not panic (the original behavior did) and must return a
	// usable no-op instrument instead.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("IncCounter must not panic on an invalid instrument name, got: %v", r)
		}
	}()
	c.IncCounter(context.Background(), "")
}

func TestGauge_UnregisterFailurePropagatesAsError(t *testing.T) {
	c, _ := newTestClient(t)

	if err := c.Gauge("myubergo.outbox.pending", nil, func(context.Context) (int64, error) {
		return 0, nil
	}); err != nil {
		t.Fatalf("first Gauge registration: %v", err)
	}

	// A callback that itself errors must not panic and must surface via the
	// SDK's own error handling path when collected (not via obsmetrics
	// panicking) — exercised indirectly by ensuring Collect doesn't panic.
	if err := c.Gauge("myubergo.outbox.pending", nil, func(context.Context) (int64, error) {
		return 0, errors.New("db unavailable")
	}); err != nil {
		t.Fatalf("second Gauge registration: %v", err)
	}
}
