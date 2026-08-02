// Package obsmetrics is an OTel-backed implementation of each service's
// local decorator.MetricsClient interface.
package obsmetrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Client's method set (IncCounter/RecordDuration) matches each service's
// local decorator.MetricsClient interface exactly, so a *Client is directly
// assignable wherever that interface is expected — see
// services/*/internal/infrastructure/metrics/metrics.go, which constructs
// one of these in place of the old logging-only LoggingMetricsClient.
type Client struct {
	meter metric.Meter

	mu         sync.Mutex
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
}

func NewClient(serviceName string) *Client {
	return &Client{
		meter:      otel.Meter(serviceName),
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
	}
}

// IncCounter increments a named counter by 1, with the given attributes —
// e.g. command.name/outcome from the decorators, or a business dimension
// like currency/role/type from a hand-instrumented command handler.
func (c *Client) IncCounter(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	c.counter(name).Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordDuration records d (in seconds) into a named histogram. ctx is
// threaded through so the SDK can attach an exemplar linking the
// measurement to the currently active span, if any.
func (c *Client) RecordDuration(ctx context.Context, name string, d time.Duration, attrs ...attribute.KeyValue) {
	c.histogram(name, "s").Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
}

// RecordValue records an arbitrary (non-duration) value into a named
// histogram — e.g. a fare amount in minor units, a distance in km, a count
// of offer rounds. Distinct from RecordDuration so callers never have to
// smuggle a non-time value through time.Duration to make the units line
// up — and, critically, so the resulting instrument isn't tagged with
// RecordDuration's "s" (seconds) unit, which would otherwise mislabel e.g.
// a money value as a duration in the exported metric name/metadata.
func (c *Client) RecordValue(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	c.histogram(name, "").Record(ctx, value, metric.WithAttributes(attrs...))
}

// Gauge registers a callback-driven observable gauge — for values that are
// read on demand (e.g. `ZCARD drivers:online`, `SELECT count(*) ... WHERE
// status='open'`) rather than pushed on every state change.
func (c *Client) Gauge(name string, attrs []attribute.KeyValue, callback func(ctx context.Context) (int64, error)) error {
	_, err := c.meter.Int64ObservableGauge(name,
		metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
			v, err := callback(ctx)
			if err != nil {
				return err
			}
			obs.Observe(v, metric.WithAttributes(attrs...))
			return nil
		}),
	)
	return err
}

func (c *Client) counter(name string) metric.Int64Counter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctr, ok := c.counters[name]; ok {
		return ctr
	}
	ctr, err := c.meter.Int64Counter(name)
	if err != nil {
		panic(fmt.Sprintf("obsmetrics: create counter %q: %v", name, err))
	}
	c.counters[name] = ctr
	return ctr
}

// histogram lazily creates (or returns the cached) instrument for name.
// unit is only consulted on first creation — callers never mix
// RecordDuration and RecordValue for the same metric name in this repo, so
// a name is never registered with two different units.
func (c *Client) histogram(name, unit string) metric.Float64Histogram {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.histograms[name]; ok {
		return h
	}
	opts := []metric.Float64HistogramOption{}
	if unit != "" {
		opts = append(opts, metric.WithUnit(unit))
	}
	h, err := c.meter.Float64Histogram(name, opts...)
	if err != nil {
		panic(fmt.Sprintf("obsmetrics: create histogram %q: %v", name, err))
	}
	c.histograms[name] = h
	return h
}
