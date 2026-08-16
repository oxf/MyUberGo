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
	"go.opentelemetry.io/otel/metric/noop"
)

// gaugeCallbackTimeout bounds a Gauge's callback so a slow read (e.g. a stuck `SELECT count(*)`)
// can't block the SDK's periodic collection cycle for every other instrument.
const gaugeCallbackTimeout = 5 * time.Second

// descriptions gives Prometheus HELP text to metrics this repo emits by name — a best-effort
// lookup, not a required registry; unlisted names still work, just without a description.
var descriptions = map[string]string{
	"myubergo.command.duration":             "Duration of a CQRS command handler invocation.",
	"myubergo.query.duration":               "Duration of a CQRS query handler invocation.",
	"myubergo.ride.time_to_match":           "Seconds from ride.requested to a successful match.",
	"myubergo.ride.estimated_fare_minor":    "Estimated ride fare, in minor currency units.",
	"myubergo.payment.amount_minor":         "Payment/invoice amount, in minor currency units.",
	"myubergo.signups":                      "Successful account signups.",
	"myubergo.logins":                       "Login attempts.",
	"myubergo.rides.requested":              "Rides requested.",
	"myubergo.rides.completed":              "Rides completed.",
	"myubergo.rides.cancelled":              "Rides cancelled.",
	"myubergo.matching.rides_failed":        "Rides that exhausted all matching retry attempts.",
	"myubergo.matching.broadcast_rounds":    "Broadcast rounds consumed before a ride was matched or failed.",
	"myubergo.matching.rate_limited":        "Offers withheld by the per-driver notification rate limit.",
	"myubergo.matching.offers_broadcast":    "Ride offers broadcast to drivers.",
	"myubergo.matching.accept_result":       "Outcome of a driver's ride-accept attempt.",
	"myubergo.driver.status_transitions":    "Driver status transitions.",
	"myubergo.shifts.started":               "Driver shifts started.",
	"myubergo.shifts.ended":                 "Driver shifts ended.",
	"myubergo.invoices.created":             "Invoices created.",
	"myubergo.payments.attempted":           "Payment collection attempts.",
	"myubergo.drivers.online":               "Drivers currently online.",
	"myubergo.outbox.pending":               "Outbox messages awaiting publish (retries below the park threshold).",
	"myubergo.outbox.parked":                "Outbox messages parked after exceeding the retry cap — requires manual triage.",
	"myubergo.location.pings_accepted":      "Location pings accepted after validation.",
	"myubergo.location.pings_rejected":      "Location pings rejected by validation, by reason.",
	"myubergo.location.ingest_lag":          "Seconds between a ping's device timestamp and server acceptance — the freshness SLI.",
	"myubergo.location.staleness_evictions": "Drivers evicted from the geo index for not pinging within the staleness window.",
	"myubergo.matching.location_fallbacks":  "Broadcast rounds that fell back to the rating-only pool because location-service was unavailable or erroring.",
}

func descriptionFor(name string) string { return descriptions[name] }

// Client's method set matches each service's local decorator.MetricsClient interface exactly, so it's
// directly assignable wherever that's expected — see services/*/internal/infrastructure/metrics/metrics.go.
type Client struct {
	meter metric.Meter

	mu         sync.RWMutex
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Registration
}

func NewClient(serviceName string) *Client {
	return &Client{
		meter:      otel.Meter(serviceName),
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Registration),
	}
}

// IncCounter increments a named counter by 1, with attributes — e.g. command.name/outcome from the
// decorators, or a business dimension like currency/role/type from a hand-instrumented handler.
func (c *Client) IncCounter(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	c.counter(name).Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordDuration records d (in seconds) into a named histogram. ctx is threaded through so the SDK
// can attach an exemplar linking the measurement to the currently active span, if any.
func (c *Client) RecordDuration(ctx context.Context, name string, d time.Duration, attrs ...attribute.KeyValue) {
	c.histogram(name, "s").Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
}

// RecordValue records an arbitrary non-duration value (fare, distance, a count) into a named histogram,
// distinct from RecordDuration so the instrument isn't mistagged with a seconds unit. Bucket boundaries come from otelinit.Setup's Views.
func (c *Client) RecordValue(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	c.histogram(name, "").Record(ctx, value, metric.WithAttributes(attrs...))
}

// Gauge registers a callback-driven observable gauge for values read on demand (e.g. `ZCARD drivers:online`)
// rather than pushed. Re-registering the same name retires the prior callback instead of stacking a second one.
func (c *Client) Gauge(name string, attrs []attribute.KeyValue, callback func(ctx context.Context) (int64, error)) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if prev, ok := c.gauges[name]; ok {
		if err := prev.Unregister(); err != nil {
			return fmt.Errorf("obsmetrics: unregister previous gauge %q: %w", name, err)
		}
		delete(c.gauges, name)
	}

	opts := []metric.Int64ObservableGaugeOption{}
	if desc := descriptionFor(name); desc != "" {
		opts = append(opts, metric.WithDescription(desc))
	}

	gauge, err := c.meter.Int64ObservableGauge(name, opts...)
	if err != nil {
		return fmt.Errorf("obsmetrics: create gauge %q: %w", name, err)
	}

	// A separate RegisterCallback (vs. metric.WithInt64Callback at creation) is what yields the
	// metric.Registration handle Unregister() needs to retire a stale callback on re-registration.
	reg, err := c.meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		cbCtx, cancel := context.WithTimeout(ctx, gaugeCallbackTimeout)
		defer cancel()
		v, err := callback(cbCtx)
		if err != nil {
			return err
		}
		obs.ObserveInt64(gauge, v, metric.WithAttributes(attrs...))
		return nil
	}, gauge)
	if err != nil {
		return fmt.Errorf("obsmetrics: register gauge callback %q: %w", name, err)
	}
	c.gauges[name] = reg
	return nil
}

// counter returns the cached instrument for name, creating it under a write lock only on first use —
// the common (already-cached) case takes just a read lock, so hot-path emission doesn't serialize on one writer.
func (c *Client) counter(name string) metric.Int64Counter {
	c.mu.RLock()
	ctr, ok := c.counters[name]
	c.mu.RUnlock()
	if ok {
		return ctr
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if ctr, ok := c.counters[name]; ok {
		return ctr
	}

	opts := []metric.Int64CounterOption{}
	if desc := descriptionFor(name); desc != "" {
		opts = append(opts, metric.WithDescription(desc))
	}
	ctr, err := c.meter.Int64Counter(name, opts...)
	if err != nil {
		// A malformed metric name must never take the process down: report via
		// otelinit.InstallErrorHandler's SDK error handler and degrade to a no-op instrument.
		otel.Handle(fmt.Errorf("obsmetrics: create counter %q: %w", name, err))
		return noop.Int64Counter{}
	}
	c.counters[name] = ctr
	return ctr
}

// histogram lazily creates (or returns the cached) instrument for name. unit is only consulted on first
// creation — no name in this repo is ever registered with two different units.
func (c *Client) histogram(name, unit string) metric.Float64Histogram {
	c.mu.RLock()
	h, ok := c.histograms[name]
	c.mu.RUnlock()
	if ok {
		return h
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.histograms[name]; ok {
		return h
	}

	opts := []metric.Float64HistogramOption{}
	if unit != "" {
		opts = append(opts, metric.WithUnit(unit))
	}
	if desc := descriptionFor(name); desc != "" {
		opts = append(opts, metric.WithDescription(desc))
	}
	h, err := c.meter.Float64Histogram(name, opts...)
	if err != nil {
		otel.Handle(fmt.Errorf("obsmetrics: create histogram %q: %w", name, err))
		return noop.Float64Histogram{}
	}
	c.histograms[name] = h
	return h
}
