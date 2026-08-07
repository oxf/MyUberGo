// Package otelinit bootstraps the OpenTelemetry SDK (traces, metrics, logs) as global providers from the
// standard OTEL_* env vars. OTEL_SDK_DISABLED is the one exception: unimplemented by the Go SDK itself, so Setup honors it via an explicit early return.
package otelinit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// Providers holds the three SDK providers configured by Setup, with a means to flush/close them together.
// A zero-value Providers (Setup's return when OTEL_SDK_DISABLED is set) makes Shutdown a correct no-op.
type Providers struct {
	Tracer *sdktrace.TracerProvider
	Meter  *sdkmetric.MeterProvider
	Logger *sdklog.LoggerProvider
}

// durationViews bounds the histogram buckets for this repo's hand-emitted application metrics — the SDK's
// default boundaries are millisecond-shaped, making p50/p95/p99 meaningless for anything measured in seconds.
func durationViews() []sdkmetric.View {
	commandBuckets := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	timeToMatchBuckets := []float64{.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300}
	fareMinorBuckets := []float64{100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000}

	return []sdkmetric.View{
		// obsmetrics.RecordDuration always feeds seconds; command/query
		// duration share one bucket set.
		sdkmetric.NewView(
			sdkmetric.Instrument{Name: "myubergo.*.duration"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: commandBuckets}},
		),
		// Recorded via RecordValue (not RecordDuration) but is itself a seconds value that can run
		// much longer than a command — matching can legitimately take tens of seconds across retry rounds.
		sdkmetric.NewView(
			sdkmetric.Instrument{Name: "myubergo.ride.time_to_match"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: timeToMatchBuckets}},
		),
		// Money histograms (minor units) — the millisecond-shaped default
		// buckets are accidentally tolerable for these, never intentional.
		sdkmetric.NewView(
			sdkmetric.Instrument{Name: "myubergo.ride.estimated_fare_minor"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: fareMinorBuckets}},
		),
		sdkmetric.NewView(
			sdkmetric.Instrument{Name: "myubergo.payment.amount_minor"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: fareMinorBuckets}},
		),
	}
}

// InstallErrorHandler routes internal OTel SDK errors (export failures, dropped batches) through structured
// JSON logging on a dedicated logger never wired to the OTLP bridge, so a failure can't recursively retrigger itself.
func InstallErrorHandler(serviceName string) {
	errLogger := logrus.New()
	errLogger.SetFormatter(&logrus.JSONFormatter{})
	errLogger.SetOutput(os.Stderr)
	entry := errLogger.WithFields(logrus.Fields{
		"service.name": serviceName,
		"component":    "otel-sdk",
	})

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		entry.WithError(err).Error("otel sdk internal error")
	}))
}

// Setup builds gRPC OTLP exporters for traces/metrics/logs, installs them as the global providers, and
// starts the Go runtime metrics reader. Exporters dial lazily, so a down Collector degrades telemetry rather than failing boot.
func Setup(ctx context.Context, serviceName string) (*Providers, error) {
	if os.Getenv("OTEL_SDK_DISABLED") == "true" {
		return &Providers{}, nil
	}

	InstallErrorHandler(serviceName)

	res, err := resource.New(ctx,
		// Defaults — anything set via OTEL_RESOURCE_ATTRIBUTES below wins,
		// since resource.New merges left-to-right with last-value-wins.
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceInstanceID(uuid.NewString()),
		),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithProcess(),
		resource.WithContainer(),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build resource: %w", err)
	}

	// cleanup accumulates shutdown funcs for everything installed so far, so a later step's failure can't
	// leak an already-globally-installed provider that Setup never returned to the caller.
	var cleanup []func(context.Context)
	unwind := func(ctx context.Context) {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i](ctx)
		}
	}

	traceExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("observability: build trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	cleanup = append(cleanup, func(ctx context.Context) { _ = tp.Shutdown(ctx) })

	metricExporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		unwind(ctx)
		return nil, fmt.Errorf("observability: build metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(durationViews()...),
	)
	otel.SetMeterProvider(mp)
	cleanup = append(cleanup, func(ctx context.Context) { _ = mp.Shutdown(ctx) })

	logExporter, err := otlploggrpc.New(ctx)
	if err != nil {
		unwind(ctx)
		return nil, fmt.Errorf("observability: build log exporter: %w", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	logglobal.SetLoggerProvider(lp)
	cleanup = append(cleanup, func(ctx context.Context) { _ = lp.Shutdown(ctx) })

	if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
		unwind(ctx)
		return nil, fmt.Errorf("observability: start runtime metrics: %w", err)
	}

	return &Providers{Tracer: tp, Meter: mp, Logger: lp}, nil
}

// Shutdown flushes and closes all three providers; register via each service's shutdown.Manager.OnStop so
// in-flight telemetry isn't dropped on exit. Each provider gets its own bounded 5s sub-timeout of ctx.
func (p *Providers) Shutdown(ctx context.Context) error {
	const perProviderTimeout = 5 * time.Second

	shutdownOne := func(name string, fn func(context.Context) error) error {
		c, cancel := context.WithTimeout(ctx, perProviderTimeout)
		defer cancel()
		if err := fn(c); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	}

	var errs []error
	if p.Tracer != nil {
		if err := shutdownOne("tracer provider", p.Tracer.Shutdown); err != nil {
			errs = append(errs, err)
		}
	}
	if p.Meter != nil {
		if err := shutdownOne("meter provider", p.Meter.Shutdown); err != nil {
			errs = append(errs, err)
		}
	}
	if p.Logger != nil {
		if err := shutdownOne("logger provider", p.Logger.Shutdown); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
