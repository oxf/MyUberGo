// Package otelinit bootstraps the OpenTelemetry SDK (traces, metrics, logs)
// and installs it as the process-wide global providers, reading the
// standard OTEL_* environment variables (OTEL_SERVICE_NAME,
// OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL,
// OTEL_RESOURCE_ATTRIBUTES, OTEL_TRACES_SAMPLER, OTEL_SDK_DISABLED, ...) via
// the SDK's own env support rather than inventing service-specific config.
package otelinit

import (
	"context"
	"errors"
	"fmt"

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

// Providers holds the three SDK providers configured by Setup, plus the
// means to flush and close them together on shutdown.
type Providers struct {
	Tracer *sdktrace.TracerProvider
	Meter  *sdkmetric.MeterProvider
	Logger *sdklog.LoggerProvider
}

// Setup builds gRPC OTLP exporters for traces/metrics/logs and installs the
// resulting providers as the global otel.TracerProvider/MeterProvider and
// the global log provider (via go.opentelemetry.io/otel/log/global), and
// starts the Go runtime metrics reader (goroutines, heap, GC pause).
//
// serviceName is only a fallback for the resource's service.name attribute
// when OTEL_SERVICE_NAME is unset — every service in this repo sets that
// env var explicitly (see docker-compose.yml), so this mainly matters for
// local `go run` without it.
//
// Exporter construction here does not dial the Collector — gRPC exporters
// connect lazily and retry in the background, so a Collector that is down
// or not yet started degrades to dropped telemetry, never a failed boot.
func Setup(ctx context.Context, serviceName string) (*Providers, error) {
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build resource: %w", err)
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

	metricExporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("observability: build metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	logExporter, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("observability: build log exporter: %w", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	logglobal.SetLoggerProvider(lp)

	if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
		return nil, fmt.Errorf("observability: start runtime metrics: %w", err)
	}

	return &Providers{Tracer: tp, Meter: mp, Logger: lp}, nil
}

// Shutdown flushes and closes all three providers. Register it with each
// service's shutdown.Manager.OnStop so it runs during graceful shutdown,
// after the HTTP server stops accepting new requests but before the process
// exits — otherwise in-flight batched telemetry is dropped on exit.
func (p *Providers) Shutdown(ctx context.Context) error {
	var errs []error
	if p.Tracer != nil {
		if err := p.Tracer.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("tracer provider: %w", err))
		}
	}
	if p.Meter != nil {
		if err := p.Meter.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("meter provider: %w", err))
		}
	}
	if p.Logger != nil {
		if err := p.Logger.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger provider: %w", err))
		}
	}
	return errors.Join(errs...)
}
