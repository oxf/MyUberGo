package commondecorator

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// MetricsClient's methods take ctx (so the SDK can attach exemplars linking
// a measurement to the currently active span) and variadic attributes
// (so e.g. commands.*.{success,failure,duration_ms} collapses from a
// cartesian product of key strings into one histogram sliceable by
// command.name/outcome).
type MetricsClient interface {
	IncCounter(ctx context.Context, name string, attrs ...attribute.KeyValue)
	RecordDuration(ctx context.Context, name string, d time.Duration, attrs ...attribute.KeyValue)
	RecordValue(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue)
}

func outcomeOf(err error, isNotFound func(error) bool) string {
	switch {
	case err == nil:
		return "success"
	case isNotFound != nil && isNotFound(err):
		return "notfound"
	default:
		return "failure"
	}
}

type commandMetricsDecorator[C any, R any] struct {
	base       CommandHandler[C, R]
	client     MetricsClient
	isNotFound func(error) bool
}

func (d commandMetricsDecorator[C, R]) Handle(ctx context.Context, cmd C) (result R, err error) {
	start := time.Now()

	actionName := ActionName(cmd)

	defer func() {
		d.client.RecordDuration(ctx, "myubergo.command.duration", time.Since(start),
			attribute.String("command.name", actionName),
			attribute.String("outcome", outcomeOf(err, d.isNotFound)),
		)
	}()

	return d.base.Handle(ctx, cmd)
}

type commandMetricsDecoratorNoResult[C any] struct {
	base       CommandHandlerNoResult[C]
	client     MetricsClient
	isNotFound func(error) bool
}

func (d commandMetricsDecoratorNoResult[C]) Handle(ctx context.Context, cmd C) (err error) {
	start := time.Now()

	actionName := ActionName(cmd)

	defer func() {
		d.client.RecordDuration(ctx, "myubergo.command.duration", time.Since(start),
			attribute.String("command.name", actionName),
			attribute.String("outcome", outcomeOf(err, d.isNotFound)),
		)
	}()

	return d.base.Handle(ctx, cmd)
}

type queryMetricsDecorator[C any, R any] struct {
	base       QueryHandler[C, R]
	client     MetricsClient
	isNotFound func(error) bool
}

func (d queryMetricsDecorator[C, R]) Handle(ctx context.Context, query C) (result R, err error) {
	start := time.Now()

	actionName := ActionName(query)

	defer func() {
		d.client.RecordDuration(ctx, "myubergo.query.duration", time.Since(start),
			attribute.String("query.name", actionName),
			attribute.String("outcome", outcomeOf(err, d.isNotFound)),
		)
	}()

	return d.base.Handle(ctx, query)
}
