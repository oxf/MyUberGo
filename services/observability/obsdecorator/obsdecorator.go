// Package obsdecorator wraps command/query handlers with tracing spans.
//
// It declares its own CommandHandler/QueryHandler interfaces (separate Go modules per service) that Go's structural typing
// makes freely assignable to/from a service's local decorator types — a small addition inside ApplyCommandDecorators, not a rewrite:
//
//	base: obsdecorator.TraceCommand[H, R](
//		commandMetricsDecorator[H, R]{base: handler, client: metricsClient},
//		isExpectedErr, // e.g. func(err error) bool { return errors.Is(err, commonerrors.ErrNotFound) }
//	),
//
// Tracing wraps metrics (not the other way around) so RecordDuration's ctx carries the command's own span, not whatever
// span was active before it started. See services/*/internal/common/decorator/command.go.
package obsdecorator

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("myubergo/decorator")

type CommandHandler[C any, R any] interface {
	Handle(ctx context.Context, cmd C) (R, error)
}

type CommandHandlerNoResult[C any] interface {
	Handle(ctx context.Context, cmd C) error
}

type QueryHandler[Q any, R any] interface {
	Handle(ctx context.Context, q Q) (R, error)
}

// ExpectedErrorFunc classifies err as an expected business outcome (not found, validation, idempotent no-op) rather than a defect.
// It's still recorded via RecordError but doesn't flip span status to Error, avoiding inflated error-rate/SLO dashboards. Pass nil to treat every error as a failure.
type ExpectedErrorFunc func(err error) bool

// ActionName returns the package-unqualified name from %T of a command/query value (e.g. ride.RequestRide -> "RequestRide"),
// replacing six duplicated, panic-prone copies across services (see PLAN.md). Falls back to the full %T string rather than panicking on an unqualified type name.
func ActionName(v any) string {
	full := fmt.Sprintf("%T", v)
	if i := strings.LastIndex(full, "."); i >= 0 && i+1 < len(full) {
		return full[i+1:]
	}
	return full
}

// finish ends span, recording err on it. isExpected (if non-nil) decides whether err also flips the span's status to
// codes.Error — see ExpectedErrorFunc.
func finish(span trace.Span, err error, isExpected ExpectedErrorFunc) {
	defer span.End()
	if err == nil {
		return
	}
	span.RecordError(err)
	if isExpected != nil && isExpected(err) {
		return
	}
	span.SetStatus(codes.Error, err.Error())
}

// recoverAndReraise, deferred first (so it runs last, after finish), marks a panicking handler's span as an error, ends
// it, then re-panics so the goroutine's own recover (this repo's goSafe) still handles it — otherwise the span for the exact failure most worth inspecting is left unended and the SDK silently drops it.
func recoverAndReraise(span trace.Span) {
	if r := recover(); r != nil {
		span.RecordError(fmt.Errorf("panic: %v", r))
		span.SetStatus(codes.Error, "panic")
		span.End()
		panic(r)
	}
}

type tracedCommand[C any, R any] struct {
	base       CommandHandler[C, R]
	isExpected ExpectedErrorFunc
}

func (d tracedCommand[C, R]) Handle(ctx context.Context, cmd C) (result R, err error) {
	name := ActionName(cmd)
	ctx, span := tracer.Start(ctx, "command "+name,
		trace.WithAttributes(attribute.String("command.name", name)))
	// Order matters: deferred calls run LIFO, so recoverAndReraise (deferred second) runs FIRST on a panic, recording
	// the error and ending the span before finish's own (then-no-op) span.End() runs — verified empirically as required, not just convention.
	defer func() { finish(span, err, d.isExpected) }()
	defer recoverAndReraise(span)

	result, err = d.base.Handle(ctx, cmd)
	return result, err
}

// TraceCommand wraps a command handler with a span named "command <CommandName>". isExpected classifies which errors
// are business-as-usual rather than failures for span status — pass nil to mark every error as a failure.
func TraceCommand[C any, R any](handler CommandHandler[C, R], isExpected ExpectedErrorFunc) CommandHandler[C, R] {
	return tracedCommand[C, R]{base: handler, isExpected: isExpected}
}

type tracedCommandNoResult[C any] struct {
	base       CommandHandlerNoResult[C]
	isExpected ExpectedErrorFunc
}

func (d tracedCommandNoResult[C]) Handle(ctx context.Context, cmd C) (err error) {
	name := ActionName(cmd)
	ctx, span := tracer.Start(ctx, "command "+name,
		trace.WithAttributes(attribute.String("command.name", name)))
	// Order matters: deferred calls run LIFO, so recoverAndReraise (deferred second) runs FIRST on a panic, recording
	// the error and ending the span before finish's own (then-no-op) span.End() runs — verified empirically as required, not just convention.
	defer func() { finish(span, err, d.isExpected) }()
	defer recoverAndReraise(span)

	err = d.base.Handle(ctx, cmd)
	return err
}

// TraceCommandNoResult is TraceCommand for handlers with no result value.
func TraceCommandNoResult[C any](handler CommandHandlerNoResult[C], isExpected ExpectedErrorFunc) CommandHandlerNoResult[C] {
	return tracedCommandNoResult[C]{base: handler, isExpected: isExpected}
}

type tracedQuery[Q any, R any] struct {
	base       QueryHandler[Q, R]
	isExpected ExpectedErrorFunc
}

func (d tracedQuery[Q, R]) Handle(ctx context.Context, q Q) (result R, err error) {
	name := ActionName(q)
	ctx, span := tracer.Start(ctx, "query "+name,
		trace.WithAttributes(attribute.String("query.name", name)))
	// Order matters: deferred calls run LIFO, so recoverAndReraise (deferred second) runs FIRST on a panic, recording
	// the error and ending the span before finish's own (then-no-op) span.End() runs — verified empirically as required, not just convention.
	defer func() { finish(span, err, d.isExpected) }()
	defer recoverAndReraise(span)

	result, err = d.base.Handle(ctx, q)
	return result, err
}

// TraceQuery wraps a query handler with a span named "query <QueryName>".
func TraceQuery[Q any, R any](handler QueryHandler[Q, R], isExpected ExpectedErrorFunc) QueryHandler[Q, R] {
	return tracedQuery[Q, R]{base: handler, isExpected: isExpected}
}
