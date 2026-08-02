// Package obsdecorator wraps command/query handlers with tracing spans.
//
// It defines its own CommandHandler/QueryHandler interfaces rather than
// importing any service's decorator package — each service is its own Go
// module, so that isn't even possible directly. Go's structural interface
// typing means a value of a service's local decorator.CommandHandler[C,R]
// is directly assignable to this package's CommandHandler[C,R] (identical
// method sets), and the value this package returns is directly assignable
// back — so wiring this in is a one-line addition inside each service's
// ApplyCommandDecorators, not a rewrite of every command/query file:
//
//	base: commandMetricsDecorator[H, R]{
//		base:   obsdecorator.TraceCommand[H, R](handler), // added
//		client: metricsClient,
//	},
//
// See services/*/internal/common/decorator/command.go.
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

// actionName mirrors each service's private decorator.generateActionName:
// %T of the command/query value, package-qualified, split on ".". Every
// command/query in this repo is a package-level struct, so the index into
// the split never panics.
func actionName(v any) string {
	return strings.Split(fmt.Sprintf("%T", v), ".")[1]
}

func finish(span trace.Span, err error) {
	defer span.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

type tracedCommand[C any, R any] struct {
	base CommandHandler[C, R]
}

func (d tracedCommand[C, R]) Handle(ctx context.Context, cmd C) (R, error) {
	name := actionName(cmd)
	ctx, span := tracer.Start(ctx, "command "+name,
		trace.WithAttributes(attribute.String("command.name", name)))
	result, err := d.base.Handle(ctx, cmd)
	finish(span, err)
	return result, err
}

// TraceCommand wraps a command handler with a span named "command
// <CommandName>".
func TraceCommand[C any, R any](handler CommandHandler[C, R]) CommandHandler[C, R] {
	return tracedCommand[C, R]{base: handler}
}

type tracedCommandNoResult[C any] struct {
	base CommandHandlerNoResult[C]
}

func (d tracedCommandNoResult[C]) Handle(ctx context.Context, cmd C) error {
	name := actionName(cmd)
	ctx, span := tracer.Start(ctx, "command "+name,
		trace.WithAttributes(attribute.String("command.name", name)))
	err := d.base.Handle(ctx, cmd)
	finish(span, err)
	return err
}

// TraceCommandNoResult is TraceCommand for handlers with no result value.
func TraceCommandNoResult[C any](handler CommandHandlerNoResult[C]) CommandHandlerNoResult[C] {
	return tracedCommandNoResult[C]{base: handler}
}

type tracedQuery[Q any, R any] struct {
	base QueryHandler[Q, R]
}

func (d tracedQuery[Q, R]) Handle(ctx context.Context, q Q) (R, error) {
	name := actionName(q)
	ctx, span := tracer.Start(ctx, "query "+name,
		trace.WithAttributes(attribute.String("query.name", name)))
	result, err := d.base.Handle(ctx, q)
	finish(span, err)
	return result, err
}

// TraceQuery wraps a query handler with a span named "query <QueryName>".
func TraceQuery[Q any, R any](handler QueryHandler[Q, R]) QueryHandler[Q, R] {
	return tracedQuery[Q, R]{base: handler}
}
