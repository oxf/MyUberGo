// Package commondecorator holds the CQRS command/query decorator machinery
// shared by every service (logging, metrics, tracing). Named commondecorator
// rather than decorator so a service's own `package decorator` can import it
// unaliased — mirrors observability's obsdecorator naming for the same reason.
package commondecorator

import (
	"context"

	"github.com/oxf/MyUber/observability/obsdecorator"
	"github.com/sirupsen/logrus"
)

// CommandHandler is a generic command interface that returns a result of type R.
type CommandHandler[C any, R any] interface {
	Handle(ctx context.Context, cmd C) (R, error)
}

// CommandHandlerNoResult is a generic command interface that returns only an error.
type CommandHandlerNoResult[C any] interface {
	Handle(ctx context.Context, cmd C) error
}

// ApplyCommandDecorators wraps a command handler that returns a result of type R
// with logging, metrics and tracing decorators and returns the decorated handler.
//
// isExpectedError classifies known domain errors as expected outcomes rather than
// defects for span status. isNotFound additionally classifies the "not found"
// outcome specifically, for metrics/log-message selection. Both are per-service
// business logic, injected by the caller rather than hardcoded here.
//
// Tracing wraps metrics, not vice versa: RecordDuration's ctx must carry
// the command's own span for its exemplar to link correctly.
func ApplyCommandDecorators[H any, R any](
	handler CommandHandler[H, R],
	logger *logrus.Entry,
	metricsClient MetricsClient,
	isExpectedError func(err error) bool,
	isNotFound func(err error) bool,
) CommandHandler[H, R] {
	return commandLoggingDecorator[H, R]{
		base: obsdecorator.TraceCommand(
			commandMetricsDecorator[H, R]{
				base:       handler,
				client:     metricsClient,
				isNotFound: isNotFound,
			},
			isExpectedError,
		),
		logger:     logger,
		isNotFound: isNotFound,
	}
}

// ApplyCommandDecoratorsNoResult wraps a command handler that returns only an error
// with logging, metrics and tracing decorators and returns the decorated handler.
func ApplyCommandDecoratorsNoResult[C any](
	handler CommandHandlerNoResult[C],
	logger *logrus.Entry,
	metricsClient MetricsClient,
	isExpectedError func(err error) bool,
	isNotFound func(err error) bool,
) CommandHandlerNoResult[C] {
	return commandLoggingDecoratorNoResult[C]{
		base: obsdecorator.TraceCommandNoResult(
			commandMetricsDecoratorNoResult[C]{
				base:       handler,
				client:     metricsClient,
				isNotFound: isNotFound,
			},
			isExpectedError,
		),
		logger:     logger,
		isNotFound: isNotFound,
	}
}

// ActionName delegates to obsdecorator.ActionName.
func ActionName(v any) string {
	return obsdecorator.ActionName(v)
}
