package decorator

import (
	"context"
	"fmt"
	"strings"

	"github.com/oxf/MyUber/observability/obsdecorator"
	"github.com/sirupsen/logrus"
)

// ApplyCommandDecorators wraps a command handler that returns a result of type R
// with logging, metrics and tracing decorators and returns the decorated handler.
func ApplyCommandDecorators[H any, R any](handler CommandHandler[H, R], logger *logrus.Entry, metricsClient MetricsClient) CommandHandler[H, R] {
	return commandLoggingDecorator[H, R]{
		base: commandMetricsDecorator[H, R]{
			base:   obsdecorator.TraceCommand[H, R](handler),
			client: metricsClient,
		},
		logger: logger,
	}
}

// ApplyCommandDecoratorsNoResult wraps a command handler that returns only an error
// with logging, metrics and tracing decorators and returns the decorated handler.
func ApplyCommandDecoratorsNoResult[C any](handler CommandHandlerNoResult[C], logger *logrus.Entry, metricsClient MetricsClient) CommandHandlerNoResult[C] {
	return commandLoggingDecoratorNoResult[C]{
		base: commandMetricsDecoratorNoResult[C]{
			base:   obsdecorator.TraceCommandNoResult[C](handler),
			client: metricsClient,
		},
		logger: logger,
	}
}

// CommandHandler is a generic command interface that returns a result of type R.
type CommandHandler[C any, R any] interface {
	Handle(ctx context.Context, cmd C) (R, error)
}

// CommandHandlerNoResult is a generic command interface that returns only an error.
type CommandHandlerNoResult[C any] interface {
	Handle(ctx context.Context, cmd C) error
}

func generateActionName(handler any) string {
	return strings.Split(fmt.Sprintf("%T", handler), ".")[1]
}
