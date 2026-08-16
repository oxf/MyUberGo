// Package decorator wires location-service's own error classification into the
// shared CQRS decorator machinery in github.com/oxf/MyUber/common/commondecorator.
package decorator

import (
	"errors"

	commonerrors "location-service/internal/common/errors"

	"github.com/oxf/MyUber/common/commondecorator"
	"github.com/sirupsen/logrus"
)

type CommandHandler[C any, R any] = commondecorator.CommandHandler[C, R]
type CommandHandlerNoResult[C any] = commondecorator.CommandHandlerNoResult[C]
type QueryHandler[Q any, R any] = commondecorator.QueryHandler[Q, R]
type MetricsClient = commondecorator.MetricsClient

// isExpectedError classifies an unknown caller/malformed query as an expected
// outcome, not a defect, so it doesn't inflate the span-status error rate.
func isExpectedError(err error) bool {
	return errors.Is(err, commonerrors.ErrNotFound) ||
		errors.Is(err, commonerrors.ErrForbidden) ||
		errors.Is(err, commonerrors.ErrInvalidInput)
}

func isNotFound(err error) bool {
	return errors.Is(err, commonerrors.ErrNotFound)
}

// ApplyCommandDecorators wraps a command handler that returns a result of type R
// with logging, metrics and tracing decorators and returns the decorated handler.
func ApplyCommandDecorators[H any, R any](handler CommandHandler[H, R], logger *logrus.Entry, metricsClient MetricsClient) CommandHandler[H, R] {
	return commondecorator.ApplyCommandDecorators(handler, logger, metricsClient, isExpectedError, isNotFound)
}

// ApplyCommandDecoratorsNoResult wraps a command handler that returns only an error
// with logging, metrics and tracing decorators and returns the decorated handler.
func ApplyCommandDecoratorsNoResult[C any](handler CommandHandlerNoResult[C], logger *logrus.Entry, metricsClient MetricsClient) CommandHandlerNoResult[C] {
	return commondecorator.ApplyCommandDecoratorsNoResult(handler, logger, metricsClient, isExpectedError, isNotFound)
}

func ApplyQueryDecorators[H any, R any](handler QueryHandler[H, R], logger *logrus.Entry, metricsClient MetricsClient) QueryHandler[H, R] {
	return commondecorator.ApplyQueryDecorators(handler, logger, metricsClient, isExpectedError, isNotFound)
}
