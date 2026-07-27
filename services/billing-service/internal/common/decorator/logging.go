package decorator

import (
	"billing-service/internal/common/reqctx"
	"context"
	"errors"
	"reflect"

	commonerrors "billing-service/internal/common/errors"

	"github.com/sirupsen/logrus"
)

// loggerFromContext attaches the per-request correlation ID (if any) to the
// base logger, so a single HTTP request's handler/command/query log lines
// can be grepped together.
func loggerFromContext(ctx context.Context, base *logrus.Entry) *logrus.Entry {
	if id, ok := reqctx.RequestID(ctx); ok {
		return base.WithField("request_id", id)
	}
	return base
}

func logOutcome(logger *logrus.Entry, err error, successMsg, notFoundMsg, failureMsg string) {
	switch {
	case err == nil:
		logger.Info(successMsg)
	case errors.Is(err, commonerrors.ErrNotFound):
		logger.Warn(notFoundMsg)
	default:
		logger.WithError(err).Error(failureMsg)
	}
}

// idField extracts a struct's "ID" field for log correlation, if it has one,
// instead of dumping the whole command/query body (every field, on every
// call, at Info level) as the previous %#v-based logging did.
func idField(cmd any) (string, bool) {
	v := reflect.ValueOf(cmd)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return "", false
	}
	f := v.FieldByName("ID")
	if !f.IsValid() || f.Kind() != reflect.String {
		return "", false
	}
	return f.String(), true
}

type commandLoggingDecorator[C any, R any] struct {
	base   CommandHandler[C, R]
	logger *logrus.Entry
}

func (d commandLoggingDecorator[C, R]) Handle(ctx context.Context, cmd C) (result R, err error) {
	fields := logrus.Fields{"command": generateActionName(cmd)}
	if id, ok := idField(cmd); ok {
		fields["command_id"] = id
	}
	logger := loggerFromContext(ctx, d.logger).WithFields(fields)

	logger.Debug("Executing command")
	defer func() {
		logOutcome(logger, err,
			"Command executed successfully",
			"Command no-op: target not found",
			"Failed to execute command",
		)
	}()

	return d.base.Handle(ctx, cmd)
}

type commandLoggingDecoratorNoResult[C any] struct {
	base   CommandHandlerNoResult[C]
	logger *logrus.Entry
}

func (d commandLoggingDecoratorNoResult[C]) Handle(ctx context.Context, cmd C) (err error) {
	fields := logrus.Fields{"command": generateActionName(cmd)}
	if id, ok := idField(cmd); ok {
		fields["command_id"] = id
	}
	logger := loggerFromContext(ctx, d.logger).WithFields(fields)

	logger.Debug("Executing command")
	defer func() {
		logOutcome(logger, err,
			"Command executed successfully",
			"Command no-op: target not found",
			"Failed to execute command",
		)
	}()

	return d.base.Handle(ctx, cmd)
}

type queryLoggingDecorator[C any, R any] struct {
	base   QueryHandler[C, R]
	logger *logrus.Entry
}

func (d queryLoggingDecorator[C, R]) Handle(ctx context.Context, cmd C) (result R, err error) {
	fields := logrus.Fields{"query": generateActionName(cmd)}
	if id, ok := idField(cmd); ok {
		fields["query_id"] = id
	}
	logger := loggerFromContext(ctx, d.logger).WithFields(fields)

	logger.Debug("Executing query")
	defer func() {
		logOutcome(logger, err,
			"Query executed successfully",
			"Query no-op: target not found",
			"Failed to execute query",
		)
	}()

	return d.base.Handle(ctx, cmd)
}
