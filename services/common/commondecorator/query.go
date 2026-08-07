package commondecorator

import (
	"context"

	"github.com/oxf/MyUber/observability/obsdecorator"
	"github.com/sirupsen/logrus"
)

// QueryHandler is a generic query interface that returns a result of type R.
type QueryHandler[Q any, R any] interface {
	Handle(ctx context.Context, q Q) (R, error)
}

func ApplyQueryDecorators[H any, R any](
	handler QueryHandler[H, R],
	logger *logrus.Entry,
	metricsClient MetricsClient,
	isExpectedError func(err error) bool,
	isNotFound func(err error) bool,
) QueryHandler[H, R] {
	return queryLoggingDecorator[H, R]{
		base: obsdecorator.TraceQuery(
			queryMetricsDecorator[H, R]{
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
