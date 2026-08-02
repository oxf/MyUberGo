package decorator

import (
	"context"

	"github.com/oxf/MyUber/observability/obsdecorator"
	"github.com/sirupsen/logrus"
)

func ApplyQueryDecorators[H any, R any](handler QueryHandler[H, R], logger *logrus.Entry, metricsClient MetricsClient) QueryHandler[H, R] {
	return queryLoggingDecorator[H, R]{
		base: queryMetricsDecorator[H, R]{
			base:   obsdecorator.TraceQuery[H, R](handler),
			client: metricsClient,
		},
		logger: logger,
	}
}

type QueryHandler[Q any, R any] interface {
	Handle(ctx context.Context, q Q) (R, error)
}
