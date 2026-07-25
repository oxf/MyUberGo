package decorator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	commonerrors "auth-service/internal/common/errors"
)

type MetricsClient interface {
	Inc(key string, value int)
}

func recordOutcome(client MetricsClient, prefix string, err error) {
	switch {
	case err == nil:
		client.Inc(prefix+".success", 1)
	case errors.Is(err, commonerrors.ErrNotFound):
		client.Inc(prefix+".notfound", 1)
	default:
		client.Inc(prefix+".failure", 1)
	}
}

type commandMetricsDecorator[C any, R any] struct {
	base   CommandHandler[C, R]
	client MetricsClient
}

func (d commandMetricsDecorator[C, R]) Handle(ctx context.Context, cmd C) (result R, err error) {
	start := time.Now()

	actionName := strings.ToLower(generateActionName(cmd))

	defer func() {
		d.client.Inc(fmt.Sprintf("commands.%s.duration_ms", actionName), int(time.Since(start).Milliseconds()))
		recordOutcome(d.client, fmt.Sprintf("commands.%s", actionName), err)
	}()

	return d.base.Handle(ctx, cmd)
}

type commandMetricsDecoratorNoResult[C any] struct {
	base   CommandHandlerNoResult[C]
	client MetricsClient
}

func (d commandMetricsDecoratorNoResult[C]) Handle(ctx context.Context, cmd C) (err error) {
	start := time.Now()

	actionName := strings.ToLower(generateActionName(cmd))

	defer func() {
		d.client.Inc(fmt.Sprintf("commands.%s.duration_ms", actionName), int(time.Since(start).Milliseconds()))
		recordOutcome(d.client, fmt.Sprintf("commands.%s", actionName), err)
	}()

	return d.base.Handle(ctx, cmd)
}

type queryMetricsDecorator[C any, R any] struct {
	base   QueryHandler[C, R]
	client MetricsClient
}

func (d queryMetricsDecorator[C, R]) Handle(ctx context.Context, query C) (result R, err error) {
	start := time.Now()

	actionName := strings.ToLower(generateActionName(query))

	defer func() {
		d.client.Inc(fmt.Sprintf("queries.%s.duration_ms", actionName), int(time.Since(start).Milliseconds()))
		recordOutcome(d.client, fmt.Sprintf("queries.%s", actionName), err)
	}()

	return d.base.Handle(ctx, query)
}
