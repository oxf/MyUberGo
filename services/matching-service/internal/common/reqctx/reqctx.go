// Package reqctx propagates a per-request/per-message correlation ID through
// context.Context so log lines across a single logical operation's
// command/query decorators can be tied together, mirroring driver-service's
// internal/common/reqctx (kept for parity even though nothing currently
// populates a request ID here — matching-service is Kafka-consumer-driven,
// not HTTP, and has no middleware layer yet).
package reqctx

import "context"

type ctxKey struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func RequestID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ctxKey{}).(string)
	return id, ok
}
