// Package reqctx propagates a per-request correlation ID through
// context.Context so log lines from the HTTP handler, the command/query
// decorators, and (via the outbox_id already logged by the outbox worker)
// later async processing can be tied back to the request that caused them.
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
