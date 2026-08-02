// Package obsoutbox makes trace context durable across the transactional
// outbox pattern (see ride/driver/billing-service's internal/workers).
//
// Every other hop in this repo propagates trace context through a live
// context.Context. The outbox deliberately breaks that: the HTTP request
// that inserts an outbox row returns long before the background worker
// polls and publishes it, on its own ticker context with no link back. To
// keep one trace spanning "request-ride HTTP call" through "ride.requested
// lands in Kafka", the trace context has to become durable state written in
// the same transaction as the outbox row (a `trace_context` column), then
// rehydrated by the worker before it publishes.
package obsoutbox

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// MarshalTraceContext serializes ctx's trace context (and baggage) into a
// small JSON object for storage alongside an outbox row, in the same
// transaction as the domain write it accompanies. Returns nil if ctx
// carries no active trace context — the resulting outbox row simply has no
// trace_context, handled the same way as one written before this feature
// existed (see UnmarshalTraceContext).
func MarshalTraceContext(ctx context.Context) []byte {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	b, err := json.Marshal(carrier)
	if err != nil {
		return nil
	}
	return b
}

// UnmarshalTraceContext rebuilds a context carrying the trace context
// stored by MarshalTraceContext, rooted at ctx (normally the outbox
// worker's own background context). A nil/empty/unparseable payload returns
// ctx unchanged — an outbox row with no stored trace context (written
// before this feature existed, or whose request carried no active trace)
// simply starts a fresh trace at publish time instead of failing.
func UnmarshalTraceContext(ctx context.Context, data []byte) context.Context {
	if len(data) == 0 {
		return ctx
	}
	var carrier propagation.MapCarrier
	if err := json.Unmarshal(data, &carrier); err != nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
