// Package obsoutbox makes trace context durable across the transactional outbox pattern (see
// ride/driver/billing-service's internal/workers), since the background publisher's ticker context has no live link back to the original request.
package obsoutbox

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// MarshalTraceContext serializes ctx's trace context (and baggage) into a small JSON object for storage
// alongside an outbox row. Returns nil if ctx carries no active trace, same as a row written pre-feature.
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

// UnmarshalTraceContext rebuilds a context carrying the trace stored by MarshalTraceContext, rooted at ctx.
// A nil/empty payload degrades to ctx unchanged silently; a corrupt payload does too, but reported via otel.Handle first.
func UnmarshalTraceContext(ctx context.Context, data []byte) context.Context {
	if len(data) == 0 {
		return ctx
	}
	var carrier propagation.MapCarrier
	if err := json.Unmarshal(data, &carrier); err != nil {
		otel.Handle(fmt.Errorf("obsoutbox: unmarshal stored trace context: %w", err))
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
