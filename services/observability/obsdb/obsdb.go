// Package obsdb instruments database/sql access with OTel spans and pool metrics.
// Wrapping each service's one sql.Open call instruments every repository's queries without touching repository code.
package obsdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

type suppressTracingKey struct{}

// SuppressTracing marks ctx so a query through Open's *sql.DB produces no span — for health-check pings, since the pool's
// Connect call is spanned by default even though Ping itself isn't, which otherwise churns out orphan root traces.
func SuppressTracing(ctx context.Context) context.Context {
	return context.WithValue(ctx, suppressTracingKey{}, true)
}

func isTracingSuppressed(ctx context.Context) bool {
	v, _ := ctx.Value(suppressTracingKey{}).(bool)
	return v
}

// dbSystemAttr resolves dbSystem to the typed semconv constant when recognized, falling back to the raw string otherwise.
// otelsql is pinned to an older semconv version, so the typed constant avoids drift while the fallback stays forward-compatible.
func dbSystemAttr(dbSystem string) attribute.KeyValue {
	switch dbSystem {
	case "postgresql":
		return semconv.DBSystemNamePostgreSQL
	default:
		return semconv.DBSystemNameKey.String(dbSystem)
	}
}

// Open wraps sql.Open with span + pool-stats instrumentation tagged with a db.system.name attribute (dbSystem, e.g. "postgresql").
// service.name is deliberately omitted here — it's already a resource-level attribute from otelinit.Setup; repeating it risks colliding backend labels.
func Open(driverName, dsn, dbSystem string) (*sql.DB, error) {
	attrs := []attribute.KeyValue{dbSystemAttr(dbSystem)}

	db, err := otelsql.Open(driverName, dsn,
		otelsql.WithAttributes(attrs...),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			// driver.ErrSkip is database/sql control flow (an optional fast path declined), not a failure — without this,
			// otelsql records it as a span error and inflates error-rate dashboards with perfectly healthy queries.
			DisableErrSkip: true,
			// Left enabled: every query here is parameterized, so db.query.text on spans is a debugging asset, not a leak risk.
			DisableQuery: false,
			SpanFilter: func(ctx context.Context, _ otelsql.Method, _ string, _ []driver.NamedValue) bool {
				return !isTracingSuppressed(ctx)
			},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("obsdb: open %s: %w", driverName, err)
	}

	if _, err := otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(attrs...)); err != nil {
		return nil, fmt.Errorf("obsdb: register pool metrics: %w", err)
	}

	return db, nil
}
