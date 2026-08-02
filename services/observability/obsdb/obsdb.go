// Package obsdb instruments database/sql access with OTel spans and pool
// metrics. Every Postgres-backed service in this repo (auth, ride, driver,
// billing) opens its DB once via sql.Open("postgres", dsn) in main() and
// threads it through repositories that take a *sql.DB/DBExecutor — wrapping
// that one call site instruments every repository's queries without
// touching any repository file.
package obsdb

import (
	"database/sql"
	"fmt"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// Open wraps sql.Open(driverName, dsn) with span + pool-stats instrumentation,
// tagging every span/metric with a db.system and service.name attribute.
// dbSystem should be an OTel semconv db.system value, e.g. "postgresql".
func Open(driverName, dsn, service, dbSystem string) (*sql.DB, error) {
	attrs := []attribute.KeyValue{
		semconv.DBSystemNameKey.String(dbSystem),
		attribute.String("service.name", service),
	}

	db, err := otelsql.Open(driverName, dsn, otelsql.WithAttributes(attrs...))
	if err != nil {
		return nil, fmt.Errorf("obsdb: open %s: %w", driverName, err)
	}

	if _, err := otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(attrs...)); err != nil {
		return nil, fmt.Errorf("obsdb: register pool metrics: %w", err)
	}

	return db, nil
}
