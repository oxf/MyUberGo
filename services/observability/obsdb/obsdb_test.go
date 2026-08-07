package obsdb

import (
	"context"
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

func TestSuppressTracing(t *testing.T) {
	if isTracingSuppressed(context.Background()) {
		t.Error("an ordinary context must not be suppressed")
	}
	suppressed := SuppressTracing(context.Background())
	if !isTracingSuppressed(suppressed) {
		t.Error("expected SuppressTracing to mark the context")
	}
}

func TestDBSystemAttr(t *testing.T) {
	if got := dbSystemAttr("postgresql"); got != semconv.DBSystemNamePostgreSQL {
		t.Errorf("expected the typed PostgreSQL constant, got %+v", got)
	}
	if got := dbSystemAttr("mysql"); got.Value.AsString() != "mysql" {
		t.Errorf("expected fallback to the raw string for an unrecognized db.system, got %+v", got)
	}
	if got := dbSystemAttr("mysql"); got.Key != semconv.DBSystemNameKey {
		t.Errorf("expected fallback to still use the correct semconv key, got %+v", got.Key)
	}
}
