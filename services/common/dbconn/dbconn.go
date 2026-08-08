// Package dbconn centralizes the Postgres bootstrap idiom shared by every
// Postgres-backed service's cmd/main.go: DSN resolution, a production-DSN
// safety guard, and pool tuning.
package dbconn

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/oxf/MyUber/common/envconfig"
	"github.com/oxf/MyUber/observability/obsdb"
)

// Open resolves PG_DSN (falling back to defaultDSN), refuses to start in
// production with the default DSN still in effect, opens the DB via
// obsdb.Open, and applies pool tuning from DB_MAX_OPEN_CONNS/
// DB_MAX_IDLE_CONNS/DB_CONN_MAX_LIFETIME_MIN (defaults 20/10/5m).
func Open(defaultDSN string) (*sql.DB, error) {
	dsn := envconfig.String("PG_DSN", defaultDSN)
	if os.Getenv("APP_ENV") == "production" && dsn == defaultDSN {
		return nil, fmt.Errorf("refusing to start in production with the default PG_DSN — set a real PG_DSN")
	}

	db, err := obsdb.Open("postgres", dsn, "postgresql")
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(envconfig.Int("DB_MAX_OPEN_CONNS", 20))
	db.SetMaxIdleConns(envconfig.Int("DB_MAX_IDLE_CONNS", 10))
	db.SetConnMaxLifetime(time.Duration(envconfig.Int("DB_CONN_MAX_LIFETIME_MIN", 5)) * time.Minute)

	return db, nil
}
