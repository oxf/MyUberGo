package health

import (
	"context"
	"database/sql"
	"time"

	commonhealth "github.com/oxf/MyUber/common/health"
	"github.com/oxf/MyUber/observability/obsdb"
	"github.com/redis/go-redis/v9"
)

type Checker = commonhealth.Checker

type State = commonhealth.State

// redisPinger adapts *redis.Client to commonhealth.Pinger. Tracing exclusion
// for this ping happens at the source (main.go's commandFilter), not here.
type redisPinger struct{ rdb *redis.Client }

func (p redisPinger) Ping(ctx context.Context) error {
	return p.rdb.Ping(ctx).Err()
}

// postgresPinger adapts *sql.DB to commonhealth.Pinger — see
// billing-service's own postgresPinger for the SuppressTracing rationale.
type postgresPinger struct{ db *sql.DB }

func (p postgresPinger) Ping(ctx context.Context) error {
	return p.db.PingContext(obsdb.SuppressTracing(ctx))
}

// NewChecker creates a new health checker gated on both Redis and Postgres
// (Slice 3: LOCATION_SPEC.md §0.1's readiness correction — deliberately
// NOT the history store too, since §6.2 insists ingest/archive-tier health
// must never gate readiness; DynamoDB reachability is a metric, not a
// liveness/readiness input).
func NewChecker(rdb *redis.Client, db *sql.DB, checkInterval time.Duration) *Checker {
	return commonhealth.NewChecker(
		commonhealth.MultiPinger(redisPinger{rdb: rdb}, postgresPinger{db: db}),
		checkInterval,
	)
}

var GoSafe = commonhealth.GoSafe

var HealthcheckSelf = commonhealth.HealthcheckSelf
