package health

import (
	"context"
	"database/sql"
	"time"

	commonhealth "github.com/oxf/MyUber/common/health"
	"github.com/oxf/MyUber/observability/obsdb"
)

type Checker = commonhealth.Checker

type State = commonhealth.State

// postgresPinger adapts *sql.DB to commonhealth.Pinger. SuppressTracing
// marks the context so the periodic health-check ping never produces a
// stray span — wrapping it here rather than in the outer context is
// behaviorally identical, since it's a context value.
type postgresPinger struct{ db *sql.DB }

func (p postgresPinger) Ping(ctx context.Context) error {
	return p.db.PingContext(obsdb.SuppressTracing(ctx))
}

// NewChecker creates a new health checker
func NewChecker(db *sql.DB, checkInterval time.Duration) *Checker {
	return commonhealth.NewChecker(postgresPinger{db: db}, checkInterval)
}

var GoSafe = commonhealth.GoSafe

var HealthcheckSelf = commonhealth.HealthcheckSelf
