package health

import (
	"context"
	"time"

	commonhealth "github.com/oxf/MyUber/common/health"
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

// NewChecker creates a new health checker
func NewChecker(rdb *redis.Client, checkInterval time.Duration) *Checker {
	return commonhealth.NewChecker(redisPinger{rdb: rdb}, checkInterval)
}

var GoSafe = commonhealth.GoSafe

var HealthcheckSelf = commonhealth.HealthcheckSelf
