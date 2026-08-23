// Package redisconn centralizes the Redis bootstrap idiom shared by every
// Redis-backed service's cmd/main.go: URL resolution, pool tuning, and
// OTel tracing/metrics instrumentation (including the PING-exclusion
// command filter).
package redisconn

import (
	"fmt"
	"strings"
	"time"

	"github.com/oxf/MyUber/common/envconfig"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// Open resolves REDIS_URL (falling back to defaultURL) as a redis:// URL,
// applies pool tuning from REDIS_POOL_SIZE/REDIS_MIN_IDLE_CONNS/
// REDIS_CONN_MAX_LIFETIME_MIN (defaults 50/10/5m), and wires OTel tracing +
// metrics instrumentation. Unlike dbconn.Open, pool options are set on the
// *redis.Options before construction — go-redis has no post-construction
// pool setters the way *sql.DB does.
func Open(defaultURL string) (*redis.Client, error) {
	url := envconfig.String("REDIS_URL", defaultURL)

	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redisconn: parse REDIS_URL: %w", err)
	}

	opts.PoolSize = envconfig.Int("REDIS_POOL_SIZE", 50)
	opts.MinIdleConns = envconfig.Int("REDIS_MIN_IDLE_CONNS", 10)
	opts.ConnMaxLifetime = time.Duration(envconfig.Int("REDIS_CONN_MAX_LIFETIME_MIN", 5)) * time.Minute

	client := redis.NewClient(opts)

	if err := redisotel.InstrumentTracing(client, redisotel.WithCommandFilter(commandFilter)); err != nil {
		return nil, err
	}
	if err := redisotel.InstrumentMetrics(client); err != nil {
		return nil, err
	}

	return client, nil
}

// commandFilter extends redisotel's DefaultCommandFilter to also exclude PING (from
// health.Checker's ticker), else every ping emits its own orphan trace in Tempo.
func commandFilter(cmd redis.Cmder) bool {
	if strings.EqualFold(cmd.Name(), "ping") {
		return true
	}
	return redisotel.DefaultCommandFilter(cmd)
}
