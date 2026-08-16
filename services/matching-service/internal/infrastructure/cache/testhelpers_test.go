package cache

import (
	"context"
	"log"
	"os"
	"testing"

	redisgo "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

var testRedis *redisgo.Client

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests spins up one ephemeral Redis container for the package — Kafka-driven paths are already
// covered by internal/consumers; this verifies OnlineRatings' ZMSCORE against a real Redis.
func runTests(m *testing.M) int {
	ctx := context.Background()

	redisCtr, err := redis.Run(ctx, "redis:7")
	if err != nil {
		log.Fatalf("start redis container: %v", err)
	}
	defer func() {
		if err := redisCtr.Terminate(ctx); err != nil {
			log.Printf("terminate redis container: %v", err)
		}
	}()

	connStr, err := redisCtr.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("redis connection string: %v", err)
	}
	redisOpts, err := redisgo.ParseURL(connStr)
	if err != nil {
		log.Fatalf("parse redis url: %v", err)
	}
	testRedis = redisgo.NewClient(redisOpts)
	defer testRedis.Close()

	return m.Run()
}
