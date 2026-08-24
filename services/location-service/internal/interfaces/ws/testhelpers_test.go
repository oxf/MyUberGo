package ws

import (
	"context"
	"io"
	"log"
	"os"
	"testing"
	"time"

	redisgo "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

var testRedisURL string

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests spins up one ephemeral Redis for the whole package run.
// TestHub_CrossInstanceFanOut deliberately opens its own two *redis.Client
// connections against this same server, rather than sharing a package-level
// client, to actually exercise cross-connection Pub/Sub.
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
	testRedisURL = connStr

	return m.Run()
}

func newTestRedisClient(t *testing.T) *redisgo.Client {
	t.Helper()
	opts, err := redisgo.ParseURL(testRedisURL)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	client := redisgo.NewClient(opts)
	t.Cleanup(func() { client.Close() })
	return client
}

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !check() {
		t.Fatal("timed out waiting for condition")
	}
}
