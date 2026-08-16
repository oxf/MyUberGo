package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	app "location-service/internal/application"
	"location-service/internal/application/command"
	"location-service/internal/infrastructure/cache"
	"location-service/internal/infrastructure/metrics"
	"os"
	"sync/atomic"
	"testing"
	"time"

	redisgo "github.com/redis/go-redis/v9"
	segmentio "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

// Re-joining the hardcoded GroupID per test was flaky (slow member reap), so
// this consumer starts once per package run on one fixed topic.
const shiftUpdatedTestTopic = "shift.updated.pkgtest"

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

var (
	testRedis   *redisgo.Client
	kafkaBroker string
)

var seedSeq atomic.Int64

func nextSeq() int64 {
	return seedSeq.Add(1)
}

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests spins up one ephemeral Redis and one ephemeral Kafka container for the whole
// run, so tests exercise a real read-loop against real Redis, not a faked reader/repo.
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

	kafkaCtr, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0", kafka.WithClusterID("test-cluster"))
	if err != nil {
		log.Fatalf("start kafka container: %v", err)
	}
	defer func() {
		if err := kafkaCtr.Terminate(ctx); err != nil {
			log.Printf("terminate kafka container: %v", err)
		}
	}()

	brokers, err := kafkaCtr.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		log.Fatalf("kafka brokers: %v", err)
	}
	kafkaBroker = brokers[0]

	if err := createTopicNoTest(shiftUpdatedTestTopic); err != nil {
		log.Fatalf("create topic %s: %v", shiftUpdatedTestTopic, err)
	}

	ownerRepo := cache.NewOwnerRepository(testRedis)
	application := app.Application{
		Commands: app.Commands{
			UpsertOwner: command.NewUpsertOwnerHandler(ownerRepo, testLogger(), metrics.NewNoopMetricsClient()),
		},
	}

	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	defer cancelConsumers()

	go NewShiftUpdatedConsumer(application, kafkaBroker, testLogger()).Run(consumerCtx, shiftUpdatedTestTopic)

	return m.Run()
}

// createTopicNoTest is createTopic's TestMain-time counterpart (no
// *testing.T available yet). CreateTopics is idempotent.
func createTopicNoTest(topic string) error {
	conn, err := segmentio.Dial("tcp", kafkaBroker)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.CreateTopics(segmentio.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}

// produce publishes one JSON-encoded event to topic. No topic-creation race is expected
// (the topic is created upfront in runTests), but a short retry is kept as cheap insurance.
func produce(t *testing.T, topic string, event any) {
	t.Helper()

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	writer := &segmentio.Writer{
		Addr:     segmentio.TCP(kafkaBroker),
		Balancer: &segmentio.LeastBytes{},
	}
	defer writer.Close()

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = writer.WriteMessages(ctx, segmentio.Message{Topic: topic, Value: payload})
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("produce message to %s: %v", topic, lastErr)
}

// waitFor polls check until it returns true or timeout elapses, failing the
// test on timeout — used instead of a fixed sleep since consumption is async.
func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !check() {
		t.Fatal("timed out waiting for condition")
	}
}

func nextID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, nextSeq())
}
