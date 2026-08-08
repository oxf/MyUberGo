package consumers

import (
	"context"
	"database/sql"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"driver-service/internal/infrastructure/metrics"
	"driver-service/internal/persistence"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	segmentio "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Re-joining the hardcoded GroupID per test was flaky on this single-node KRaft broker
// (slow member reap), so each consumer starts once per package run on one fixed topic.
const (
	rideAcceptedTestTopic  = "ride.accepted.pkgtest"
	rideCompletedTestTopic = "ride.completed.pkgtest"
	rideCancelledTestTopic = "ride.cancelled.pkgtest"
)

// migrationFiles lists the split migration files in apply order — WithInitScripts alone
// would apply them in sorted-name order, which is wrong once numbering exceeds one digit.
var migrationFiles = []string{
	"../../../shared/migrations/sql/0001_extensions.up.sql",
	"../../../shared/migrations/sql/0002_auth.up.sql",
	"../../../shared/migrations/sql/0003_ride.up.sql",
	"../../../shared/migrations/sql/0004_driver.up.sql",
	"../../../shared/migrations/sql/0005_ride_driver_fk.up.sql",
	"../../../shared/migrations/sql/0006_billing.up.sql",
}

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

var (
	testDB      *sql.DB
	kafkaBroker string
)

var seedSeq atomic.Int64

func nextSeq() int64 {
	return seedSeq.Add(1)
}

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests spins up one ephemeral Postgres (via the real init.sql) and one ephemeral Kafka
// container for the whole run, so tests exercise a real read-loop, not a faked reader.
func runTests(m *testing.M) int {
	ctx := context.Background()

	pgCtr, err := postgres.Run(ctx, "postgres:15",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithOrderedInitScripts(migrationFiles...),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}
	defer func() {
		if err := pgCtr.Terminate(ctx); err != nil {
			log.Printf("terminate postgres container: %v", err)
		}
	}()

	dsn, err := pgCtr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("connection string: %v", err)
	}

	testDB, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer func() {
		if err := testDB.Close(); err != nil {
			log.Printf("close test db: %v", err)
		}
	}()

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

	for _, topic := range []string{rideAcceptedTestTopic, rideCompletedTestTopic, rideCancelledTestTopic} {
		if err := createTopicNoTest(topic); err != nil {
			log.Fatalf("create topic %s: %v", topic, err)
		}
	}

	profileRepo := persistence.NewPostgresDriverRepository(testDB)
	transactionManager := persistence.NewPostgresTransactionManager(testDB)
	application := app.Application{
		Commands: app.Commands{
			ProcessRideAccepted:  command.NewProcessRideAcceptedHandler(profileRepo, transactionManager, testLogger(), metrics.NewNoopMetricsClient()),
			ProcessRideCompleted: command.NewProcessRideCompletedHandler(profileRepo, transactionManager, testLogger(), metrics.NewNoopMetricsClient()),
			ProcessRideCancelled: command.NewProcessRideCancelledHandler(profileRepo, transactionManager, testLogger(), metrics.NewNoopMetricsClient()),
		},
	}

	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	defer cancelConsumers()

	go NewRideAcceptedConsumer(application, kafkaBroker, testLogger()).Run(consumerCtx, rideAcceptedTestTopic)
	go NewRideCompletedConsumer(application, kafkaBroker, testLogger()).Run(consumerCtx, rideCompletedTestTopic)
	go NewRideCancelledConsumer(application, kafkaBroker, testLogger()).Run(consumerCtx, rideCancelledTestTopic)

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
// (all topics are created upfront in runTests), but a short retry is kept as cheap insurance.
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

// seedDriver inserts an auth.user (role=Driver) + driver.driver row at the
// given status and returns the driver id.
func seedDriver(t *testing.T, db *sql.DB, status string) string {
	t.Helper()

	var userID string
	email := fmt.Sprintf("driver-%d@test.myubergo.local", nextSeq())
	if err := db.QueryRow(
		`INSERT INTO auth.user (email, password_hash, name, role) VALUES ($1, 'x', 'Test Driver', 'Driver') RETURNING id`,
		email,
	).Scan(&userID); err != nil {
		t.Fatalf("seed auth.user (driver): %v", err)
	}

	var driverID string
	if err := db.QueryRow(
		`INSERT INTO driver.driver (user_id, status) VALUES ($1, $2) RETURNING id`,
		userID, status,
	).Scan(&driverID); err != nil {
		t.Fatalf("seed driver.driver: %v", err)
	}

	return driverID
}
