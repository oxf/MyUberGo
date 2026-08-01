package consumers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"
	"ride-service/internal/infrastructure/metrics"
	"ride-service/internal/persistence"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	segmentio "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Each consumer's GroupID is hardcoded in production code (see
// ride_accepted_consumer.go/payment_completed_consumer.go). Re-joining that
// group per test (fresh Run() call each time) was observed to be flaky in
// this single-node KRaft test broker — a leaving member isn't always fully
// reaped before the next one joins, and the next join can then hang for the
// remainder of the group's session timeout. Instead, each consumer is
// started exactly once for the whole package's test run, on one fixed
// topic; individual tests only produce more (uniquely-IDed) events to that
// same topic and assert on their own ride/driver/invoice ids.
const (
	rideAcceptedTestTopic     = "ride.accepted.pkgtest"
	paymentCompletedTestTopic = "payment.completed.pkgtest"
)

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

// runTests spins up one ephemeral Postgres container (migrated with the real
// services/shared/migrations/init.sql, same as internal/persistence's
// harness) and one ephemeral Kafka container for the whole package's test
// run, so consumer tests exercise a real read-loop against a real broker
// rather than a faked reader. It then starts both real consumers once,
// against their fixed test topics, before running any test.
func runTests(m *testing.M) int {
	ctx := context.Background()

	pgCtr, err := postgres.Run(ctx, "postgres:15",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithInitScripts("../../../shared/migrations/init.sql"),
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

	if err := createTopicNoTest(rideAcceptedTestTopic); err != nil {
		log.Fatalf("create topic %s: %v", rideAcceptedTestTopic, err)
	}
	if err := createTopicNoTest(paymentCompletedTestTopic); err != nil {
		log.Fatalf("create topic %s: %v", paymentCompletedTestTopic, err)
	}

	rideRepo := persistence.NewPostgresRideRepository(testDB)
	application := app.Application{
		Commands: app.Commands{
			MarkRideMatched: command.NewMarkRideMatchedHandler(rideRepo, testLogger(), metrics.NewNoopMetricsClient()),
			MarkRideBilled:  command.NewMarkRideBilledHandler(rideRepo, testLogger(), metrics.NewNoopMetricsClient()),
		},
	}

	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	defer cancelConsumers()

	go NewRideAcceptedConsumer(application, kafkaBroker).Run(consumerCtx, rideAcceptedTestTopic)
	go NewPaymentCompletedConsumer(application, kafkaBroker).Run(consumerCtx, paymentCompletedTestTopic)

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

// produce publishes one JSON-encoded event to topic. Both test topics are
// created once in runTests before any consumer joins, so no per-call
// topic-creation/propagation race is expected here — a short retry is kept
// anyway since it's cheap insurance against a transient produce error.
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

// seedClient inserts an auth.user (role=Client) + auth.client row and
// returns the client id, satisfying ride.ride.client_id's FK.
func seedClient(t *testing.T, db *sql.DB) string {
	t.Helper()

	var userID string
	email := fmt.Sprintf("client-%d@test.myubergo.local", nextSeq())
	if err := db.QueryRow(
		`INSERT INTO auth.user (email, password_hash, name, role) VALUES ($1, 'x', 'Test Client', 'Client') RETURNING id`,
		email,
	).Scan(&userID); err != nil {
		t.Fatalf("seed auth.user (client): %v", err)
	}

	var clientID string
	if err := db.QueryRow(
		`INSERT INTO auth.client (user_id) VALUES ($1) RETURNING id`,
		userID,
	).Scan(&clientID); err != nil {
		t.Fatalf("seed auth.client: %v", err)
	}

	return clientID
}

// seedDriver inserts an auth.user (role=Driver) + driver.driver row and
// returns the driver id, satisfying ride.ride.driver_id's FK.
func seedDriver(t *testing.T, db *sql.DB) string {
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
		`INSERT INTO driver.driver (user_id) VALUES ($1) RETURNING id`,
		userID,
	).Scan(&driverID); err != nil {
		t.Fatalf("seed driver.driver: %v", err)
	}

	return driverID
}

// seedRide inserts a ride.ride row directly at the given status, so tests
// exercising a consumer's transition don't depend on CreateRide's own
// correctness.
func seedRide(t *testing.T, db *sql.DB, clientID, status string) string {
	t.Helper()

	var id string
	if err := db.QueryRow(`
		INSERT INTO ride.ride
			(client_id, status, pickup_lat, pickup_lng, pickup_address, dest_lat, dest_lng, dest_address, estimated_price_minor, currency, estimated_distance_km)
		VALUES ($1, $2, 10.0, 10.0, 'Pickup', 20.0, 20.0, 'Dest', 1000, 'EUR', 5.0)
		RETURNING id
	`, clientID, status).Scan(&id); err != nil {
		t.Fatalf("seed ride.ride: %v", err)
	}

	return id
}
