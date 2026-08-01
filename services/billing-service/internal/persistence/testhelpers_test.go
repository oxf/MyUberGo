package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var testDB *sql.DB

var seedSeq atomic.Int64

func nextSeq() int64 {
	return seedSeq.Add(1)
}

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests spins up one ephemeral Postgres container for the whole package's
// test run, migrated with the real services/shared/migrations/init.sql (not
// a hand-extracted subset), so FK-dependent fixtures (ride.ride, auth.client,
// driver.driver) behave like production.
func runTests(m *testing.M) int {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:15",
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
		if err := ctr.Terminate(ctx); err != nil {
			log.Printf("terminate postgres container: %v", err)
		}
	}()

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
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

	return m.Run()
}

// seedClient inserts an auth.user (role=Client) + auth.client row and
// returns the client id, satisfying billing.customer/payment_method/
// invoice.client_id's FK.
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
// returns the driver id, satisfying ride.ride.driver_id/billing.invoice.
// driver_id's FK.
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

// seedRide inserts a ride.ride row directly, satisfying billing.invoice.
// ride_id's FK. driverID may be empty (nil column).
func seedRide(t *testing.T, db *sql.DB, clientID, driverID string) string {
	t.Helper()

	var driverArg any
	if driverID != "" {
		driverArg = driverID
	}

	var id string
	if err := db.QueryRow(`
		INSERT INTO ride.ride
			(client_id, driver_id, status, pickup_lat, pickup_lng, pickup_address, dest_lat, dest_lng, dest_address, estimated_price_minor, currency, estimated_distance_km)
		VALUES ($1, $2, 'Completed', 10.0, 10.0, 'Pickup', 20.0, 20.0, 'Dest', 1000, 'EUR', 5.0)
		RETURNING id
	`, clientID, driverArg).Scan(&id); err != nil {
		t.Fatalf("seed ride.ride: %v", err)
	}

	return id
}
