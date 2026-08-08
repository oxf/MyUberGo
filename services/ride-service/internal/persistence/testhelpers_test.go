package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"

	"github.com/oxf/MyUber/common/pgtest"
)

var testDB *sql.DB

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

var seedSeq atomic.Int64

func nextSeq() int64 {
	return seedSeq.Add(1)
}

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests spins up one ephemeral Postgres container for the whole package's
// test run, migrated with the real services/shared/migrations/init.sql (not
// a hand-extracted subset), so FK-dependent fixtures behave like production.
func runTests(m *testing.M) int {
	ctx := context.Background()

	c, err := pgtest.StartContainer(ctx, migrationFiles)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := c.Close(ctx); err != nil {
			log.Printf("close postgres container: %v", err)
		}
	}()

	testDB = c.DB

	return m.Run()
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

// seedRide inserts a ride.ride row directly (bypassing the repository under
// test) at the given status, so tests exercising a specific transition don't
// depend on CreateRide's own correctness.
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
