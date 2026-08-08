package persistence

import (
	"context"
	"database/sql"
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
// a hand-extracted subset).
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
