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

// migrationFiles: location.ride_summary/outbox_message carry no cross-schema
// FKs (LOCATION_SPEC.md §6.1 — "cross-schema FK is optional, follow 0005's
// precedent"), so only the uuid-ossp extension and the location schema
// itself are needed here.
var migrationFiles = []string{
	"../../../shared/migrations/sql/0001_extensions.up.sql",
	"../../../shared/migrations/sql/0010_location.up.sql",
}

var seedSeq atomic.Int64

func nextSeq() int64 {
	return seedSeq.Add(1)
}

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

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

func nextRideID() string {
	return fmt.Sprintf("11111111-1111-1111-1111-%012d", nextSeq())
}
