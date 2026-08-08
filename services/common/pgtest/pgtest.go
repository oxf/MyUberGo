// Package pgtest centralizes the ephemeral-Postgres-container bootstrap
// hand-copied across services' testcontainers-backed test suites. It is a
// regular (non-_test.go) package so it can be imported across module
// boundaries; nothing in any service's shipped binary imports it, so it
// never reaches a production image.
package pgtest

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Container bundles an ephemeral Postgres testcontainer with an open *sql.DB
// against it.
type Container struct {
	DB  *sql.DB
	ctr *postgres.PostgresContainer
}

// StartContainer spins up one Postgres 15 container migrated in order by
// migrationFiles (WithOrderedInitScripts — plain WithInitScripts sorts by
// filename, which breaks past 9 migrations) and opens a *sql.DB against it.
// Caller must defer Close.
func StartContainer(ctx context.Context, migrationFiles []string) (*Container, error) {
	ctr, err := postgres.Run(ctx, "postgres:15",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithOrderedInitScripts(migrationFiles...),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = ctr.Terminate(ctx)
		return nil, fmt.Errorf("connection string: %w", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		_ = ctr.Terminate(ctx)
		return nil, fmt.Errorf("open db: %w", err)
	}

	return &Container{DB: db, ctr: ctr}, nil
}

// Close closes the DB connection then terminates the container, aggregating
// any errors from both.
func (c *Container) Close(ctx context.Context) error {
	var errs []error
	if err := c.DB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close test db: %w", err))
	}
	if err := c.ctr.Terminate(ctx); err != nil {
		errs = append(errs, fmt.Errorf("terminate postgres container: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}
