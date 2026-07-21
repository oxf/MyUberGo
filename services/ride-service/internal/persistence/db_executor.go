package persistence

import (
	"context"
	"database/sql"
)

type DBExecutor interface {
	ExecContext(
		context.Context,
		string,
		...any,
	) (sql.Result, error)

	QueryContext(
		context.Context,
		string,
		...any,
	) (*sql.Rows, error)

	QueryRowContext(
		context.Context,
		string,
		...any,
	) *sql.Row
}
