package persistence

import (
	"context"
	"database/sql"

	"github.com/oxf/MyUber/common/dbexec"
)

type DBExecutor = dbexec.DBExecutor

type PostgresTransactionManager = dbexec.PostgresTransactionManager

func WithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return dbexec.WithTx(ctx, tx)
}

func Executor(ctx context.Context, db *sql.DB) DBExecutor {
	return dbexec.Executor(ctx, db)
}

func NewPostgresTransactionManager(db *sql.DB) *PostgresTransactionManager {
	return dbexec.NewPostgresTransactionManager(db)
}
