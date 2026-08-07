package dbexec

import (
	"context"
	"database/sql"
)

type PostgresTransactionManager struct {
	db *sql.DB
}

func NewPostgresTransactionManager(
	db *sql.DB,
) *PostgresTransactionManager {

	return &PostgresTransactionManager{
		db: db,
	}
}

func (m *PostgresTransactionManager) WithinTransaction(
	ctx context.Context,
	fn func(context.Context) error,
) error {

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	ctx = WithTx(ctx, tx)

	if err := fn(ctx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}
