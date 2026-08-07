package dbexec

import (
	"context"
	"database/sql"
)

type txKey struct{}

func WithTx(
	ctx context.Context,
	tx *sql.Tx,
) context.Context {

	return context.WithValue(
		ctx,
		txKey{},
		tx,
	)
}

func Executor(
	ctx context.Context,
	db *sql.DB,
) DBExecutor {

	tx, ok := ctx.Value(txKey{}).(*sql.Tx)

	if ok {
		return tx
	}

	return db
}
