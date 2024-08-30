package gorundb

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/jswidler/gorun/errors"
	"github.com/jswidler/gorun/logger"
)

type txStmts = func(ctx context.Context, tx *sqlx.Tx) error

func (db *Db) useTx(ctx context.Context, stmts txStmts) (err error) {
	tx := getTx(ctx)
	if tx == nil {
		// The outer most call must start and finish the transaction.

		// Start a tx for performance monitoring
		// var span *apm.Span
		// span, ctx = apm.StartSpan(ctx, "database transaction", "db.postgres.tx")
		// defer span.End()

		// Start the database transaction
		tx, err = db.db.BeginTxx(ctx, nil)
		if err != nil {
			err = errors.Wrap(ErrDatabaseError, errors.WithCause(err))
			return
		}

		// Set the transaction for inner statements to find
		ctx = setTx(ctx, tx)

		// When all the statements are done, finish up
		defer func() {
			if err == nil {
				// If no error, try to commit, new errors are returned
				err = tx.Commit()
				if err != nil {
					err = errors.Wrap(ErrDatabaseError, errors.WithCause(err))
				}
			} else {
				// since there is already an error, rollback and log any new error
				err2 := tx.Rollback()
				if err2 != nil {
					err2 = errors.Wrap(ErrDatabaseError, errors.WithCause(err2))
					logger.Ctx(ctx).Warn().
						Err(err2).
						Msg("db rollback failed")
				}
			}
		}()
	}

	err = stmts(ctx, tx)
	return
}

type txKeyType int

const (
	txKey txKeyType = iota
)

func setTx(ctx context.Context, tx *sqlx.Tx) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

func getTx(ctx context.Context) *sqlx.Tx {
	tx, _ := ctx.Value(txKey).(*sqlx.Tx)
	return tx
}
