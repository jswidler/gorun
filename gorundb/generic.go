package gorundb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jswidler/gorun/errors"
	"github.com/jswidler/gorun/gorundb/internal/columns"
	"github.com/jswidler/gorun/tenantctx"
	"github.com/lib/pq"
)

// This could be better abstracted for sharing.

// This file provides a library of opinionated generic database functions that can be used to interact with the database.
// TODO: write this in a way it can be a bit more flexible depending on the columns in the table, maybe allow customizable
// column names and types in some cases.

// Some tables fields are special:
// `id` - primary varchar-32 key, required
// `tenant_id` - tenant id as string.  if it is added to the context, it will be used in most generated queries
// `created_at` - timestamp without time zone, required, managed by these functions
// `updated_at` - timestamp without time zone, required to make updates (omit for write once tables), managed by these functions

type GetContexter interface {
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type ExecContexter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type NamedExecContexter interface {
	NamedExecContext(ctx context.Context, query string, args interface{}) (sql.Result, error)
}

type SelectContexter interface {
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

func byId[T any](ctx context.Context, db GetContexter, table string, id string) (*T, error) {
	if tenantctx.GetTenant(ctx) == "" {
		return byFieldWithoutTenant[T](ctx, db, table, "id", id)
	}
	return byFieldWithTenant[T](ctx, db, table, "id", id)
}

func byIds[T any](ctx context.Context, db SelectContexter, table string, ids []string) ([]*T, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	tenantId := tenantctx.GetTenant(ctx)
	if tenantId == "" {
		return queryMany[T](ctx, db,
			fmt.Sprintf(`SELECT * FROM "%s" WHERE "id" = ANY($1)`, table),
			pq.StringArray(ids),
		)
	}

	return queryMany[T](ctx, db,
		fmt.Sprintf(`SELECT * FROM "%s" WHERE "tenant_id" = $1 AND "id" = ANY($2)`, table),
		tenantId, pq.StringArray(ids),
	)
}

func byFieldWithTenant[T any](ctx context.Context, db GetContexter, table, field, value string) (*T, error) {
	query := fmt.Sprintf(`SELECT * FROM "%s" WHERE "tenant_id" = $1 AND "%s" = $2`, table, field)
	return queryOne[T](ctx, db, query, tenantctx.MustGetTenant(ctx), value)
}

func byFieldWithoutTenant[T any](ctx context.Context, db GetContexter, table, field, value string) (*T, error) {
	query := fmt.Sprintf(`SELECT * FROM "%s" WHERE "%s" = $1`, table, field)
	return queryOne[T](ctx, db, query, value)
}

func updateById[T any](ctx context.Context, db GetContexter, table string, row *T) error {
	cols := columns.Of(row)
	cols.Remove("created_at")
	cols.Remove("tenant_id") // TODO: add to where clause

	_, found := cols.Get("updated_at")
	if !found {
		return errors.Wrap(ErrNotUpdateable)
	}
	cols.Set("updated_at", time.Now().UTC())

	id, _ := cols.Get("id")
	if id == nil {
		return errors.Wrap(ErrDatabaseError, errors.WithMessage("failed to update row, no id"))
	}
	cols.Remove("id")

	query := fmt.Sprintf(`UPDATE "%s" SET (%s)=(%s) WHERE id=$1 RETURNING *`, table, cols.Columns(), cols.ColumnsPlaceholder(2))
	params := []any{id}
	params = append(params, cols.Values()...)

	var r T
	err := db.GetContext(ctx, &r, query, params...)
	if err != nil {
		if isConflict(err) {
			return errors.Wrap(ErrConflict, errors.WithCause(err))
		} else if isInvalidForeignKey(err) {
			return errors.Wrap(ErrInvalidForeignKey, errors.WithCause(err))
		}
		return errors.Wrap(ErrDatabaseError, errors.WithCause(err), errors.WithMessagef("failed to update %s with id %s", table, id))
	}

	*row = r
	return nil
}

func deleteById(ctx context.Context, db ExecContexter, table string, id string) error {
	tenantId := tenantctx.GetTenant(ctx)
	if tenantId == "" {
		return delete(ctx, db, fmt.Sprintf(`DELETE FROM "%s" WHERE id = $1`, table), id)
	}
	return delete(ctx, db, fmt.Sprintf(`DELETE FROM "%s" WHERE tenant_id = $1 AND id = $2`, table), tenantId, id)
}

func deleteByIds(ctx context.Context, db ExecContexter, table string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tenantId := tenantctx.GetTenant(ctx)
	if tenantId == "" {
		return delete(ctx, db, fmt.Sprintf(`DELETE FROM "%s" WHERE "id" = ANY($1)`, table), pq.StringArray(ids))
	}
	return delete(ctx, db, fmt.Sprintf(`DELETE FROM "%s" WHERE "tenant_id" = $1 AND "id" = ANY($2)`, table), tenantId, pq.StringArray(ids))
}

func queryOne[T any](ctx context.Context, db GetContexter, query string, args ...interface{}) (*T, error) {
	var val T
	e := db.GetContext(ctx, &val, query, args...)
	if e != nil {
		if e == sql.ErrNoRows {
			return nil, errors.Wrap(ErrNotFound, errors.WithCause(e))
		}
		return &val, errors.Wrap(ErrDatabaseError, errors.WithCause(e))
	}
	return &val, nil
}

func queryMany[T any](ctx context.Context, db SelectContexter, query string, args ...interface{}) ([]*T, error) {
	var val []*T
	e := db.SelectContext(ctx, &val, query, args...)
	if e != nil {
		return nil, errors.Wrap(ErrDatabaseError, errors.WithCause(e))
	}
	return val, nil
}

func insert[T any](ctx context.Context, db GetContexter, table string, row *T) error {
	l := columns.Of(row)
	now := time.Now().UTC()
	if _, found := l.Get("created_at"); found {
		l.Set("created_at", now)
	}
	if _, found := l.Get("updated_at"); found {
		l.Set("updated_at", now)
	}

	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s) RETURNING *`, table, l.Columns(), l.ColumnsPlaceholder(1))

	var r T
	err := db.GetContext(ctx, &r, query, l.Values()...)
	if err != nil {
		if isConflict(err) {
			return errors.Wrap(ErrConflict, errors.WithCause(err))
		} else if isInvalidForeignKey(err) {
			return errors.Wrap(ErrInvalidForeignKey, errors.WithCause(err))
		}
		return errors.Wrap(ErrDatabaseError, errors.WithCause(err))
	}

	*row = r
	return nil
}

func insertBulk[T any](ctx context.Context, db NamedExecContexter, table string, rows []*T) error {
	if len(rows) == 0 {
		return nil
	}

	l := columns.Columns{}
	rowsInsert := []map[string]any{}

	now := time.Now().UTC()
	for _, row := range rows {
		l = columns.Of(row)
		if _, found := l.Get("created_at"); found {
			l.Set("created_at", now)
		}
		if _, found := l.Get("updated_at"); found {
			l.Set("updated_at", now)
		}
		rowsInsert = append(rowsInsert, l.Map())
	}

	placeholders := l.ColumnsNamedPlaceholder()
	columnNames := l.Columns()

	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, table, columnNames, placeholders)

	_, err := db.NamedExecContext(ctx, query, rowsInsert)
	if err != nil {
		if isConflict(err) {
			return errors.Wrap(ErrConflict, errors.WithCause(err))
		} else if isInvalidForeignKey(err) {
			return errors.Wrap(ErrInvalidForeignKey, errors.WithCause(err))
		}
		return errors.Wrap(ErrDatabaseError, errors.WithCause(err))
	}

	return nil
}

func upsert[T any](ctx context.Context, db NamedExecContexter, table string, row *T, conflict string) error {
	return upsertBulk(ctx, db, table, []*T{row}, conflict)
}

// updateBulk updates or insterts multiple rows in a table. Conflict should be one of the following to be valid SQL:
//
//	`(column1 [, column2, ...])` - 1 or more columns in parenthesis, depending on the columns that are unique
//	`ON CONSTRAINT "<constraint_name>"` - depending on a named constraint
//	`WHERE <condition>“ - depending on a condition
func upsertBulk[T any](ctx context.Context, db NamedExecContexter, table string, rows []*T, conflict string) error {
	return upsertBulkAtTime(ctx, db, time.Now().UTC(), table, rows, conflict)
}

func upsertBulkAtTime[T any](ctx context.Context, db NamedExecContexter, now time.Time, table string, rows []*T, conflict string) error {
	if len(rows) == 0 {
		return nil
	}

	l := columns.Columns{}
	rowsInsert := []map[string]any{}

	for _, row := range rows {
		l = columns.Of(row)
		// Require the table to have created_at and updated_at columns
		if _, found := l.Get("created_at"); !found {
			return errors.Wrap(ErrNotUpdateable)
		}
		if _, found := l.Get("updated_at"); !found {
			return errors.Wrap(ErrNotUpdateable)
		}
		l.Set("created_at", now)
		l.Set("updated_at", now)
		rowsInsert = append(rowsInsert, l.Map())
	}

	placeholders := l.ColumnsNamedPlaceholder()
	columnNames := l.Columns()

	l.Remove("id")
	l.Remove("created_at")
	l.Remove("tenant_id")

	upsertColumns := l.Names()
	onUpdate := []string{}
	for _, col := range upsertColumns {
		onUpdate = append(onUpdate, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col, col))
	}
	onUpdateClause := strings.Join(onUpdate, ", ")

	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s) ON CONFLICT %s DO UPDATE SET %s`,
		table, columnNames, placeholders,
		conflict, onUpdateClause,
	)

	_, err := db.NamedExecContext(ctx, query, rowsInsert)
	if err != nil {
		if isConflict(err) {
			return errors.Wrap(ErrConflict, errors.WithCause(err))
		} else if isInvalidForeignKey(err) {
			return errors.Wrap(ErrInvalidForeignKey, errors.WithCause(err))
		}
		return errors.Wrap(ErrDatabaseError, errors.WithCause(err))
	}

	return nil
}

func delete(ctx context.Context, db ExecContexter, query string, args ...interface{}) error {
	_, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		if isInvalidForeignKey(err) {
			return errors.Wrap(ErrDeleteViolatesForeignKey, errors.WithCause(err))
		}
		return errors.Wrap(ErrDatabaseError, errors.WithCause(err))
	}
	return nil
}
