package gorundb

import (
	"github.com/jswidler/gorun/errors"
	"github.com/lib/pq"
)

var (
	ErrConflict                 = errors.Sentinel("uniqueness conflict", 500, errors.WithUserMessage("An unexpected error occurred."))
	ErrInvalidForeignKey        = errors.Sentinel("invalid foreign key", 500, errors.WithUserMessage("An unexpected error occurred."))
	ErrDeleteViolatesForeignKey = errors.Sentinel("delete violates foreign key", 500, errors.WithUserMessage("An unexpected error occurred."))
	ErrDatabaseError            = errors.Sentinel("database error", 500, errors.WithUserMessage("An unexpected error occurred."))
	ErrNotFound                 = errors.Sentinel("not found", 500, errors.WithUserMessage("That information could not be found."))

	ErrNotUpdateable = errors.Sentinel("not updateable", 500, errors.WithUserMessage("An unexpected error occurred."))
)

const (
	uniqueViolationErr   = pq.ErrorCode("23505")
	invalidForeignKeyErr = pq.ErrorCode("23503")
)

func isConflict(err error) bool {
	var pqErr *pq.Error
	ok := errors.As(err, &pqErr)
	return ok && pqErr.Code == uniqueViolationErr
}

func isInvalidForeignKey(err error) bool {
	var pqErr *pq.Error
	ok := errors.As(err, &pqErr)
	return ok && pqErr.Code == invalidForeignKeyErr
}
