package gorundb

import (
	"github.com/jswidler/gorun/errors"
	"github.com/lib/pq"
)

var (
	ErrConflict                 = errors.Sentinel("uniqueness conflict")
	ErrInvalidForeignKey        = errors.Sentinel("invalid foreign key")
	ErrDeleteViolatesForeignKey = errors.Sentinel("delete violates foreign key")
	ErrDatabaseError            = errors.Sentinel("database error")
	ErrNotFound                 = errors.Sentinel("not found")

	ErrNotUpdateable = errors.Sentinel("not updateable")
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
