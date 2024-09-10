package errors

import (
	"errors"
	"fmt"

	"github.com/ansel1/merry/v2"
)

func Sentinel(message string) error {
	return merry.New(message, merry.NoCaptureStack())
}

func New(message string) error {
	return merry.WrapSkipping(err(message), 1, merry.CaptureStack(false))
}

func Newf(message string, a ...any) error {
	return merry.WrapSkipping(fmt.Errorf(message, a...), 1, merry.CaptureStack(false))
}

func Wrap(err error, wrappers ...merry.Wrapper) error {
	wrappers = append(wrappers, merry.CaptureStack(false))
	return merry.WrapSkipping(err, 1, wrappers...)
}

func Panic(r any, s []byte) error {
	return merry.Wrap(fmt.Errorf("panic: %v", r), merry.WithFormattedStack([]string{string(s)}))
}

func WithCause(err error) merry.Wrapper {
	return merry.WithCause(err)
}

func WithMessage(msg string) merry.Wrapper {
	return merry.WithMessage(msg)
}

func WithMessagef(msg string, args ...any) merry.Wrapper {
	return merry.WithMessagef(msg, args...)
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	return errors.As(err, target)
}

type err string

func (e err) Error() string {
	return string(e)
}
