package errors

import (
	"errors"
	"fmt"

	"github.com/ansel1/merry/v2"
)

var (
	ErrInternalServerError = Sentinel("internal server error", 500,
		WithUserMessage("An internal service error occurred."))

	ErrNotYetImplemented = Sentinel("internal server error", 500,
		WithUserMessage("This endpoint is not yet implemented."))

	ErrUnauthorized = Sentinel("unauthorized", 401,
		WithUserMessage("You must be logged in to continue."))

	ErrForbidden = Sentinel("forbidden", 403,
		WithUserMessage("You are not authorized to perform this action."))

	ErrNotFound = Sentinel("not found", 400,
		WithUserMessage("The requested resource was not found."))

	ErrBadRequest = Sentinel("bad request", 400,
		WithUserMessage("The request was invalid."))

	ErrInputValidation = Sentinel("input validation", 400,
		WithUserMessage("Request arguments failed validation."))
)

func Sentinel(message string, httpCode int, wrappers ...merry.Wrapper) error {
	wrappers = append(wrappers, merry.WithHTTPCode(httpCode), merry.NoCaptureStack())
	return merry.New(message, wrappers...)
}

func Wrap(err error, wrappers ...merry.Wrapper) error {
	wrappers = append(wrappers, merry.CaptureStack(false))
	return merry.WrapSkipping(err, 1, wrappers...)
}

// New returns a new Internal Server Error.
func New(message string) error {
	return merry.WrapSkipping(ErrInternalServerError, 1, WithCause(errors.New(message)), merry.CaptureStack(false))
}

// New returns a new Internal Server Error using a format string.
func Newf(message string, a ...any) error {
	return merry.WrapSkipping(ErrInternalServerError, 1, WithCause(fmt.Errorf(message, a...)), merry.CaptureStack(false))
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

func WithUserMessage(msg string) merry.Wrapper {
	return merry.WithUserMessage(msg)
}

func WithUserMessagef(msg string, args ...any) merry.Wrapper {
	return merry.WithUserMessagef(msg, args...)
}

func WithHTTPCode(code int) merry.Wrapper {
	return merry.WithHTTPCode(code)
}

func WithFieldErrors(fieldErrors map[string]any) merry.Wrapper {
	return merry.WithValue(fieldErrorsKey, fieldErrors)
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	return errors.As(err, target)
}

type Err struct {
	Error       string         `json:"error"`
	Code        int            `json:"code"`
	Message     string         `json:"message"`
	FieldErrors map[string]any `json:"fieldErrors,omitempty"`
}

// ErrorBody return a response body for the given error.  If the error code is 500, the default error message is redacted for the user.
func ErrorBody(err error) Err {
	code := merry.HTTPCode(err)
	e := "internal server error"
	if code != 500 {
		e = err.Error()
	}

	return Err{
		Message:     UserMessage(err),
		Error:       e,
		Code:        code,
		FieldErrors: FieldErrors(err),
	}
}

// UserMessage returns the end-user message.
// If not set, returns "An internal service error occurred." (in contrast to merry's default of empty strings).
func UserMessage(err error) string {
	msg := merry.UserMessage(err)
	if msg == "" {
		return "An internal service error occurred."
	}
	return msg
}

func FieldErrors(err error) map[string]any {
	m, _ := merry.Value(err, fieldErrorsKey).(map[string]any)
	return m
}

type errKey int

const (
	fieldErrorsKey errKey = iota
)
