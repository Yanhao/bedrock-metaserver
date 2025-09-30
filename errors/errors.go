// Package errors provides a unified error handling mechanism
package errors

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

// Define error type constants
const (
	// System-level errors
	ErrCodeSystem              = "SYSTEM_ERROR"
	ErrCodeNotInitialized      = "NOT_INITIALIZED"
	ErrCodeInternal            = "INTERNAL_ERROR"
	ErrCodeTimeout             = "TIMEOUT"
	ErrCodeDatabase            = "DATABASE_ERROR"
	ErrCodeNetwork             = "NETWORK_ERROR"
	ErrCodeResourceExhausted   = "RESOURCE_EXHAUSTED"
	ErrCodeNotLeader           = "NOT_LEADER"
	ErrCodeNoAvailableResource = "NO_AVAILABLE_RESOURCE"
	ErrCodeUnsupported         = "UNSUPPORTED"

	// Business-level errors
	ErrCodeNotFound           = "NOT_FOUND"
	ErrCodeAlreadyExists      = "ALREADY_EXISTS"
	ErrCodeInvalidArgument    = "INVALID_ARGUMENT"
	ErrCodeUnauthorized       = "UNAUTHORIZED"
	ErrCodeForbidden          = "FORBIDDEN"
	ErrCodeFailedPrecondition = "FAILED_PRECONDITION"
	ErrCodePermissionDenied   = "PERMISSION_DENIED"
)

// Error custom error type containing error code, error message, original error, and stack trace

type Error struct {
	Code    string // Error code
	Message string // Error message
	Err     error  // Original error
	Stack   string // Stack trace
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.Err != nil {
		if e.Stack != "" {
			return fmt.Sprintf("[%s] %s: %v\n%s", e.Code, e.Message, e.Err, e.Stack)
		}
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	if e.Stack != "" {
		return fmt.Sprintf("[%s] %s\n%s", e.Code, e.Message, e.Stack)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap implements the errors.Unwrap interface
func (e *Error) Unwrap() error {
	return e.Err
}

// Is implements the errors.Is interface
func (e *Error) Is(target error) bool {
	if target, ok := target.(*Error); ok {
		return e.Code == target.Code
	}
	return false
}

// getStackTrace creates stack trace information
func getStackTrace() string {
	const maxCallers = 10
	var stack strings.Builder

	for i := 2; i < maxCallers; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		funcName := runtime.FuncForPC(pc).Name()
		// Skip internal calls within the errors package
		if strings.Contains(funcName, "sr.ht/moyanhao/bedrock-metaserver/errors") {
			continue
		}
		stack.WriteString(fmt.Sprintf("\t%s:%d %s\n", file, line, funcName))
	}

	return stack.String()
}

// New creates a new error
func New(code, message string) error {
	return &Error{
		Code:    code,
		Message: message,
		Stack:   getStackTrace(),
	}
}

// Newf creates an error with formatted message
func Newf(code, message string, args ...interface{}) error {
	return &Error{
		Code:    code,
		Message: fmt.Sprintf(message, args...),
		Stack:   getStackTrace(),
	}
}

// Wrap wraps an error with additional context information
func Wrap(err error, code, message string) error {
	if err == nil {
		return nil
	}

	// If it's already an Error type, preserve the original stack trace
	if customErr, ok := err.(*Error); ok {
		return &Error{
			Code:    code,
			Message: message,
			Err:     err,
			Stack:   customErr.Stack,
		}
	}

	return &Error{
		Code:    code,
		Message: message,
		Err:     err,
		Stack:   getStackTrace(),
	}
}

// Wrapf wraps an error with formatted context information
func Wrapf(err error, code, message string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	// If it's already an Error type, preserve the original stack trace
	if customErr, ok := err.(*Error); ok {
		return &Error{
			Code:    code,
			Message: fmt.Sprintf(message, args...),
			Err:     err,
			Stack:   customErr.Stack,
		}
	}

	return &Error{
		Code:    code,
		Message: fmt.Sprintf(message, args...),
		Err:     err,
		Stack:   getStackTrace(),
	}
}

// IsNotFound checks if an error is of NotFound type
func IsNotFound(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeNotFound
	}
	return false
}

// IsAlreadyExists checks if an error is of AlreadyExists type
func IsAlreadyExists(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeAlreadyExists
	}
	return false
}

// IsInvalidArgument checks if an error is of InvalidArgument type
func IsInvalidArgument(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeInvalidArgument
	}
	return false
}

// IsTimeout checks if an error is of Timeout type
func IsTimeout(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeTimeout
	}
	return false
}

// IsInternal checks if an error is of Internal type
func IsInternal(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeInternal
	}
	return false
}

// IsNotLeader checks if an error is of NotLeader type
func IsNotLeader(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeNotLeader
	}
	return false
}

// IsNoAvailableResource checks if an error is of NoAvailableResource type
func IsNoAvailableResource(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeNoAvailableResource
	}
	return false
}

// IsUnsupported checks if an error is of Unsupported type
func IsUnsupported(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodeUnsupported
	}
	return false
}

// IsPermissionDenied checks if an error is of PermissionDenied type
func IsPermissionDenied(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCodePermissionDenied
	}
	return false
}

// Unwrap gets the original error
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// As attempts to convert the error to the specified type
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

// Define additional error type constants
const (
	ErrCodeAborted         = "ABORTED"
	ErrCodeUnavailable     = "UNAVAILABLE"
	ErrCodeUnauthenticated = "UNAUTHENTICATED"
	ErrCodeNotSupported    = "NOT_SUPPORTED"
)

// Predefined common errors
var (
	ErrUnknown             = New(ErrCodeInternal, "unknown error")
	ErrInvalidArgument     = New(ErrCodeInvalidArgument, "invalid argument")
	ErrNotFound            = New(ErrCodeNotFound, "resource not found")
	ErrAlreadyExists       = New(ErrCodeAlreadyExists, "resource already exists")
	ErrInternal            = New(ErrCodeInternal, "internal error")
	ErrTimeout             = New(ErrCodeTimeout, "operation timed out")
	ErrNotLeader           = New(ErrCodeNotLeader, "not the leader")
	ErrNoAvailableResource = New(ErrCodeNoAvailableResource, "no available resource")
	ErrUnsupported         = New(ErrCodeUnsupported, "unsupported operation")
	ErrPermissionDenied    = New(ErrCodePermissionDenied, "permission denied")
	ErrAborted             = New(ErrCodeAborted, "operation aborted")
	ErrUnavailable         = New(ErrCodeUnavailable, "service unavailable")
	ErrUnauthenticated     = New(ErrCodeUnauthenticated, "unauthenticated")
	ErrNotSupported        = New(ErrCodeNotSupported, "not supported")

	// Data-related errors
	ErrNoSuchShard        = New(ErrCodeNotFound, "no such shard")
	ErrNoSuchStorage      = New(ErrCodeNotFound, "no such storage")
	ErrNoSuchDataServer   = New(ErrCodeNotFound, "no such data server")
	ErrNoSuchKey          = New(ErrCodeNotFound, "no such key")
	ErrDatabaseError      = New(ErrCodeDatabase, "database error")
	ErrNetworkError       = New(ErrCodeNetwork, "network error")
	ErrResourceExhausted  = New(ErrCodeResourceExhausted, "resource exhausted")
	ErrUnauthorized       = New(ErrCodeUnauthorized, "unauthorized")
	ErrForbidden          = New(ErrCodeForbidden, "forbidden")
	ErrFailedPrecondition = New(ErrCodeFailedPrecondition, "failed precondition")
)
