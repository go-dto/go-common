package cache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrorKind is a stable cache failure classification for control flow,
// metrics, and sanitized logs.
type ErrorKind string

const (
	KindInvalid     ErrorKind = "invalid"
	KindMiss        ErrorKind = "miss"
	KindClosed      ErrorKind = "closed"
	KindCanceled    ErrorKind = "canceled"
	KindTimeout     ErrorKind = "timeout"
	KindUnavailable ErrorKind = "unavailable"
	KindTooLarge    ErrorKind = "too_large"
)

var (
	// ErrMiss means the key does not exist or has expired.
	ErrMiss = errors.New("cache: miss")
	// ErrClosed means the store has already been closed.
	ErrClosed = errors.New("cache: closed")
)

// Store is safe for concurrent use. Implementations own their resources and
// Close must be idempotent.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Close() error
}

// Error is a sanitized cache error. Error never includes a key, value,
// credential, endpoint, or raw backend error. Unwrap preserves the cause for
// errors.Is/errors.As without making the cause part of the public message.
type Error struct {
	Operation string
	Kind      ErrorKind
	cause     error
}

// NewError creates a sanitized cache error for Store implementations.
func NewError(operation string, kind ErrorKind, cause error) *Error {
	return &Error{Operation: operation, Kind: kind, cause: cause}
}

func (e *Error) Error() string {
	if e == nil {
		return "cache: error"
	}
	if e.Operation == "" {
		return fmt.Sprintf("cache: %s", e.Kind)
	}
	return fmt.Sprintf("cache: %s: %s", e.Operation, e.Kind)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// KindOf returns a stable classification for err.
func KindOf(err error) ErrorKind {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, ErrMiss):
		return KindMiss
	case errors.Is(err, ErrClosed):
		return KindClosed
	case errors.Is(err, context.Canceled):
		return KindCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout
	}
	var target *Error
	if errors.As(err, &target) {
		return target.Kind
	}
	return KindUnavailable
}

// IsMiss reports whether err represents an absent or expired key.
func IsMiss(err error) bool { return errors.Is(err, ErrMiss) }

// IsTimeout reports whether the operation exceeded its context or backend
// timeout.
func IsTimeout(err error) bool { return KindOf(err) == KindTimeout }
