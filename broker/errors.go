package broker

import (
	"context"
	"errors"
	"runtime/debug"
)

// ErrClosed is returned by drivers after Close.
var ErrClosed = errors.New("broker: closed")

// Permanent marks err as non-retryable: the handler gives up on the message
// (malformed payload, unknown type, …), so it is acked and dropped instead of
// being redelivered. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or any error it wraps) was marked with
// Permanent.
func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// PanicError is the error a handler panic is converted into. The message is
// nacked, so a handler that always panics on a message loops until the
// broker's dead letter policy removes it.
type PanicError struct {
	Value any
	// Stack is populated only when Options.LogPanicStack is enabled.
	Stack []byte
}

func (e *PanicError) Error() string { return "handler panic" }

func safeCall(ctx context.Context, h Handler, msg *Message, captureStack bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			panicErr := &PanicError{Value: r}
			if captureStack {
				panicErr.Stack = debug.Stack()
			}
			err = panicErr
		}
	}()
	return h(ctx, msg)
}
