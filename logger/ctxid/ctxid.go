// Package ctxid stores the transport-level correlation id (X-Request-ID) in a
// context.Context. The logger package reads it and emits it as "trace_id".
package ctxid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// Header is the HTTP header carrying the correlation id, on inbound requests
// (set by echo's RequestID middleware) and on outbound calls.
const Header = "X-Request-ID"

type ctxKey struct{}

var requestIDKey ctxKey

// WithContext returns a copy of ctx carrying id.
func WithContext(ctx context.Context, id string) context.Context {
	if ctx == nil {
		return context.WithValue(context.Background(), requestIDKey, id)
	}
	return context.WithValue(ctx, requestIDKey, id)
}

// FromContext returns the id stored by WithContext. ok is false when no
// non-empty id is present.
func FromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	id, ok := ctx.Value(requestIDKey).(string)
	return id, ok && id != ""
}

// New returns a random 32-character hex id (same length as echo's RequestID).
func New() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read does not fail on supported platforms
	return hex.EncodeToString(b[:])
}

// Ensure returns ctx unchanged if it already carries an id; otherwise it
// stores a new one. Use it at the start of work that does not come from an
// HTTP request (background workers, consumers, cron jobs) so its logs still
// share one trace_id.
func Ensure(ctx context.Context) (context.Context, string) {
	if id, ok := FromContext(ctx); ok {
		return ctx, id
	}
	id := New()
	return WithContext(ctx, id), id
}
