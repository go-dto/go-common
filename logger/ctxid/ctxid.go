// Package ctxid stores the transport-level correlation id (X-Request-ID) in a
// context.Context. The logger package reads it and emits it as "trace_id".
package ctxid

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
	"time"
)

// Header is the HTTP header carrying the correlation id, on inbound requests
// (set by echo's RequestID middleware) and on outbound calls.
const Header = "X-Request-ID"

// MaxLength bounds externally supplied request IDs before they are copied into
// contexts, response headers, and logs.
const MaxLength = 128

type ctxKey struct{}

var requestIDKey ctxKey

var (
	readRandom      = rand.Read
	fallbackCounter atomic.Uint64
)

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

// Valid reports whether an externally supplied request ID is safe to use.
// IDs are intentionally limited to a small, portable character set to avoid
// control characters, unbounded log fields, and high-cardinality garbage.
func Valid(id string) bool {
	if len(id) == 0 || len(id) > MaxLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '-', '.', '_', ':':
			continue
		default:
			return false
		}
	}
	return true
}

// New returns a random 32-character hex id (same length as echo's RequestID).
func New() string {
	var b [16]byte
	if n, err := readRandom(b[:]); err != nil || n != len(b) {
		// Correlation IDs are not credentials, but they still must remain unique
		// when the operating-system random source is temporarily unavailable.
		var seed [16]byte
		binary.BigEndian.PutUint64(seed[:8], uint64(time.Now().UnixNano()))
		binary.BigEndian.PutUint64(seed[8:], fallbackCounter.Add(1))
		sum := sha256.Sum256(seed[:])
		copy(b[:], sum[:len(b)])
	}
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
