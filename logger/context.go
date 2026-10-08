package logger

import (
	"context"

	"go.uber.org/zap"

	"github.com/go-dto/go-common/logger/ctxid"
)

type ctxFieldsKey struct{}

// WithFields returns a copy of ctx carrying extra log fields. Every logger
// obtained with FromContext(ctx) — in this function or any callee — includes
// them, so you set org_id / request_id once instead of in every layer.
//
// Fields with the same key added later override earlier ones.
func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(fields) == 0 {
		return ctx
	}
	prev, _ := ctx.Value(ctxFieldsKey{}).([]zap.Field)
	merged := make([]zap.Field, 0, len(prev)+len(fields))
	// Drop previous fields whose key is being overridden, so the JSON output
	// never contains the same key twice.
	for _, p := range prev {
		if !hasKey(fields, p.Key) {
			merged = append(merged, p)
		}
	}
	merged = append(merged, fields...)
	return context.WithValue(ctx, ctxFieldsKey{}, merged)
}

// FieldsFromContext returns the fields stored with WithFields (nil if none).
func FieldsFromContext(ctx context.Context) []zap.Field {
	if ctx == nil {
		return nil
	}
	f, _ := ctx.Value(ctxFieldsKey{}).([]zap.Field)
	return f
}

// FromContext returns the global logger enriched with the trace id (set by the
// HTTP request-id middleware via ctxid) and any fields added with WithFields.
// It always reflects the current global logger and level.
func FromContext(ctx context.Context) *zap.Logger {
	l := L()
	if ctx == nil {
		return l
	}
	fields := FieldsFromContext(ctx)
	if id, ok := ctxid.FromContext(ctx); ok && id != "" && !hasKey(fields, KeyTraceID) {
		l = l.With(zap.String(KeyTraceID, id))
	}
	if len(fields) > 0 {
		l = l.With(fields...)
	}
	return l
}

// Ctx is a short alias for FromContext.
func Ctx(ctx context.Context) *zap.Logger { return FromContext(ctx) }

// SugarFromContext is FromContext for printf-style logging.
func SugarFromContext(ctx context.Context) *zap.SugaredLogger { return FromContext(ctx).Sugar() }

func hasKey(fields []zap.Field, key string) bool {
	for _, f := range fields {
		if f.Key == key {
			return true
		}
	}
	return false
}
