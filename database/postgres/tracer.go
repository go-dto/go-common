package postgres

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/logger"
)

// Defaults for QueryLogOptions.
const (
	DefaultSlowThreshold     = 500 * time.Millisecond
	DefaultMaxStatementBytes = 1000
)

// QueryLogOptions configures the query tracer. Levels mirror logger/mongolog:
// success at debug, slow at warn, failure at warn (the repository Op reports
// the error at error level once).
type QueryLogOptions struct {
	// Disable turns query logging off.
	Disable bool

	// SuccessLevel for successful queries. Default debug.
	SuccessLevel *zapcore.Level

	// SlowThreshold logs successful queries at warn when they take at least
	// this long. Default 500ms; negative disables.
	SlowThreshold time.Duration

	// MaxStatementBytes truncates db_statement. Default 1000.
	MaxStatementBytes int
}

// NewQueryTracer returns a pgx.QueryTracer that logs each Query/QueryRow/Exec
// with the caller's context (trace_id, op, span_id, …). Only the SQL text is
// logged — never the arguments, which may hold personal data.
func NewQueryTracer(opts QueryLogOptions) pgx.QueryTracer {
	t := &queryTracer{successLevel: zapcore.DebugLevel, slow: DefaultSlowThreshold, maxBytes: DefaultMaxStatementBytes}
	if opts.SuccessLevel != nil {
		t.successLevel = *opts.SuccessLevel
	}
	if opts.SlowThreshold != 0 {
		t.slow = opts.SlowThreshold
	}
	if opts.MaxStatementBytes > 0 {
		t.maxBytes = opts.MaxStatementBytes
	}
	return t
}

type queryTracer struct {
	successLevel zapcore.Level
	slow         time.Duration
	maxBytes     int
}

type queryStartKey struct{}

type queryStart struct {
	sql   string
	start time.Time
}

func (t *queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, queryStartKey{}, queryStart{sql: data.SQL, start: time.Now()})
}

func (t *queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	qs, ok := ctx.Value(queryStartKey{}).(queryStart)
	if !ok {
		return
	}
	d := time.Since(qs.start)
	err := data.Err
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}

	lvl, msg := t.successLevel, "postgres query"
	switch {
	case err != nil:
		lvl, msg = zapcore.WarnLevel, "postgres query failed"
	case t.slow > 0 && d >= t.slow:
		lvl, msg = zapcore.WarnLevel, "postgres query slow"
	}
	l := logger.FromContext(ctx).WithOptions(zap.WithCaller(false))
	ce := l.Check(lvl, msg)
	if ce == nil {
		return
	}
	fields := []zap.Field{
		zap.String(logger.KeyDBSystem, database.SystemPostgreSQL),
		zap.String(database.KeyStatement, normalizeSQL(qs.sql, t.maxBytes)),
		zap.Float64(logger.KeyDurationMs, float64(d)/float64(time.Millisecond)),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	} else if tag := data.CommandTag.String(); tag != "" {
		op, _, _ := strings.Cut(tag, " ")
		fields = append(fields, zap.String(database.KeyOperation, op), zap.Int64(database.KeyRows, data.CommandTag.RowsAffected()))
	}
	ce.Write(fields...)
}

// normalizeSQL collapses whitespace (multi-line queries become one line) and
// truncates to maxBytes on a rune boundary.
func normalizeSQL(s string, maxBytes int) string {
	s = strings.Join(strings.FieldsFunc(s, unicode.IsSpace), " ")
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
