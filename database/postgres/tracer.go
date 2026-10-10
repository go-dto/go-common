package postgres

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	// It is used only when IncludeStatement is true.
	MaxStatementBytes int

	// IncludeStatement adds normalized SQL text to logs. It is false by
	// default because SQL assembled with literals may contain credentials or
	// personal data even when query arguments are excluded.
	IncludeStatement bool
}

// NewQueryTracer returns a pgx.QueryTracer that logs each Query/QueryRow/Exec
// with the caller's context (trace_id, op, span_id, …). Query arguments are
// never logged. SQL text is omitted by default and must be explicitly enabled.
func NewQueryTracer(opts QueryLogOptions) pgx.QueryTracer {
	t := &queryTracer{
		successLevel: zapcore.DebugLevel,
		slow:         DefaultSlowThreshold,
		maxBytes:     DefaultMaxStatementBytes,
		statement:    opts.IncludeStatement,
	}
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
	statement    bool
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
		zap.Float64(logger.KeyDurationMs, float64(d)/float64(time.Millisecond)),
	}
	if t.statement {
		fields = append(fields, zap.String(database.KeyStatement, normalizeSQL(qs.sql, t.maxBytes)))
	}
	operation := statementOperation(qs.sql)
	if tag := data.CommandTag.String(); tag != "" {
		operation, _, _ = strings.Cut(tag, " ")
	}
	if operation != "" {
		fields = append(fields, zap.String(database.KeyOperation, operation))
	}
	if err != nil {
		fields = append(fields, zap.String("error_kind", "query"))
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code != "" {
			fields = append(fields, zap.String("db_error_code", pgErr.Code))
		}
	} else if data.CommandTag.String() != "" {
		fields = append(fields, zap.Int64(database.KeyRows, data.CommandTag.RowsAffected()))
	}
	ce.Write(fields...)
}

func statementOperation(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return ""
	}
	op := strings.ToUpper(fields[0])
	if len(op) > 32 {
		return ""
	}
	for _, r := range op {
		if r < 'A' || r > 'Z' {
			return ""
		}
	}
	return op
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
