package postgres

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

const secret = "p@ss:w/rd?#"

func TestPoolConfigFromFields(t *testing.T) {
	pc, err := poolConfig(Config{
		Host: "db.internal", User: "svc", Password: secret, Database: "fhir", SSLMode: "require",
		ApplicationName: "webhook", StatementTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cc := pc.ConnConfig
	if cc.Host != "db.internal" || cc.Port != DefaultPort || cc.User != "svc" || cc.Password != secret || cc.Database != "fhir" {
		t.Errorf("conn = host %s port %d user %s db %s passwd ok=%v", cc.Host, cc.Port, cc.User, cc.Database, cc.Password == secret)
	}
	if cc.TLSConfig == nil {
		t.Error("sslmode=require should configure TLS")
	}
	if cc.RuntimeParams["application_name"] != "webhook" || cc.RuntimeParams["statement_timeout"] != "15000" {
		t.Errorf("runtime params = %v", cc.RuntimeParams)
	}
	if cc.ConnectTimeout != DefaultConnectTimeout || cc.Tracer == nil {
		t.Errorf("ConnectTimeout=%v Tracer=%v", cc.ConnectTimeout, cc.Tracer)
	}
	if pc.MaxConns != DefaultMaxConns || pc.MaxConnLifetime != DefaultMaxConnLifetime || pc.MaxConnIdleTime != DefaultMaxConnIdleTime {
		t.Errorf("pool = max %d lifetime %v idle %v", pc.MaxConns, pc.MaxConnLifetime, pc.MaxConnIdleTime)
	}
}

func TestPoolConfigOverrides(t *testing.T) {
	pc, err := poolConfig(Config{
		DSN:             "postgres://u:pw@10.0.0.6:6432/app",
		Host:            "ignored",
		MaxConns:        5,
		MinConns:        2,
		MaxConnLifetime: -1,
		QueryLog:        QueryLogOptions{Disable: true},
		Configure: func(c *pgxpool.Config) error {
			c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if pc.ConnConfig.Host != "10.0.0.6" || pc.ConnConfig.Port != 6432 {
		t.Errorf("DSN not used: %s:%d", pc.ConnConfig.Host, pc.ConnConfig.Port)
	}
	if pc.MaxConns != 5 || pc.MinConns != 2 || pc.MaxConnLifetime != 0 {
		t.Errorf("pool = max %d min %d lifetime %v", pc.MaxConns, pc.MinConns, pc.MaxConnLifetime)
	}
	if pc.ConnConfig.Tracer != nil {
		t.Error("QueryLog.Disable should remove the tracer")
	}
	if pc.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeExec {
		t.Error("Configure not applied")
	}
}

func TestPoolConfigErrors(t *testing.T) {
	if _, err := poolConfig(Config{}); err == nil {
		t.Error("missing Host and DSN should fail")
	}
	_, err := poolConfig(Config{DSN: "postgres://u:" + secret + "@host:notaport/db"})
	if err == nil {
		t.Fatal("invalid DSN should fail")
	}
	if strings.Contains(err.Error(), "rd?#") {
		t.Errorf("error leaks password: %v", err)
	}
	boom := errors.New("boom")
	if _, err := poolConfig(Config{Host: "h", Configure: func(*pgxpool.Config) error { return boom }}); !errors.Is(err, boom) {
		t.Errorf("Configure error = %v", err)
	}
}

func TestOpenUnreachable(t *testing.T) {
	t.Cleanup(logger.ReplaceForTest(zap.NewNop()))
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(l.Addr().String())
	_ = l.Close()
	p, _ := strconv.Atoi(port)

	_, err = Open(context.Background(), Config{
		Host: "127.0.0.1", Port: p, User: "svc", Password: secret, SSLMode: "disable",
		Retry: database.Retry{Attempts: 2, InitialBackoff: time.Millisecond},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error leaks password: %v", err)
	}
}

func TestQueryTracer(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(logger.ReplaceForTest(zap.New(core)))
	tr := NewQueryTracer(QueryLogOptions{SlowThreshold: 50 * time.Millisecond})
	ctx := ctxid.WithContext(context.Background(), "trace-1")

	run := func(sql string, d time.Duration, tag string, err error) {
		qctx := tr.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql, Args: []any{"PATIENT-NIK-123"}})
		if d > 0 {
			qctx = context.WithValue(qctx, queryStartKey{}, queryStart{sql: sql, start: time.Now().Add(-d)})
		}
		tr.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag(tag), Err: err})
	}

	run("INSERT INTO deliveries (id)\n   VALUES ($1)", 0, "INSERT 0 1", nil)
	run("SELECT pg_sleep(1)", 100*time.Millisecond, "SELECT 1", nil)
	run("SELECT * FROM missing", 0, "", errors.New(`relation "missing" does not exist`))
	run("SELECT id FROM t WHERE id = $1", 0, "", pgx.ErrNoRows)

	all := logs.All()
	if len(all) != 4 {
		t.Fatalf("lines = %d: %v", len(all), all)
	}
	want := []struct {
		msg string
		lvl zapcore.Level
	}{
		{"postgres query", zapcore.DebugLevel},
		{"postgres query slow", zapcore.WarnLevel},
		{"postgres query failed", zapcore.WarnLevel},
		{"postgres query", zapcore.DebugLevel}, // ErrNoRows is not a failure
	}
	for i, w := range want {
		if all[i].Message != w.msg || all[i].Level != w.lvl {
			t.Errorf("line %d = %s/%s, want %s/%s", i, all[i].Level, all[i].Message, w.lvl, w.msg)
		}
		cm := all[i].ContextMap()
		if cm[logger.KeyTraceID] != "trace-1" || cm[logger.KeyDBSystem] != database.SystemPostgreSQL {
			t.Errorf("line %d fields = %v", i, cm)
		}
		for _, v := range cm {
			if s, ok := v.(string); ok && strings.Contains(s, "PATIENT-NIK") {
				t.Errorf("line %d logs a query argument: %v", i, cm)
			}
		}
	}
	first := all[0].ContextMap()
	if first[database.KeyStatement] != "INSERT INTO deliveries (id) VALUES ($1)" ||
		first[database.KeyOperation] != "INSERT" || first[database.KeyRows] != int64(1) {
		t.Errorf("insert fields = %v", first)
	}
}

func TestQueryTracerSuccessLevel(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	t.Cleanup(logger.ReplaceForTest(zap.New(core)))
	info := zapcore.InfoLevel
	tr := NewQueryTracer(QueryLogOptions{SuccessLevel: &info})
	ctx := tr.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag("SELECT 1")})
	if logs.Len() != 1 {
		t.Fatalf("lines = %d", logs.Len())
	}
	// End without Start (foreign ctx) must be a no-op.
	tr.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{})
}

func TestNormalizeSQL(t *testing.T) {
	if got := normalizeSQL("SELECT  *\n\tFROM t\n", 100); got != "SELECT * FROM t" {
		t.Errorf("got %q", got)
	}
	got := normalizeSQL("SELECT 'é'", 9) // 'é' is 2 bytes and straddles the limit
	if got != "SELECT '…" {
		t.Errorf("got %q, want cut on a rune boundary", got)
	}
}

// TestIntegration runs against a real server when POSTGRES_TEST_DSN is set, e.g.
// docker run -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:17 and
// POSTGRES_TEST_DSN='postgres://postgres:pw@127.0.0.1:5432/postgres?sslmode=disable'.
func TestIntegration(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN not set")
	}
	ctx := context.Background()
	pool, err := Open(ctx, Config{DSN: dsn, StatementTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var timeout string
	if err := pool.QueryRow(ctx, "SHOW statement_timeout").Scan(&timeout); err != nil || timeout != "100ms" {
		t.Fatalf("statement_timeout = %q, err = %v", timeout, err)
	}
	if _, err := pool.Exec(ctx, "SELECT pg_sleep(1)"); err == nil {
		t.Error("statement_timeout should cancel a long query")
	}
	if err := Check("postgres", pool).Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
