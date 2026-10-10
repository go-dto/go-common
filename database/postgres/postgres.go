// Package postgres opens a pgx connection pool (github.com/jackc/pgx/v5) with
// production defaults: connect timeout, bounded pool with connection
// recycling, optional statement_timeout, query logging through logger, and a
// startup ping with retry.
//
//	pool, err := postgres.Open(ctx, postgres.Config{
//		Host: "10.0.0.6", User: "svc", Password: pw, Database: "fhir", SSLMode: "verify-full",
//	})
//	if err != nil { ... }
//	defer pool.Close()
//
// Code written against database/sql can use stdlib.OpenDBFromPool(pool)
// (github.com/jackc/pgx/v5/stdlib) and keep these settings.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/logger"
)

// Defaults applied by Open when the matching Config field is zero.
const (
	DefaultPort            = 5432
	DefaultConnectTimeout  = 5 * time.Second
	DefaultMaxConns        = 20
	DefaultMaxConnLifetime = 30 * time.Minute
	DefaultMaxConnIdleTime = 5 * time.Minute
)

// Config configures Open. Either DSN or Host/User/… must be set.
type Config struct {
	// DSN as URL ("postgres://user:pass@host:5432/db?sslmode=require") or
	// key=value string. When set, Host/Port/User/Password/Database/SSLMode
	// are ignored; the settings below still apply.
	DSN string

	Host     string
	Port     int // default 5432
	User     string
	Password string
	Database string
	// SSLMode: disable | allow | prefer | require | verify-ca | verify-full.
	// Empty = pgx default (prefer). Use verify-full with ValidateProduction in
	// production unless a trusted local proxy intentionally terminates TLS.
	SSLMode string

	// ApplicationName shows up in pg_stat_activity.
	ApplicationName string

	// ConnectTimeout bounds dialing + TLS + auth of one connection. Default 5s.
	ConnectTimeout time.Duration

	// StatementTimeout sets the server-side statement_timeout for every
	// connection, so a runaway query is cancelled by Postgres. 0 = server
	// default.
	StatementTimeout time.Duration

	// Pool. MaxConns default 20 (keep replicas × MaxConns below the server's
	// max_connections); MinConns default 0; MaxConnLifetime 30m and
	// MaxConnIdleTime 5m (negative = never).
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration

	// QueryLog configures the query tracer.
	QueryLog QueryLogOptions

	// Retry controls the startup ping; see database.Retry.
	Retry database.Retry
	// SkipPing returns the pool without contacting the server.
	SkipPing bool

	// Configure, if set, can adjust the pool config last (QueryExecMode for
	// PgBouncer, AfterConnect, BeforeConnect for IAM tokens, …).
	Configure func(*pgxpool.Config) error
}

// Open builds the pool config, creates the pool and (unless SkipPing) waits
// until the server answers. On failure the pool is closed.
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := poolConfig(cfg)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	if !cfg.SkipPing {
		fields := []zap.Field{
			zap.String(logger.KeyDBSystem, database.SystemPostgreSQL),
			zap.String(database.KeyHost, net.JoinHostPort(pc.ConnConfig.Host, strconv.Itoa(int(pc.ConnConfig.Port)))),
			zap.String(database.KeyDBName, pc.ConnConfig.Database),
		}
		if err := database.WaitReady(ctx, cfg.Retry, pool.Ping, fields...); err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres: %w", err)
		}
	}
	return pool, nil
}

// ValidateProduction validates the effective connection settings without
// opening a socket. Every primary/fallback address must use TLS with server
// certificate verification. Applications using a trusted local proxy may
// intentionally skip this stricter validation.
func (cfg Config) ValidateProduction() error {
	pc, err := poolConfig(cfg)
	if err != nil {
		return err
	}
	cc := pc.ConnConfig
	if cc.TLSConfig == nil || cc.TLSConfig.InsecureSkipVerify {
		return errors.New("postgres: production connections require verified TLS")
	}
	for _, fallback := range cc.Fallbacks {
		if fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify {
			return errors.New("postgres: production fallback connections require verified TLS")
		}
	}
	return nil
}

// Check returns a health check that pings pool.
func Check(name string, pool *pgxpool.Pool) database.Check {
	if pool == nil {
		return database.Check{Name: name, Ping: func(context.Context) error {
			return errors.New("postgres: nil pool")
		}}
	}
	return database.Check{Name: name, Ping: pool.Ping}
}

func poolConfig(cfg Config) (*pgxpool.Config, error) {
	dsn := cfg.DSN
	if dsn == "" {
		if cfg.Host == "" {
			return nil, errors.New("postgres: Host or DSN is required")
		}
		dsn = buildURL(cfg)
	}
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// pgx errors can echo the DSN; do not wrap it into logs verbatim.
		return nil, errors.New("postgres: invalid DSN or connection settings")
	}

	cc := pc.ConnConfig
	cc.ConnectTimeout = durOr(cfg.ConnectTimeout, DefaultConnectTimeout)
	if cfg.ApplicationName != "" {
		cc.RuntimeParams["application_name"] = cfg.ApplicationName
	}
	if cfg.StatementTimeout > 0 {
		cc.RuntimeParams["statement_timeout"] = strconv.FormatInt(cfg.StatementTimeout.Milliseconds(), 10)
	}
	if !cfg.QueryLog.Disable {
		cc.Tracer = NewQueryTracer(cfg.QueryLog)
	}

	pc.MaxConns = DefaultMaxConns
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pc.MinConns = max(cfg.MinConns, 0)
	pc.MaxConnLifetime = max(durOr(cfg.MaxConnLifetime, DefaultMaxConnLifetime), 0)
	pc.MaxConnIdleTime = max(durOr(cfg.MaxConnIdleTime, DefaultMaxConnIdleTime), 0)

	if cfg.Configure != nil {
		if err := cfg.Configure(pc); err != nil {
			return nil, fmt.Errorf("postgres: configure: %w", err)
		}
	}
	return pc, nil
}

// buildURL escapes credentials properly (passwords often contain @ / : ?).
func buildURL(cfg Config) string {
	port := cfg.Port
	if port == 0 {
		port = DefaultPort
	}
	u := url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(port)),
		Path:   "/" + cfg.Database,
	}
	if cfg.User != "" {
		u.User = url.UserPassword(cfg.User, cfg.Password)
	}
	if cfg.SSLMode != "" {
		u.RawQuery = url.Values{"sslmode": {cfg.SSLMode}}.Encode()
	}
	return u.String()
}

// durOr returns v, or def when v is zero (negative values pass through).
func durOr(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}
