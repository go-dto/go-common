// Package mysql opens a pooled *sql.DB for MySQL (github.com/go-sql-driver/mysql)
// with production defaults: dial/read/write timeouts, parseTime in UTC, a
// bounded pool with connection recycling, and a startup ping with retry.
//
//	db, err := mysql.Open(ctx, mysql.Config{
//		Host: "10.0.0.5", User: "svc", Password: pw, Database: "webhook",
//	})
//	if err != nil { ... }
//	defer db.Close()
//
// Queries are not logged one by one: wrap repository methods with
// logger.StartOp (db_system=mysql), which logs duration, slow calls and
// errors once per call.
package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"strconv"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/logger"
)

// Defaults applied by Open when the matching Config field is zero.
const (
	DefaultPort            = 3306
	DefaultDialTimeout     = 5 * time.Second
	DefaultReadTimeout     = 30 * time.Second
	DefaultWriteTimeout    = 30 * time.Second
	DefaultMaxOpenConns    = 20
	DefaultConnMaxLifetime = 3 * time.Minute
)

// Config configures Open. Either DSN or Host/User/… must be set.
type Config struct {
	// DSN in go-sql-driver format ("user:pass@tcp(host:3306)/db?…"). When
	// set, Host/Port/User/Password/Database/Params/TLS are ignored; timeouts,
	// pool settings and parseTime are still applied when unset in the DSN.
	DSN string

	Host     string
	Port     int // default 3306
	User     string
	Password string
	Database string
	// Params are extra session variables / DSN params, e.g. {"time_zone": "'+07:00'"}.
	Params map[string]string
	// TLS enables TLS with this config (e.g. Cloud SQL server CA).
	TLS *tls.Config

	// Driver I/O timeouts. Defaults 5s / 30s / 30s.
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Pool. MaxOpenConns default 20; MaxIdleConns defaults to MaxOpenConns
	// (as the driver recommends); ConnMaxLifetime default 3m so connections
	// are recycled before server/proxy idle timeouts (negative = never);
	// ConnMaxIdleTime 0 = none.
	// Keep replicas × MaxOpenConns below the server's max_connections.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// Retry controls the startup ping; see database.Retry.
	Retry database.Retry
	// SkipPing returns the pool without contacting the server.
	SkipPing bool

	// Configure, if set, can adjust the driver config last (collation,
	// interpolateParams, BeforeConnect for IAM tokens, …).
	Configure func(*driver.Config) error
}

// Open builds the driver config, opens the pool and (unless SkipPing) waits
// until the server answers. On failure the pool is closed.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	dc, err := driverConfig(cfg)
	if err != nil {
		return nil, err
	}
	connector, err := driver.NewConnector(dc)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}
	db := sql.OpenDB(connector)
	configurePool(db, cfg)

	if !cfg.SkipPing {
		fields := []zap.Field{
			zap.String(logger.KeyDBSystem, database.SystemMySQL),
			zap.String(database.KeyHost, dc.Addr),
			zap.String(database.KeyDBName, dc.DBName),
		}
		if err := database.WaitReady(ctx, cfg.Retry, db.PingContext, fields...); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("mysql: %w", err)
		}
	}
	return db, nil
}

// ValidateProduction validates the connection settings without opening a
// socket. TCP connections must use TLS with certificate verification and may
// not fall back to plaintext. Unix sockets are accepted for trusted local
// proxies. Custom named TLS registrations remain the caller's responsibility.
func (cfg Config) ValidateProduction() error {
	dc, err := driverConfig(cfg)
	if err != nil {
		return err
	}
	if dc.Net == "unix" {
		return nil
	}
	if dc.AllowFallbackToPlaintext {
		return errors.New("mysql: production TLS must not fall back to plaintext")
	}
	if dc.TLS != nil {
		if dc.TLS.InsecureSkipVerify {
			return errors.New("mysql: production TLS must verify server certificates")
		}
		return nil
	}
	switch dc.TLSConfig {
	case "", "false", "preferred", "skip-verify":
		return errors.New("mysql: production TCP connections require verified TLS")
	default:
		return nil
	}
}

func driverConfig(cfg Config) (*driver.Config, error) {
	var dc *driver.Config
	if cfg.DSN != "" {
		var err error
		if dc, err = driver.ParseDSN(cfg.DSN); err != nil {
			return nil, fmt.Errorf("mysql: parse DSN: %w", err)
		}
	} else {
		if cfg.Host == "" {
			return nil, errors.New("mysql: Host or DSN is required")
		}
		port := cfg.Port
		if port == 0 {
			port = DefaultPort
		}
		dc = driver.NewConfig()
		dc.Net = "tcp"
		dc.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(port))
		dc.User = cfg.User
		dc.Passwd = cfg.Password
		dc.DBName = cfg.Database
		dc.Params = maps.Clone(cfg.Params)
		dc.TLS = cfg.TLS
	}

	dc.ParseTime = true
	if dc.Loc == nil {
		dc.Loc = time.UTC
	}
	dc.Timeout = pick(dc.Timeout, cfg.DialTimeout, DefaultDialTimeout)
	dc.ReadTimeout = pick(dc.ReadTimeout, cfg.ReadTimeout, DefaultReadTimeout)
	dc.WriteTimeout = pick(dc.WriteTimeout, cfg.WriteTimeout, DefaultWriteTimeout)
	dc.Logger = driverLogger{}

	if cfg.Configure != nil {
		if err := cfg.Configure(dc); err != nil {
			return nil, fmt.Errorf("mysql: configure: %w", err)
		}
	}
	return dc, nil
}

// pick returns the explicit Config value, else what the DSN set, else def.
func pick(fromDSN, explicit, def time.Duration) time.Duration {
	switch {
	case explicit > 0:
		return explicit
	case fromDSN > 0:
		return fromDSN
	default:
		return def
	}
}

func configurePool(db *sql.DB, cfg Config) {
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = DefaultMaxOpenConns
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = maxOpen
	}
	lifetime := cfg.ConnMaxLifetime
	if lifetime == 0 {
		lifetime = DefaultConnMaxLifetime
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(max(lifetime, 0))
	db.SetConnMaxIdleTime(max(cfg.ConnMaxIdleTime, 0))
}

// driverLogger sends the driver's own messages (broken connections, packet
// errors) to the structured logger instead of stderr.
type driverLogger struct{}

func (driverLogger) Print(v ...any) {
	logger.L().WithOptions(zap.WithCaller(false)).Warn("mysql driver event",
		zap.String(logger.KeyDBSystem, database.SystemMySQL), zap.String("error_kind", "driver"))
}
