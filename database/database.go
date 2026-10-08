package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"

	"github.com/go-dto/go-common/logger"
)

// Log field keys shared by the drivers (db_system is logger.KeyDBSystem).
const (
	KeyDBName    = "db_name"
	KeyHost      = "db_host"
	KeyStatement = "db_statement"
	KeyOperation = "db_operation"
	KeyRows      = "db_rows_affected"
	KeyAttempt   = "attempt"
)

// db_system values.
const (
	SystemMySQL      = "mysql"
	SystemPostgreSQL = "postgresql"
	SystemMongoDB    = "mongodb"
)

// Defaults applied to Retry fields left at zero.
const (
	DefaultRetryAttempts  = 5
	DefaultInitialBackoff = 500 * time.Millisecond
	DefaultMaxBackoff     = 5 * time.Second
	DefaultPingTimeout    = 5 * time.Second
)

// Retry controls how Open/Connect wait for the database at startup, so a
// service started alongside its database (or a Cloud SQL proxy sidecar) does
// not crash-loop. The zero value uses the Default* constants.
type Retry struct {
	// Attempts is the total number of pings (default 5; 1 = no retry).
	Attempts int

	// InitialBackoff doubles after each failed ping up to MaxBackoff, with
	// jitter. Defaults 500ms and 5s.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	// PingTimeout bounds each ping. Default 5s.
	PingTimeout time.Duration
}

func (r Retry) withDefaults() Retry {
	if r.Attempts <= 0 {
		r.Attempts = DefaultRetryAttempts
	}
	if r.InitialBackoff <= 0 {
		r.InitialBackoff = DefaultInitialBackoff
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = DefaultMaxBackoff
	}
	if r.MaxBackoff < r.InitialBackoff {
		r.MaxBackoff = r.InitialBackoff
	}
	if r.PingTimeout <= 0 {
		r.PingTimeout = DefaultPingTimeout
	}
	return r
}

// Overridable in tests.
var sleep = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// WaitReady pings until it succeeds, r.Attempts is exhausted or ctx is done.
// fields identify the database in logs (db_system, db_host, db_name); never
// pass credentials. Each failed ping is logged at warn, success at info.
func WaitReady(ctx context.Context, r Retry, ping func(context.Context) error, fields ...zap.Field) error {
	r = r.withDefaults()
	l := logger.FromContext(ctx).With(fields...)
	start := time.Now()
	backoff := r.InitialBackoff
	var err error
	for attempt := 1; ; attempt++ {
		pctx, cancel := context.WithTimeout(ctx, r.PingTimeout)
		err = ping(pctx)
		cancel()
		if err == nil {
			l.Info("database connected",
				zap.Int(KeyAttempt, attempt),
				zap.Float64(logger.KeyDurationMs, float64(time.Since(start))/float64(time.Millisecond)))
			return nil
		}
		if attempt >= r.Attempts || ctx.Err() != nil {
			break
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		l.Warn("database not ready, retrying", zap.Int(KeyAttempt, attempt), zap.Duration("backoff", wait), zap.Error(err))
		if serr := sleep(ctx, wait); serr != nil {
			err = errors.Join(err, serr)
			break
		}
		backoff = min(backoff*2, r.MaxBackoff)
	}
	l.Error("database unreachable", zap.Error(err))
	return fmt.Errorf("database: not ready after %s: %w", time.Since(start).Round(time.Millisecond), err)
}

// SQLCheck returns a health Check that pings db.
func SQLCheck(name string, db *sql.DB) Check {
	return Check{Name: name, Ping: db.PingContext}
}
