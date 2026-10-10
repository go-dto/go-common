// Package mongodb connects a pooled *mongo.Client (go.mongodb.org/mongo-driver
// v1) with production defaults: fail-fast connect and server selection
// timeouts, a bounded pool, command logging via logger/mongolog, and a startup
// ping with retry.
//
//	client, err := mongodb.Connect(ctx, mongodb.Config{URI: uri, AppName: "webhook-service"})
//	if err != nil { ... }
//	defer client.Disconnect(context.Background())
//	coll := client.Database("webhook").Collection("deliveries")
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/mongolog"
)

// Defaults applied by Connect when the matching Config field is zero. The
// driver's own defaults (30s timeouts, pool 100) make a missing server look
// like a hang, so these fail faster.
const (
	DefaultConnectTimeout         = 5 * time.Second
	DefaultServerSelectionTimeout = 5 * time.Second
	DefaultMaxPoolSize            = 50
	DefaultMaxConnIdleTime        = 5 * time.Minute
)

// Config configures Connect.
type Config struct {
	// URI is required ("mongodb://…" or "mongodb+srv://…"). Settings below
	// override the same settings in the URI.
	URI string

	// AppName shows up in server logs and currentOp.
	AppName string

	// Timeouts. ConnectTimeout and ServerSelectionTimeout default 5s.
	// Timeout is the client-side operation timeout (CSOT) applied when the
	// operation's ctx has no deadline; 0 = none.
	ConnectTimeout         time.Duration
	ServerSelectionTimeout time.Duration
	Timeout                time.Duration

	// Pool. MaxPoolSize default 50 per server (keep replicas × MaxPoolSize
	// within the cluster's connection limit); MinPoolSize default 0;
	// MaxConnIdleTime default 5m (negative = never).
	MaxPoolSize     uint64
	MinPoolSize     uint64
	MaxConnIdleTime time.Duration

	// CommandLog configures logger/mongolog; DisableCommandLog turns it off.
	CommandLog        mongolog.Options
	DisableCommandLog bool

	// Retry controls the startup ping; see database.Retry.
	Retry database.Retry
	// SkipPing returns the client without contacting the server.
	SkipPing bool

	// Configure, if set, can adjust the client options last (read
	// preference, write concern, TLS, compressors, …).
	Configure func(*options.ClientOptions) error
}

// Connect builds the client options, connects and (unless SkipPing) waits
// for the primary to answer. On failure the client is disconnected.
func Connect(ctx context.Context, cfg Config) (*mongo.Client, error) {
	opts, err := clientOptions(cfg)
	if err != nil {
		return nil, err
	}
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb: connect: %w", err)
	}
	if !cfg.SkipPing {
		fields := []zap.Field{
			zap.String(logger.KeyDBSystem, database.SystemMongoDB),
			zap.String(database.KeyHost, strings.Join(opts.Hosts, ",")),
		}
		if err := database.WaitReady(ctx, cfg.Retry, ping(client), fields...); err != nil {
			_ = client.Disconnect(context.WithoutCancel(ctx))
			return nil, fmt.Errorf("mongodb: %w", err)
		}
	}
	return client, nil
}

// ValidateProduction validates the effective client settings without opening
// a socket. It requires TLS with server certificate verification.
func (cfg Config) ValidateProduction() error {
	opts, err := clientOptions(cfg)
	if err != nil {
		return err
	}
	if opts.TLSConfig == nil || opts.TLSConfig.InsecureSkipVerify {
		return errors.New("mongodb: production connections require verified TLS")
	}
	return nil
}

// Check returns a health check that pings the primary.
func Check(name string, client *mongo.Client) database.Check {
	if client == nil {
		return database.Check{Name: name, Ping: func(context.Context) error {
			return errors.New("mongodb: nil client")
		}}
	}
	return database.Check{Name: name, Ping: ping(client)}
}

func ping(client *mongo.Client) func(context.Context) error {
	return func(ctx context.Context) error { return client.Ping(ctx, readpref.Primary()) }
}

func clientOptions(cfg Config) (*options.ClientOptions, error) {
	if cfg.URI == "" {
		return nil, errors.New("mongodb: URI is required")
	}
	opts := options.Client().ApplyURI(cfg.URI)
	if cfg.AppName != "" {
		opts.SetAppName(cfg.AppName)
	}
	opts.SetConnectTimeout(durOr(cfg.ConnectTimeout, DefaultConnectTimeout))
	opts.SetServerSelectionTimeout(durOr(cfg.ServerSelectionTimeout, DefaultServerSelectionTimeout))
	if cfg.Timeout > 0 {
		opts.SetTimeout(cfg.Timeout)
	}

	maxPool := cfg.MaxPoolSize
	if maxPool == 0 {
		maxPool = DefaultMaxPoolSize
	}
	opts.SetMaxPoolSize(maxPool)
	opts.SetMinPoolSize(cfg.MinPoolSize)
	opts.SetMaxConnIdleTime(max(durOr(cfg.MaxConnIdleTime, DefaultMaxConnIdleTime), 0))

	if !cfg.DisableCommandLog {
		opts.SetMonitor(mongolog.NewMonitor(cfg.CommandLog))
	}
	if cfg.Configure != nil {
		if err := cfg.Configure(opts); err != nil {
			return nil, fmt.Errorf("mongodb: configure: %w", err)
		}
	}
	// Validate only after every package override and caller customization has
	// been applied. ClientOptions caches validation results, so validating the
	// URI first would miss invalid final combinations such as minPool > maxPool.
	if err := opts.Validate(); err != nil {
		// Driver errors may quote the URI; keep credentials out of logs.
		return nil, errors.New("mongodb: invalid URI or options")
	}
	return opts, nil
}

func durOr(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}
