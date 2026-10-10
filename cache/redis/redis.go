package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/cache"
	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/internal/rediscore"
	"github.com/go-dto/go-common/logger"
)

const (
	System                  = "redis"
	DefaultTTL              = 5 * time.Minute
	DefaultOperationTimeout = 2 * time.Second
	DefaultSlowThreshold    = 200 * time.Millisecond
	DefaultMaxKeyBytes      = 1024
	DefaultMaxValueBytes    = 1 << 20
	DefaultMaxDeleteKeys    = 100
)

// Config configures a Redis cache Store. Addresses are host:port values;
// credentials are separate so they never appear in parse errors.
type Config struct {
	Addrs            []string
	ClientName       string
	Username         string
	Password         string
	SentinelUsername string
	SentinelPassword string
	DB               int
	MasterName       string
	TLSConfig        *tls.Config

	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolTimeout     time.Duration
	PoolSize        int
	MinIdleConns    int
	MaxActiveConns  int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	StartupAttempts int
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	PingTimeout     time.Duration
	SkipPing        bool

	KeyPrefix        string
	DefaultTTL       time.Duration
	OperationTimeout time.Duration
	SlowThreshold    time.Duration
	MaxKeyBytes      int
	MaxValueBytes    int
	MaxDeleteKeys    int
	DisableLogging   bool

	// Client is for controlled tests/dev only and is never closed by Store.
	Client goredis.UniversalClient
}

// Store is safe for concurrent use.
type Store struct {
	handle *rediscore.Handle
	client goredis.UniversalClient
	cfg    Config
	closed atomic.Bool
}

var _ cache.Store = (*Store)(nil)

// ValidateProduction enforces verified TLS, startup ping, bounded operations,
// a namespace prefix, and a package-owned client.
func (cfg Config) ValidateProduction() error {
	normalized, err := normalize(cfg)
	if err != nil {
		return err
	}
	if normalized.KeyPrefix == "" {
		return errors.New("cache/redis: production KeyPrefix is required")
	}
	return rediscore.ValidateProduction(normalized.core())
}

// New opens and startup-pings a Redis cache.
func New(ctx context.Context, cfg Config) (*Store, error) {
	normalized, err := normalize(cfg)
	if err != nil {
		return nil, cache.NewError("new", cache.KindInvalid, err)
	}
	handle, err := rediscore.Open(ctx, normalized.core())
	if err != nil {
		return nil, cache.NewError("new", kind(err), err)
	}
	normalized.Password = ""
	normalized.SentinelPassword = ""
	normalized.Addrs = nil
	normalized.ClientName = ""
	normalized.Username = ""
	normalized.SentinelUsername = ""
	normalized.MasterName = ""
	normalized.TLSConfig = nil
	normalized.Client = nil
	return &Store{handle: handle, client: handle.Client(), cfg: normalized}, nil
}

func normalize(cfg Config) (Config, error) {
	if cfg.DefaultTTL == 0 {
		cfg.DefaultTTL = DefaultTTL
	}
	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = DefaultOperationTimeout
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = DefaultSlowThreshold
	}
	if cfg.MaxKeyBytes == 0 {
		cfg.MaxKeyBytes = DefaultMaxKeyBytes
	}
	if cfg.MaxValueBytes == 0 {
		cfg.MaxValueBytes = DefaultMaxValueBytes
	}
	if cfg.MaxDeleteKeys == 0 {
		cfg.MaxDeleteKeys = DefaultMaxDeleteKeys
	}
	if cfg.DefaultTTL <= 0 || cfg.OperationTimeout <= 0 || cfg.SlowThreshold < 0 || cfg.MaxKeyBytes <= 0 || cfg.MaxValueBytes <= 0 || cfg.MaxDeleteKeys <= 0 {
		return Config{}, errors.New("cache/redis: TTL, timeout, and size bounds are invalid")
	}
	if len(cfg.KeyPrefix) > 128 || !utf8.ValidString(cfg.KeyPrefix) || strings.ContainsAny(cfg.KeyPrefix, "\r\n\x00") {
		return Config{}, errors.New("cache/redis: invalid KeyPrefix")
	}
	if err := rediscore.Validate(cfg.core()); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (cfg Config) core() rediscore.Config {
	return rediscore.Config{
		Addrs: cfg.Addrs, ClientName: cfg.ClientName, Username: cfg.Username, Password: cfg.Password,
		SentinelUsername: cfg.SentinelUsername, SentinelPassword: cfg.SentinelPassword,
		DB: cfg.DB, MasterName: cfg.MasterName, TLSConfig: cfg.TLSConfig,
		DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout,
		PoolTimeout: cfg.PoolTimeout, PoolSize: cfg.PoolSize, MinIdleConns: cfg.MinIdleConns,
		MaxActiveConns: cfg.MaxActiveConns, ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifetime, MaxRetries: cfg.MaxRetries,
		MinRetryBackoff: cfg.MinRetryBackoff, MaxRetryBackoff: cfg.MaxRetryBackoff,
		StartupAttempts: cfg.StartupAttempts, InitialBackoff: cfg.InitialBackoff,
		MaxBackoff: cfg.MaxBackoff, PingTimeout: cfg.PingTimeout, SkipPing: cfg.SkipPing,
		Client: cfg.Client, Role: "cache",
	}
}

// Get returns a copy of the cached value.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel, err := s.operationContext(ctx, "get", key)
	if err != nil {
		return nil, err
	}
	defer cancel()
	started := time.Now()
	value, rawErr := s.client.Get(ctx, s.cfg.KeyPrefix+key).Bytes()
	if errors.Is(rawErr, goredis.Nil) {
		s.observe(ctx, "get", "miss", time.Since(started), nil)
		return nil, cache.ErrMiss
	}
	if rawErr != nil {
		err = cache.NewError("get", kind(rawErr), rediscore.SafeCause(rawErr))
		s.observe(ctx, "get", "error", time.Since(started), err)
		return nil, err
	}
	if len(value) > s.cfg.MaxValueBytes {
		err = cache.NewError("get", cache.KindTooLarge, nil)
		s.observe(ctx, "get", "too_large", time.Since(started), err)
		return nil, err
	}
	s.observe(ctx, "get", "hit", time.Since(started), nil)
	return slices.Clone(value), nil
}

// Set stores value with ttl. Zero uses Config.DefaultTTL; negative is invalid.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	ctx, cancel, err := s.operationContext(ctx, "set", key)
	if err != nil {
		return err
	}
	defer cancel()
	if len(value) > s.cfg.MaxValueBytes {
		return cache.NewError("set", cache.KindTooLarge, nil)
	}
	if ttl == 0 {
		ttl = s.cfg.DefaultTTL
	}
	if ttl < 0 {
		return cache.NewError("set", cache.KindInvalid, errors.New("TTL must not be negative"))
	}
	started := time.Now()
	rawErr := s.client.Set(ctx, s.cfg.KeyPrefix+key, value, ttl).Err()
	if rawErr != nil {
		err = cache.NewError("set", kind(rawErr), rediscore.SafeCause(rawErr))
		s.observe(ctx, "set", "error", time.Since(started), err)
		return err
	}
	s.observe(ctx, "set", "stored", time.Since(started), nil)
	return nil
}

// Delete removes keys. Missing keys are ignored.
func (s *Store) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if s == nil || s.closed.Load() {
		return cache.ErrClosed
	}
	if len(keys) > s.cfg.MaxDeleteKeys {
		return cache.NewError("delete", cache.KindTooLarge, errors.New("too many cache keys"))
	}
	full := make([]string, len(keys))
	for i, key := range keys {
		if err := validateKey(key, s.cfg.MaxKeyBytes, "delete"); err != nil {
			return err
		}
		full[i] = s.cfg.KeyPrefix + key
	}
	ctx, cancel, err := boundedContext(ctx, s.cfg.OperationTimeout, "delete")
	if err != nil {
		return err
	}
	defer cancel()
	started := time.Now()
	rawErr := s.client.Del(ctx, full...).Err()
	if rawErr != nil {
		err = cache.NewError("delete", kind(rawErr), rediscore.SafeCause(rawErr))
		s.observe(ctx, "delete", "error", time.Since(started), err)
		return err
	}
	s.observe(ctx, "delete", "deleted", time.Since(started), nil)
	return nil
}

// Ping checks readiness without exposing backend details.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.closed.Load() {
		return cache.ErrClosed
	}
	ctx, cancel, err := boundedContext(ctx, s.cfg.OperationTimeout, "ping")
	if err != nil {
		return err
	}
	defer cancel()
	if err := s.handle.Ping(ctx); err != nil {
		return cache.NewError("ping", kind(err), err)
	}
	return nil
}

// Check returns a readiness check compatible with database.HealthHandler.
func Check(name string, store *Store) database.Check {
	if store == nil {
		return database.Check{Name: name, Ping: func(context.Context) error { return cache.ErrClosed }}
	}
	return database.Check{Name: name, Ping: store.Ping}
}

// Close closes the package-owned pool and is idempotent.
func (s *Store) Close() error {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if err := s.handle.Close(); err != nil {
		return cache.NewError("close", kind(err), rediscore.SafeCause(err))
	}
	return nil
}

func (s *Store) operationContext(ctx context.Context, operation, key string) (context.Context, context.CancelFunc, error) {
	if s == nil || s.closed.Load() {
		return nil, nil, cache.ErrClosed
	}
	if err := validateKey(key, s.cfg.MaxKeyBytes, operation); err != nil {
		return nil, nil, err
	}
	return boundedContext(ctx, s.cfg.OperationTimeout, operation)
}

func boundedContext(ctx context.Context, timeout time.Duration, operation string) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, cache.NewError(operation, cache.KindInvalid, errors.New("nil context"))
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, cache.NewError(operation, cache.KindOf(err), err)
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	return bounded, cancel, nil
}

func validateKey(key string, max int, operation string) error {
	if key == "" || len(key) > max || !utf8.ValidString(key) || strings.ContainsAny(key, "\r\n\x00") {
		return cache.NewError(operation, cache.KindInvalid, errors.New("invalid cache key"))
	}
	return nil
}

func kind(err error) cache.ErrorKind {
	switch rediscore.ErrorKind(err) {
	case "canceled":
		return cache.KindCanceled
	case "timeout":
		return cache.KindTimeout
	case "closed":
		return cache.KindClosed
	default:
		return cache.KindUnavailable
	}
}

func (s *Store) observe(ctx context.Context, operation, outcome string, duration time.Duration, err error) {
	if s.cfg.DisableLogging {
		return
	}
	fields := []zap.Field{
		zap.String("cache_system", System), zap.String("cache_operation", operation),
		zap.String("outcome", outcome), zap.Float64(logger.KeyDurationMs, float64(duration)/float64(time.Millisecond)),
	}
	l := logger.FromContext(ctx)
	switch {
	case err != nil:
		l.Warn("cache operation failed", append(fields, zap.String("error_kind", string(cache.KindOf(err))))...)
	case s.cfg.SlowThreshold > 0 && duration >= s.cfg.SlowThreshold:
		l.Warn("cache operation slow", fields...)
	default:
		l.Debug("cache operation", fields...)
	}
}
