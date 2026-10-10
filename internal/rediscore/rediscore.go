// Package rediscore contains shared connection hardening for the public Redis
// cache and broker drivers. It is internal so consumers use the purpose-built
// APIs instead of sharing one client across different workloads.
package rediscore

import (
	"context"
	"crypto/tls"
	"errors"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/logger"
)

const (
	DefaultDialTimeout     = 5 * time.Second
	DefaultReadTimeout     = 5 * time.Second
	DefaultWriteTimeout    = 5 * time.Second
	DefaultPoolTimeout     = 5 * time.Second
	DefaultPoolSize        = 20
	DefaultMinIdleConns    = 2
	DefaultConnMaxIdleTime = 5 * time.Minute
	DefaultConnMaxLifetime = 30 * time.Minute
	DefaultStartupAttempts = 5
	DefaultInitialBackoff  = 500 * time.Millisecond
	DefaultMaxBackoff      = 5 * time.Second
	DefaultPingTimeout     = 5 * time.Second
	DefaultMaxRetries      = 2
	DefaultMinRetryBackoff = 50 * time.Millisecond
	DefaultMaxRetryBackoff = 500 * time.Millisecond
	defaultConnectionRole  = "redis"
)

var (
	ErrUnavailable = errors.New("redis: unavailable")
	ErrClosed      = errors.New("redis: closed")
)

// Config is internal input copied from each purpose-built public config.
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

	Client goredis.UniversalClient
	Role   string
}

// Handle owns a Redis client unless it was injected in Config.
type Handle struct {
	client goredis.UniversalClient
	owns   bool
	once   sync.Once
	err    error
}

func Validate(cfg Config) error {
	_, err := normalize(cfg)
	return err
}

func ValidateProduction(cfg Config) error {
	normalized, err := normalize(cfg)
	if err != nil {
		return err
	}
	if normalized.Client != nil {
		return errors.New("redis: production validation cannot verify an injected client")
	}
	if normalized.SkipPing {
		return errors.New("redis: production startup ping must be enabled")
	}
	if normalized.TLSConfig == nil || normalized.TLSConfig.InsecureSkipVerify {
		return errors.New("redis: production connections require verified TLS")
	}
	if normalized.TLSConfig.MinVersion < tls.VersionTLS12 {
		return errors.New("redis: production connections require TLS 1.2 or newer")
	}
	if normalized.TLSConfig.MaxVersion != 0 && normalized.TLSConfig.MaxVersion < normalized.TLSConfig.MinVersion {
		return errors.New("redis: TLS maximum version is lower than the minimum")
	}
	return nil
}

// Open creates and startup-pings a bounded Redis client.
func Open(ctx context.Context, cfg Config) (*Handle, error) {
	if ctx == nil {
		return nil, errors.New("redis: nil context")
	}
	normalized, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	client, owns := normalized.Client, false
	if client == nil {
		client = goredis.NewUniversalClient(&goredis.UniversalOptions{
			Addrs:                 normalized.Addrs,
			ClientName:            normalized.ClientName,
			DB:                    normalized.DB,
			Username:              normalized.Username,
			Password:              normalized.Password,
			SentinelUsername:      normalized.SentinelUsername,
			SentinelPassword:      normalized.SentinelPassword,
			MasterName:            normalized.MasterName,
			TLSConfig:             normalized.TLSConfig,
			DialTimeout:           normalized.DialTimeout,
			ReadTimeout:           normalized.ReadTimeout,
			WriteTimeout:          normalized.WriteTimeout,
			ContextTimeoutEnabled: true,
			PoolSize:              normalized.PoolSize,
			MaxActiveConns:        normalized.MaxActiveConns,
			MinIdleConns:          normalized.MinIdleConns,
			PoolTimeout:           normalized.PoolTimeout,
			ConnMaxIdleTime:       normalized.ConnMaxIdleTime,
			ConnMaxLifetime:       normalized.ConnMaxLifetime,
			MaxRetries:            normalized.MaxRetries,
			MinRetryBackoff:       normalized.MinRetryBackoff,
			MaxRetryBackoff:       normalized.MaxRetryBackoff,
		})
		owns = true
	}
	h := &Handle{client: client, owns: owns}
	if normalized.SkipPing {
		return h, nil
	}
	if err := waitReady(ctx, client, normalized); err != nil {
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

func (h *Handle) Client() goredis.UniversalClient { return h.client }

func (h *Handle) Ping(ctx context.Context) error {
	if h == nil || h.client == nil {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("redis: nil context")
	}
	if err := h.client.Ping(ctx).Err(); err != nil {
		return SafeCause(err)
	}
	return nil
}

// Close is idempotent. Injected clients remain owned by their caller.
func (h *Handle) Close() error {
	if h == nil {
		return nil
	}
	h.once.Do(func() {
		if h.owns && h.client != nil {
			h.err = h.client.Close()
		}
	})
	return h.err
}

func normalize(cfg Config) (Config, error) {
	cfg.Addrs = slices.Clone(cfg.Addrs)
	if cfg.Client == nil && len(cfg.Addrs) == 0 {
		return Config{}, errors.New("redis: at least one address is required")
	}
	for _, addr := range cfg.Addrs {
		if strings.ContainsAny(addr, "\r\n\t") {
			return Config{}, errors.New("redis: address contains control characters")
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
			return Config{}, errors.New("redis: address must use host:port format")
		}
	}
	if cfg.DB < 0 {
		return Config{}, errors.New("redis: DB must not be negative")
	}
	if len(cfg.Addrs) > 1 && cfg.MasterName == "" && cfg.DB != 0 {
		return Config{}, errors.New("redis: cluster mode requires DB zero")
	}
	if strings.ContainsAny(cfg.ClientName+cfg.MasterName, "\r\n\t") {
		return Config{}, errors.New("redis: client or master name contains control characters")
	}
	if cfg.TLSConfig != nil {
		cfg.TLSConfig = cfg.TLSConfig.Clone()
		if cfg.TLSConfig.MinVersion == 0 {
			cfg.TLSConfig.MinVersion = tls.VersionTLS12
		}
	}
	defaultDuration(&cfg.DialTimeout, DefaultDialTimeout)
	defaultDuration(&cfg.ReadTimeout, DefaultReadTimeout)
	defaultDuration(&cfg.WriteTimeout, DefaultWriteTimeout)
	defaultDuration(&cfg.PoolTimeout, DefaultPoolTimeout)
	defaultDuration(&cfg.ConnMaxIdleTime, DefaultConnMaxIdleTime)
	defaultDuration(&cfg.ConnMaxLifetime, DefaultConnMaxLifetime)
	defaultDuration(&cfg.MinRetryBackoff, DefaultMinRetryBackoff)
	defaultDuration(&cfg.MaxRetryBackoff, DefaultMaxRetryBackoff)
	defaultDuration(&cfg.InitialBackoff, DefaultInitialBackoff)
	defaultDuration(&cfg.MaxBackoff, DefaultMaxBackoff)
	defaultDuration(&cfg.PingTimeout, DefaultPingTimeout)
	if cfg.PoolSize == 0 {
		cfg.PoolSize = DefaultPoolSize
	}
	if cfg.MinIdleConns == 0 {
		cfg.MinIdleConns = min(DefaultMinIdleConns, cfg.PoolSize)
	}
	if cfg.MaxActiveConns == 0 {
		cfg.MaxActiveConns = cfg.PoolSize
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.StartupAttempts == 0 {
		cfg.StartupAttempts = DefaultStartupAttempts
	}
	if cfg.Role == "" {
		cfg.Role = defaultConnectionRole
	}
	if cfg.DialTimeout <= 0 || cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 || cfg.PoolTimeout <= 0 {
		return Config{}, errors.New("redis: network and pool timeouts must be positive")
	}
	if cfg.ConnMaxIdleTime <= 0 || cfg.ConnMaxLifetime <= 0 {
		return Config{}, errors.New("redis: connection lifetime bounds must be positive")
	}
	if cfg.PoolSize <= 0 || cfg.MinIdleConns < 0 || cfg.MinIdleConns > cfg.PoolSize || cfg.MaxActiveConns < cfg.PoolSize {
		return Config{}, errors.New("redis: invalid connection pool bounds")
	}
	if cfg.MaxRetries < 0 || cfg.MinRetryBackoff <= 0 || cfg.MaxRetryBackoff < cfg.MinRetryBackoff {
		return Config{}, errors.New("redis: invalid command retry policy")
	}
	if cfg.StartupAttempts <= 0 || cfg.InitialBackoff <= 0 || cfg.MaxBackoff < cfg.InitialBackoff || cfg.PingTimeout <= 0 {
		return Config{}, errors.New("redis: invalid startup retry policy")
	}
	return cfg, nil
}

func defaultDuration(value *time.Duration, fallback time.Duration) {
	if *value == 0 {
		*value = fallback
	}
}

func waitReady(ctx context.Context, client goredis.UniversalClient, cfg Config) error {
	l := logger.FromContext(ctx).With(zap.String("redis_role", cfg.Role))
	started := time.Now()
	backoff := cfg.InitialBackoff
	var last error
	for attempt := 1; attempt <= cfg.StartupAttempts; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, cfg.PingTimeout)
		last = client.Ping(pingCtx).Err()
		cancel()
		if last == nil {
			l.Info("redis connected", zap.Int("attempt", attempt),
				zap.Float64(logger.KeyDurationMs, float64(time.Since(started))/float64(time.Millisecond)))
			return nil
		}
		if attempt == cfg.StartupAttempts || ctx.Err() != nil {
			break
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		l.Warn("redis not ready, retrying", zap.Int("attempt", attempt),
			zap.Duration("backoff", wait), zap.String("error_kind", ErrorKind(last)))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			last = ctx.Err()
			attempt = cfg.StartupAttempts
		case <-timer.C:
		}
		if backoff >= cfg.MaxBackoff-backoff {
			backoff = cfg.MaxBackoff
		} else {
			backoff *= 2
		}
	}
	l.Error("redis unreachable", zap.String("error_kind", ErrorKind(last)))
	return SafeCause(last)
}

// ErrorKind returns a stable classification without backend details.
func ErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, goredis.ErrClosed), errors.Is(err, ErrClosed):
		return "closed"
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "timeout"
		}
		return "unavailable"
	}
}

// SafeCause maps raw backend failures to stable causes whose Error messages do
// not expose addresses or credentials.
func SafeCause(err error) error {
	switch ErrorKind(err) {
	case "canceled":
		return context.Canceled
	case "timeout":
		return context.DeadlineExceeded
	case "closed":
		return ErrClosed
	case "":
		return nil
	default:
		return ErrUnavailable
	}
}
