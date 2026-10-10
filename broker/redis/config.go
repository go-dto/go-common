package redis

import (
	"crypto/tls"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	goredis "github.com/redis/go-redis/v9"

	"github.com/go-dto/go-common/broker"
	"github.com/go-dto/go-common/internal/rediscore"
)

const (
	System = "redis_streams"

	DefaultConcurrency         = 10
	DefaultReadBatchSize       = 10
	DefaultBlockTimeout        = time.Second
	DefaultClaimMinIdle        = time.Minute
	DefaultMaxDeliveryAttempts = 5
	DefaultMaxMessageBytes     = 1 << 20
	DefaultMaxAttributes       = 32
	DefaultMaxAttributeBytes   = 4096
	DefaultStreamMaxLen        = 100_000
	DefaultDeadLetterMaxLen    = 10_000
	DefaultReceiveRetryInitial = 250 * time.Millisecond
	DefaultReceiveRetryMax     = 5 * time.Second
)

// Subscription maps the broker-neutral subscription name to one topic stream
// and one Redis consumer group. Group defaults to the subscription name.
type Subscription struct {
	Topic string
	Group string
}

// Config configures a dedicated Redis Streams driver. Cache and queue clients
// must use different Config instances and pools.
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

	// KeyPrefix namespaces every stream. Production requires a non-empty value.
	// It must not contain braces because topic hash tags keep source and DLQ
	// streams in the same Redis Cluster slot.
	KeyPrefix string

	// Topics is the allowlist accepted by Publish.
	Topics []string
	// Subscriptions is the allowlist accepted by Receive.
	Subscriptions map[string]Subscription
	// ConsumerName must be unique per running process/pod when subscriptions are
	// configured. A pod name plus process identity is a suitable value.
	ConsumerName string

	Concurrency         int
	ReadBatchSize       int64
	BlockTimeout        time.Duration
	ClaimMinIdle        time.Duration
	MaxDeliveryAttempts int64
	MaxMessageBytes     int
	MaxAttributes       int
	MaxAttributeBytes   int
	StreamMaxLen        int64
	DeadLetterMaxLen    int64

	// ReceiveRetryInitialBackoff and ReceiveRetryMaxBackoff bound reconnect
	// backoff after transient XREADGROUP/XAUTOCLAIM failures. They are separate
	// from the per-command retry configuration because Receive is long-lived.
	ReceiveRetryInitialBackoff time.Duration
	ReceiveRetryMaxBackoff     time.Duration

	// AutoCreateGroups is only for tests/dev. Production provisions streams and
	// groups through IaC and ValidateProduction rejects this option.
	AutoCreateGroups bool

	// Client is for controlled tests/dev only and is never closed by Driver.
	Client goredis.UniversalClient
}

// ValidateProduction enforces verified TLS, a package-owned client, explicit
// stream provisioning, bounded memory, and stable resource names.
func (cfg Config) ValidateProduction() error {
	if cfg.StreamMaxLen <= 0 || cfg.DeadLetterMaxLen <= 0 {
		return errors.New("broker/redis: production stream length bounds must be configured explicitly")
	}
	normalized, err := normalize(cfg)
	if err != nil {
		return err
	}
	if normalized.KeyPrefix == "" {
		return errors.New("broker/redis: production KeyPrefix is required")
	}
	if normalized.AutoCreateGroups {
		return errors.New("broker/redis: production groups must be provisioned outside the service")
	}
	return rediscore.ValidateProduction(normalized.core())
}

// ValidateProductionWithOptions validates both the Redis deployment config
// and broker runtime options. Production consumers require a positive handler
// timeout so ClaimMinIdle can be proven safe against concurrent reclaim.
func (cfg Config) ValidateProductionWithOptions(opts broker.Options) error {
	if err := cfg.ValidateProduction(); err != nil {
		return err
	}
	normalized, err := normalize(cfg)
	if err != nil {
		return err
	}
	if opts.HandlerTimeout <= 0 {
		return errors.New("broker/redis: production HandlerTimeout must be positive")
	}
	if normalized.ClaimMinIdle <= opts.HandlerTimeout {
		return errors.New("broker/redis: ClaimMinIdle must exceed HandlerTimeout")
	}
	return nil
}

func normalize(cfg Config) (Config, error) {
	if cfg.Concurrency == 0 {
		cfg.Concurrency = DefaultConcurrency
	}
	if cfg.ReadBatchSize == 0 {
		cfg.ReadBatchSize = DefaultReadBatchSize
	}
	if cfg.BlockTimeout == 0 {
		cfg.BlockTimeout = DefaultBlockTimeout
	}
	if cfg.ClaimMinIdle == 0 {
		cfg.ClaimMinIdle = DefaultClaimMinIdle
	}
	if cfg.MaxDeliveryAttempts == 0 {
		cfg.MaxDeliveryAttempts = DefaultMaxDeliveryAttempts
	}
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if cfg.MaxAttributes == 0 {
		cfg.MaxAttributes = DefaultMaxAttributes
	}
	if cfg.MaxAttributeBytes == 0 {
		cfg.MaxAttributeBytes = DefaultMaxAttributeBytes
	}
	if cfg.StreamMaxLen == 0 {
		cfg.StreamMaxLen = DefaultStreamMaxLen
	}
	if cfg.DeadLetterMaxLen == 0 {
		cfg.DeadLetterMaxLen = DefaultDeadLetterMaxLen
	}
	if cfg.ReceiveRetryInitialBackoff == 0 {
		cfg.ReceiveRetryInitialBackoff = DefaultReceiveRetryInitial
	}
	if cfg.ReceiveRetryMaxBackoff == 0 {
		cfg.ReceiveRetryMaxBackoff = DefaultReceiveRetryMax
	}
	if cfg.Concurrency <= 0 || cfg.ReadBatchSize <= 0 || cfg.ReadBatchSize > int64(cfg.Concurrency) {
		return Config{}, errors.New("broker/redis: invalid concurrency or batch size")
	}
	if cfg.BlockTimeout <= 0 || cfg.ClaimMinIdle <= 0 {
		return Config{}, errors.New("broker/redis: block and claim timeouts must be positive")
	}
	effectiveReadTimeout := cfg.ReadTimeout
	if effectiveReadTimeout == 0 {
		effectiveReadTimeout = rediscore.DefaultReadTimeout
	}
	if effectiveReadTimeout <= cfg.BlockTimeout {
		return Config{}, errors.New("broker/redis: ReadTimeout must be greater than BlockTimeout")
	}
	if cfg.MaxDeliveryAttempts <= 0 || cfg.MaxMessageBytes <= 0 || cfg.MaxAttributes <= 0 || cfg.MaxAttributeBytes <= 0 {
		return Config{}, errors.New("broker/redis: message and delivery bounds must be positive")
	}
	if cfg.StreamMaxLen <= 0 || cfg.DeadLetterMaxLen <= 0 {
		return Config{}, errors.New("broker/redis: stream length bounds must be positive")
	}
	if cfg.ReceiveRetryInitialBackoff <= 0 || cfg.ReceiveRetryMaxBackoff < cfg.ReceiveRetryInitialBackoff {
		return Config{}, errors.New("broker/redis: invalid receive retry backoff")
	}
	if len(cfg.KeyPrefix) > 128 || !utf8.ValidString(cfg.KeyPrefix) || strings.ContainsAny(cfg.KeyPrefix, "{}\r\n\x00") {
		return Config{}, errors.New("broker/redis: invalid KeyPrefix")
	}
	if err := validateResource(cfg.ConsumerName, true); err != nil {
		return Config{}, errors.New("broker/redis: invalid ConsumerName")
	}
	if len(cfg.Subscriptions) > 0 && cfg.ConsumerName == "" {
		return Config{}, errors.New("broker/redis: ConsumerName is required with subscriptions")
	}

	cfg.Topics = slices.Clone(cfg.Topics)
	topics := make(map[string]struct{}, len(cfg.Topics))
	for _, topic := range cfg.Topics {
		if err := validateResource(topic, false); err != nil {
			return Config{}, errors.New("broker/redis: invalid topic")
		}
		if _, duplicate := topics[topic]; duplicate {
			return Config{}, errors.New("broker/redis: duplicate topic")
		}
		topics[topic] = struct{}{}
	}
	if len(topics) == 0 {
		return Config{}, errors.New("broker/redis: at least one topic is required")
	}

	cfg.Subscriptions = maps.Clone(cfg.Subscriptions)
	for name, sub := range cfg.Subscriptions {
		if err := validateResource(name, false); err != nil {
			return Config{}, errors.New("broker/redis: invalid subscription")
		}
		if _, ok := topics[sub.Topic]; !ok {
			return Config{}, errors.New("broker/redis: subscription references an unknown topic")
		}
		if sub.Group == "" {
			sub.Group = name
		}
		if err := validateResource(sub.Group, false); err != nil {
			return Config{}, errors.New("broker/redis: invalid consumer group")
		}
		cfg.Subscriptions[name] = sub
	}
	if err := rediscore.Validate(cfg.core()); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateResource(value string, allowEmpty bool) error {
	if value == "" && allowEmpty {
		return nil
	}
	if value == "" || len(value) > 128 || !utf8.ValidString(value) || strings.ContainsAny(value, "{}\r\n\t\x00") {
		return errors.New("invalid resource name")
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("._:-", r) {
			continue
		}
		return errors.New("invalid resource name")
	}
	return nil
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
		Client: cfg.Client, Role: "queue",
	}
}

func (cfg Config) streamKey(topic string) string {
	return cfg.KeyPrefix + "{topic:" + topic + "}:stream"
}

func (cfg Config) deadLetterKey(subscription string, sub Subscription) string {
	return cfg.KeyPrefix + "{topic:" + sub.Topic + "}:dlq:" + subscription
}
