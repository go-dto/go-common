package httpclient

import (
	"context"
	"crypto/tls"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Defaults applied by New when the matching Config field is zero.
const (
	DefaultTimeout               = 30 * time.Second
	DefaultMaxRetries            = 2
	DefaultInitialBackoff        = 100 * time.Millisecond
	DefaultMaxBackoff            = 2 * time.Second
	DefaultDialTimeout           = 5 * time.Second
	DefaultTLSHandshakeTimeout   = 5 * time.Second
	DefaultResponseHeaderTimeout = 10 * time.Second
	DefaultMaxIdleConnsPerHost   = 10
	DefaultMaxResponseBodyBytes  = int64(4 << 20) // 4 MiB
)

var defaultRetryStatuses = []int{
	http.StatusTooManyRequests,
	http.StatusBadGateway,
	http.StatusServiceUnavailable,
	http.StatusGatewayTimeout,
}

// Config configures a Client. Peer and BaseURL are required. The zero values of
// timeout, retry and connection-pool fields use production-safe defaults.
type Config struct {
	Peer    string
	BaseURL string

	DefaultHeaders HeaderMap
	Auth           Authenticator

	// MaxResponseBodyBytes bounds buffered responses. Zero uses 4 MiB.
	MaxResponseBodyBytes int64

	// Timeout is the total budget for one call across every attempt, backoff and
	// response-body read. A negative value disables it; prefer request contexts.
	Timeout time.Duration

	// PerAttemptTimeout applies through response-body reading, but a body read
	// that times out after headers were returned cannot be retried safely.
	PerAttemptTimeout time.Duration

	MaxRetries     int
	DisableRetry   bool
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	RetryStatuses  []int

	// RetryNonIdempotent permits retries without an idempotency key. Keep false
	// unless the upstream operation is known to be safe to repeat.
	RetryNonIdempotent bool

	// OnRetry runs synchronously before a retry wait. It must be fast.
	OnRetry func(RetryEvent)
	// OnResult receives one final, sanitized event per logical request. It runs
	// synchronously and must be fast and safe for concurrent use.
	OnResult func(context.Context, ResultEvent)

	// Advanced transport settings. Most application code should leave these at
	// zero and never needs to import net/http.
	Transport             http.RoundTripper
	DialTimeout           time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	// FollowRedirects permits redirects only to the same scheme and host.
	// Redirects are returned as normal 3xx responses by default.
	FollowRedirects bool
	MaxRedirects    int
	TLSClientConfig *tls.Config
	Proxy           func(*http.Request) (*url.URL, error)

	DisableLogging     bool
	DisablePropagation bool
}

// Options is kept as an alias for pre-v1 configuration code.
// Deprecated: use Config.
type Options = Config

func (c Config) normalized() (Config, *url.URL, error) {
	c.Peer = strings.TrimSpace(c.Peer)
	if c.Peer == "" || len(c.Peer) > 128 || strings.ContainsAny(c.Peer, "\r\n\t") {
		return Config{}, nil, configError("Peer must be 1-128 characters without control characters")
	}

	baseURL, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return Config{}, nil, configError("BaseURL must be an absolute HTTP(S) URL")
	}
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return Config{}, nil, configError("BaseURL scheme must be http or https")
	}
	if baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return Config{}, nil, configError("BaseURL must not contain credentials, query or fragment")
	}

	for name, value := range c.DefaultHeaders {
		if err := validateHeader(name, value); err != nil {
			return Config{}, nil, configError("invalid default header: " + err.Error())
		}
	}
	if c.Auth != nil {
		if err := c.Auth.validate(); err != nil {
			return Config{}, nil, configError("invalid auth: " + err.Error())
		}
	}
	if c.MaxResponseBodyBytes < 0 {
		return Config{}, nil, configError("MaxResponseBodyBytes must not be negative")
	}
	if c.MaxResponseBodyBytes >= math.MaxInt64 {
		return Config{}, nil, configError("MaxResponseBodyBytes is too large")
	}
	if c.PerAttemptTimeout < 0 {
		return Config{}, nil, configError("PerAttemptTimeout must not be negative")
	}
	if c.MaxConnsPerHost < 0 {
		return Config{}, nil, configError("MaxConnsPerHost must not be negative")
	}
	if c.MaxRedirects < 0 {
		return Config{}, nil, configError("MaxRedirects must not be negative")
	}
	if c.MaxRetries < 0 {
		return Config{}, nil, configError("MaxRetries must not be negative")
	}
	if c.InitialBackoff < 0 || c.MaxBackoff < 0 {
		return Config{}, nil, configError("retry backoff must not be negative")
	}
	if c.DialTimeout < 0 || c.TLSHandshakeTimeout < 0 {
		return Config{}, nil, configError("dial and TLS handshake timeouts must not be negative")
	}
	if c.MaxIdleConnsPerHost < 0 {
		return Config{}, nil, configError("MaxIdleConnsPerHost must not be negative")
	}
	for _, statusCode := range c.RetryStatuses {
		if statusCode < 100 || statusCode > 599 {
			return Config{}, nil, configError("RetryStatuses contains an invalid HTTP status")
		}
	}

	c = c.withDefaults()
	return c, cloneURL(baseURL), nil
}

func (c Config) withDefaults() Config {
	if c.Timeout == 0 {
		c.Timeout = DefaultTimeout
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = DefaultMaxRetries
	}
	if c.DisableRetry {
		c.MaxRetries = 0
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = DefaultInitialBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = DefaultMaxBackoff
	}
	if c.MaxBackoff < c.InitialBackoff {
		c.MaxBackoff = c.InitialBackoff
	}
	if c.RetryStatuses == nil {
		c.RetryStatuses = append([]int(nil), defaultRetryStatuses...)
	} else {
		c.RetryStatuses = append([]int(nil), c.RetryStatuses...)
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.TLSHandshakeTimeout <= 0 {
		c.TLSHandshakeTimeout = DefaultTLSHandshakeTimeout
	}
	if c.ResponseHeaderTimeout == 0 {
		c.ResponseHeaderTimeout = DefaultResponseHeaderTimeout
	}
	if c.MaxIdleConnsPerHost <= 0 {
		c.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}
	if c.MaxResponseBodyBytes == 0 {
		c.MaxResponseBodyBytes = DefaultMaxResponseBodyBytes
	}
	if c.FollowRedirects && c.MaxRedirects == 0 {
		c.MaxRedirects = 10
	}
	c.DefaultHeaders = c.DefaultHeaders.clone()
	if c.TLSClientConfig != nil {
		c.TLSClientConfig = c.TLSClientConfig.Clone()
	}
	return c
}

func configError(message string) error {
	return &Error{Kind: KindInvalidConfig, message: message}
}

func cloneURL(in *url.URL) *url.URL {
	if in == nil {
		return nil
	}
	copy := *in
	return &copy
}
