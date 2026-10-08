package httpclient

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/go-dto/go-common/logger"
)

// Defaults applied by New when the matching Options field is zero.
const (
	DefaultTimeout             = 30 * time.Second
	DefaultMaxRetries          = 2
	DefaultInitialBackoff      = 100 * time.Millisecond
	DefaultMaxBackoff          = 2 * time.Second
	DefaultDialTimeout         = 5 * time.Second
	DefaultTLSHandshakeTimeout = 5 * time.Second
	// DefaultResponseHeaderTimeout keeps a hung peer from eating the whole
	// Timeout in one attempt: the attempt fails and can be retried.
	DefaultResponseHeaderTimeout = 10 * time.Second
	DefaultMaxIdleConnsPerHost   = 10
)

// DefaultRetryStatuses are the response codes retried when Options.RetryStatuses is nil.
var DefaultRetryStatuses = []int{
	http.StatusTooManyRequests,     // 429
	http.StatusInternalServerError, // 500
	http.StatusBadGateway,          // 502
	http.StatusServiceUnavailable,  // 503
	http.StatusGatewayTimeout,      // 504
}

// Options configures New. The zero value is usable: every zero field falls
// back to the matching Default* value.
type Options struct {
	// Peer is a logical name for the third party (e.g. "client_webhook");
	// logged as "peer" on every outbound line.
	Peer string

	// Timeout is the total budget for one Do call, across all attempts and
	// backoff waits, including reading the response body. Default 30s;
	// negative disables it (rely on the request context instead).
	Timeout time.Duration

	// PerAttemptTimeout bounds a single attempt (connect → body closed), so a
	// hung attempt can be retried within Timeout. 0 = no per-attempt limit.
	PerAttemptTimeout time.Duration

	// MaxRetries is the number of retries after the first attempt (default 2,
	// i.e. up to 3 attempts). Set DisableRetry to turn retries off.
	MaxRetries   int
	DisableRetry bool

	// InitialBackoff and MaxBackoff shape the exponential backoff (with
	// jitter) between attempts. Defaults 100ms and 2s. A Retry-After header on
	// 429/503 is honoured when it is not longer than MaxBackoff; a longer one
	// ends retrying and returns the response to the caller.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	// RetryStatuses lists response codes to retry. nil = DefaultRetryStatuses;
	// an empty non-nil slice retries transport errors only.
	RetryStatuses []int

	// RetryNonIdempotent allows retrying POST/PATCH (and other non-idempotent
	// methods). By default those are retried only when the request carries an
	// Idempotency-Key header.
	RetryNonIdempotent bool

	// ShouldRetry, if set, replaces the status/error check (RetryStatuses and
	// the default transport-error rules). Method, body and budget rules still
	// apply. resp is nil when err is non-nil.
	ShouldRetry func(resp *http.Response, err error) bool

	// OnRetry, if set, is called synchronously each time an attempt is about
	// to be retried, after the wait has been decided and before sleeping.
	// Use it for metrics; it must be fast and must not modify the request.
	OnRetry func(RetryEvent)

	// Transport is the base RoundTripper. nil = a clone of
	// http.DefaultTransport tuned with the fields below.
	Transport http.RoundTripper

	// Base transport tuning; ignored when Transport is set.
	DialTimeout           time.Duration // default 5s
	TLSHandshakeTimeout   time.Duration // default 5s
	ResponseHeaderTimeout time.Duration // default 10s; negative disables
	MaxIdleConnsPerHost   int           // default 10 (net/http default is 2)
	MaxConnsPerHost       int           // 0 = unlimited

	// TLSClientConfig sets custom root CAs, client certificates (mTLS), etc.
	// It is cloned; nil = Go defaults. Ignored when Transport is set.
	TLSClientConfig *tls.Config

	// Proxy selects the proxy per request. nil = http.ProxyFromEnvironment
	// (HTTP_PROXY / HTTPS_PROXY / NO_PROXY). Ignored when Transport is set.
	Proxy func(*http.Request) (*url.URL, error)

	// Logging, passed to logger.NewHTTPTransport.
	DisableLogging     bool
	DisablePropagation bool
	LogHeaders         bool
	Redactor           *logger.Redactor
}

// New returns an *http.Client with timeouts, retry/backoff and logging:
//
//	http.Client{Timeout}
//	  └─ retry (backoff, Retry-After, per-attempt timeout)
//	       └─ logger.NewHTTPTransport (X-Request-ID, one log line per attempt)
//	            └─ base transport (pooled, dial/TLS timeouts)
//
// Every attempt is logged separately; retries carry an "attempt" field (2, 3, …).
// Build requests with http.NewRequestWithContext so trace_id reaches the logs
// and the request can be cancelled. Bodies are replayed with Request.GetBody,
// which http.NewRequest sets for *bytes.Buffer, *bytes.Reader and
// *strings.Reader; a request with any other body is sent once, never retried.
//
// The returned client is safe for concurrent use; create one per peer and reuse it.
func New(opts Options) *http.Client {
	opts = opts.withDefaults()

	base := opts.Transport
	if base == nil {
		base = newBaseTransport(opts)
	}
	rt := base
	if !opts.DisableLogging {
		rt = logger.NewHTTPTransport(rt, logger.HTTPClientOptions{
			Peer:               opts.Peer,
			DisablePropagation: opts.DisablePropagation,
			LogHeaders:         opts.LogHeaders,
			Redactor:           opts.Redactor,
		})
	}
	if !opts.DisableRetry || opts.PerAttemptTimeout > 0 {
		rt = newRetryTransport(rt, opts)
	}

	c := &http.Client{Transport: rt}
	if opts.Timeout > 0 {
		c.Timeout = opts.Timeout
	}
	return c
}

func (o Options) withDefaults() Options {
	if o.Timeout == 0 {
		o.Timeout = DefaultTimeout
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = DefaultMaxRetries
	}
	if o.DisableRetry {
		o.MaxRetries = 0
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = DefaultInitialBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = DefaultMaxBackoff
	}
	if o.MaxBackoff < o.InitialBackoff {
		o.MaxBackoff = o.InitialBackoff
	}
	if o.RetryStatuses == nil {
		o.RetryStatuses = DefaultRetryStatuses
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = DefaultDialTimeout
	}
	if o.TLSHandshakeTimeout <= 0 {
		o.TLSHandshakeTimeout = DefaultTLSHandshakeTimeout
	}
	if o.ResponseHeaderTimeout == 0 {
		o.ResponseHeaderTimeout = DefaultResponseHeaderTimeout
	}
	if o.MaxIdleConnsPerHost <= 0 {
		o.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}
	return o
}

func newBaseTransport(o Options) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: o.DialTimeout, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = o.TLSHandshakeTimeout
	t.ResponseHeaderTimeout = max(o.ResponseHeaderTimeout, 0)
	t.MaxIdleConnsPerHost = o.MaxIdleConnsPerHost
	t.MaxConnsPerHost = o.MaxConnsPerHost
	if t.MaxIdleConns < o.MaxIdleConnsPerHost {
		t.MaxIdleConns = o.MaxIdleConnsPerHost
	}
	if o.TLSClientConfig != nil {
		t.TLSClientConfig = o.TLSClientConfig.Clone()
	}
	if o.Proxy != nil {
		t.Proxy = o.Proxy
	}
	return t
}
