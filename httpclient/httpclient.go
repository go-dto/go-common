package httpclient

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger"
)

// Client is the high-level HTTP client exposed to applications. It hides
// net/http request construction, response-body lifecycle, retries and logging.
// A Client is safe for concurrent use and should be created once per peer.
type Client struct {
	cfg     Config
	baseURL *url.URL
	client  *http.Client
}

// New validates cfg and constructs a reusable high-level client.
func New(cfg Config) (*Client, error) {
	normalized, baseURL, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	return &Client{
		cfg:     normalized,
		baseURL: baseURL,
		client:  newStandardClient(normalized),
	}, nil
}

// MustNew is New for application startup. It panics when cfg is invalid.
func MustNew(cfg Config) *Client {
	c, err := New(cfg)
	if err != nil {
		panic(err)
	}
	return c
}

// CloseIdleConnections closes keep-alive connections currently sitting idle.
// It is useful during graceful shutdown; the Client remains reusable afterwards.
func (c *Client) CloseIdleConnections() {
	if c != nil && c.client != nil {
		c.client.CloseIdleConnections()
	}
}

func newStandardClient(cfg Config) *http.Client {
	base := cfg.Transport
	if base == nil {
		base = newBaseTransport(cfg)
	}

	var rt http.RoundTripper = base
	if !cfg.DisablePropagation {
		rt = &propagationTransport{next: rt}
	}
	if !cfg.DisableRetry || cfg.PerAttemptTimeout > 0 {
		rt = newRetryTransport(rt, cfg)
	}

	client := &http.Client{Transport: rt, CheckRedirect: redirectPolicy(cfg)}
	if cfg.Timeout > 0 {
		client.Timeout = cfg.Timeout
	}
	return client
}

func redirectPolicy(cfg Config) func(*http.Request, []*http.Request) error {
	if !cfg.FollowRedirects {
		return func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= cfg.MaxRedirects {
			return &Error{Kind: KindTransport, Peer: cfg.Peer, message: "redirect limit exceeded"}
		}
		if len(via) > 0 {
			origin := via[0].URL
			if req.URL.Scheme != origin.Scheme || !strings.EqualFold(req.URL.Host, origin.Host) {
				return &Error{Kind: KindTransport, Peer: cfg.Peer, message: "cross-host redirect blocked"}
			}
		}
		return nil
	}
}

func newBaseTransport(cfg Config) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = cfg.TLSHandshakeTimeout
	t.ResponseHeaderTimeout = max(cfg.ResponseHeaderTimeout, 0)
	t.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	t.MaxConnsPerHost = cfg.MaxConnsPerHost
	if t.MaxIdleConns < cfg.MaxIdleConnsPerHost {
		t.MaxIdleConns = cfg.MaxIdleConnsPerHost
	}
	if cfg.TLSClientConfig != nil {
		t.TLSClientConfig = cfg.TLSClientConfig.Clone()
	}
	if cfg.Proxy != nil {
		t.Proxy = cfg.Proxy
	}
	return t
}

type propagationTransport struct {
	next http.RoundTripper
}

func (t *propagationTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	logger.InjectHeaders(clone.Context(), clone.Header)
	return t.next.RoundTrip(clone)
}

type requestStateKey struct{}

type requestState struct {
	attempts  atomic.Int32
	method    Method
	operation string
}

func stateFromContext(ctx context.Context) *requestState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(requestStateKey{}).(*requestState)
	return state
}

func (c *Client) logResult(ctx context.Context, event resultEvent) {
	if c.cfg.OnResult != nil {
		invokeHook(ctx, c.cfg.DisableLogging, c.cfg.Peer, "OnResult", func() {
			c.cfg.OnResult(ctx, ResultEvent{
				Peer: c.cfg.Peer, Method: event.method, Operation: event.operation,
				StatusCode: event.statusCode, Attempts: event.attempts,
				Duration: event.duration, Outcome: event.outcome, ErrorKind: KindOf(event.err),
			})
		})
	}
	if c.cfg.DisableLogging {
		return
	}

	fields := []zap.Field{
		zap.String(logger.KeyPeer, c.cfg.Peer),
		zap.String("method", string(event.method)),
		zap.String("operation", event.operation),
		zap.Int("attempt_count", event.attempts),
		zap.Int("retry_count", max(event.attempts-1, 0)),
		zap.Float64(logger.KeyDurationMs, float64(event.duration)/float64(time.Millisecond)),
		zap.String("outcome", event.outcome),
	}
	if event.statusCode > 0 {
		fields = append(fields, zap.Int("status_code", event.statusCode))
	}
	if event.err != nil {
		fields = append(fields, zap.String("error_kind", string(KindOf(event.err))))
	}

	level := zapcore.InfoLevel
	message := "http outbound completed"
	switch {
	case event.err != nil && KindOf(event.err) != KindHTTPStatus:
		level, message = zapcore.ErrorLevel, "http outbound failed"
		if !logger.MarkErrorLogged(ctx, event.err) {
			level = zapcore.DebugLevel
		}
	case event.err != nil:
		level, message = zapcore.WarnLevel, "http outbound error status"
	case event.attempts > 1:
		level, message = zapcore.WarnLevel, "http outbound recovered"
	}

	logCtx := logger.WithFields(ctx, zap.String(logger.KeyLayer, logger.LayerClient))
	log := logger.FromContext(logCtx).WithOptions(zap.WithCaller(false))
	if checked := log.Check(level, message); checked != nil {
		checked.Write(fields...)
	}
}

func invokeHook(ctx context.Context, disableLogging bool, peer, hook string, fn func()) {
	defer func() {
		if recover() != nil && !disableLogging {
			logCtx := logger.WithFields(ctx, zap.String(logger.KeyLayer, logger.LayerClient))
			logger.FromContext(logCtx).Error("httpclient hook panicked",
				zap.String(logger.KeyPeer, peer), zap.String("hook", hook))
		}
	}()
	fn()
}

// ResultEvent is the sanitized final result of one logical request, including
// all retries and response-body reading. It contains no URL, header or body.
type ResultEvent struct {
	Peer       string
	Method     Method
	Operation  string
	StatusCode int
	Attempts   int
	Duration   time.Duration
	Outcome    string
	ErrorKind  ErrorKind
}

type resultEvent struct {
	method     Method
	operation  string
	statusCode int
	attempts   int
	duration   time.Duration
	outcome    string
	err        error
}
