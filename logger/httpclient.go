package logger

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger/ctxid"
)

// DefaultHTTPClientTimeout is applied only when WrapHTTPClient is called with
// a nil client. Callers that pass a client retain their own timeout policy.
const DefaultHTTPClientTimeout = 30 * time.Second

// HTTPClientOptions configures NewHTTPTransport.
type HTTPClientOptions struct {
	// Peer is a logical name for the third party (e.g. "client_webhook",
	// "payment_gateway"); logged as "peer".
	Peer string

	// DisablePropagation stops the transport from adding X-Request-ID to
	// outgoing requests. Propagation is on by default.
	DisablePropagation bool

	// LogHeaders adds redacted request headers to the log line.
	LogHeaders bool

	// Redactor masks URLs and headers. Defaults to DefaultRedactor().
	Redactor *Redactor

	// Route optionally returns a low-cardinality route or operation template.
	// It is logged instead of the raw URL (for example, "/patients/{id}").
	Route func(*http.Request) string

	// LogURL includes a redacted URL. It is false by default because URL paths
	// can contain personal identifiers even after query values are masked.
	LogURL bool

	// SuccessLevel is the level used for non-slow responses below status 400.
	// Nil preserves the historical info-level behavior.
	SuccessLevel *zapcore.Level

	// DisableSuccessLog suppresses ordinary successful calls. Slow responses
	// are still logged at warn level.
	DisableSuccessLog bool

	// SlowThreshold overrides Config.SlowOpThreshold for this client. Zero uses
	// the global value; a negative value disables slow-call promotion.
	SlowThreshold time.Duration

	// StatusLevel optionally classifies HTTP error statuses. It is called only
	// for status >= 400; nil uses warn level.
	StatusLevel func(statusCode int) zapcore.Level
}

// InjectHeaders sets X-Request-ID on h from ctx (if present and not already
// set), so the receiving service can log the same trace_id.
func InjectHeaders(ctx context.Context, h http.Header) {
	if h == nil {
		return
	}
	if id, ok := ctxid.FromContext(ctx); ok && h.Get(ctxid.Header) == "" {
		h.Set(ctxid.Header, id)
	}
}

// NewHTTPTransport wraps base (http.DefaultTransport if nil) so that every
// outgoing request:
//   - carries X-Request-ID = trace_id from the request context;
//   - is logged once with layer=client, peer, method, host, status_code,
//     duration_ms and the context fields (trace_id, request_id, …). Route is
//     optional; URL logging requires explicit opt-in.
//
// Default levels: info for < 400, warn for 4xx/5xx or slow calls, error for
// transport errors (timeout, connection refused, …). HTTPClientOptions can
// suppress or reclassify success/status logs. Request/response bodies are
// never read or logged here.
//
// Always build requests with http.NewRequestWithContext(ctx, …) so the
// transport sees the caller's context.
func NewHTTPTransport(base http.RoundTripper, opts HTTPClientOptions) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if opts.Redactor == nil {
		opts.Redactor = DefaultRedactor()
	}
	return &loggingTransport{base: base, opts: opts}
}

// WrapHTTPClient returns a shallow copy of c (or a new client with a bounded
// default timeout if nil) whose transport is wrapped with NewHTTPTransport.
// c itself is not modified.
func WrapHTTPClient(c *http.Client, opts HTTPClientOptions) *http.Client {
	var cp http.Client
	if c != nil {
		cp = *c
	} else {
		cp.Timeout = DefaultHTTPClientTimeout
	}
	cp.Transport = NewHTTPTransport(cp.Transport, opts)
	return &cp
}

type loggingTransport struct {
	base http.RoundTripper
	opts HTTPClientOptions
}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	if !t.opts.DisablePropagation {
		if id, ok := ctxid.FromContext(ctx); ok && req.Header.Get(ctxid.Header) == "" {
			req = req.Clone(ctx) // RoundTrippers must not modify the caller's request
			req.Header.Set(ctxid.Header, id)
		}
	}

	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	if resp == nil && err == nil {
		err = errors.New("logger: round tripper returned nil response and nil error")
	}
	d := time.Since(start)

	fields := make([]zap.Field, 0, 10)
	if !hasKey(FieldsFromContext(ctx), KeyLayer) {
		fields = append(fields, zap.String(KeyLayer, LayerClient))
	}
	fields = append(fields,
		zap.String("method", req.Method),
		zap.String("host", req.URL.Host),
		zap.Float64(KeyDurationMs, ms(d)),
	)
	if t.opts.Route != nil {
		if route := strings.TrimSpace(t.opts.Route(req)); route != "" {
			fields = append(fields, zap.String(KeyRoute, route))
		}
	} else if t.opts.LogURL {
		fields = append(fields, zap.String("url", t.opts.Redactor.URL(req.URL.String())))
	}
	if t.opts.Peer != "" {
		fields = append(fields, zap.String(KeyPeer, t.opts.Peer))
	}
	if t.opts.LogHeaders {
		fields = append(fields, zap.Any("request_headers", t.opts.Redactor.Headers(req.Header)))
	}

	// caller would point into net/http internals; the op field identifies the call site.
	l := FromContext(ctx).WithOptions(zap.WithCaller(false))
	lvl, msg := zapcore.InfoLevel, "http outbound"
	if t.opts.SuccessLevel != nil {
		lvl = *t.opts.SuccessLevel
	}
	write := true
	switch {
	case err != nil:
		errorKind := "transport"
		if errors.Is(err, context.DeadlineExceeded) {
			errorKind = "timeout"
		} else if errors.Is(err, context.Canceled) {
			errorKind = "canceled"
		}
		fields = append(fields, zap.String("error_kind", errorKind))
		lvl, msg = zapcore.ErrorLevel, "http outbound failed"
		if !MarkErrorLogged(ctx, err) {
			lvl = zapcore.DebugLevel
		}
	case resp.StatusCode >= 400:
		fields = append(fields, zap.Int("status_code", resp.StatusCode))
		lvl, msg = zapcore.WarnLevel, "http outbound error status"
		if t.opts.StatusLevel != nil {
			lvl = t.opts.StatusLevel(resp.StatusCode)
		}
	default:
		fields = append(fields, zap.Int("status_code", resp.StatusCode))
		slow := t.opts.SlowThreshold
		if slow == 0 {
			slow = slowOpThreshold()
		}
		if slow > 0 && d >= slow {
			lvl, msg = zapcore.WarnLevel, "http outbound slow"
		} else if t.opts.DisableSuccessLog {
			write = false
		}
	}
	if write {
		if ce := l.Check(lvl, msg); ce != nil {
			ce.Write(fields...)
		}
	}
	return resp, err
}
