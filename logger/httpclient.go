package logger

import (
	"context"
	"net/http"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger/ctxid"
)

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
//   - is logged once with layer=client, peer, method, url (redacted), host,
//     status_code, duration_ms and the context fields (trace_id, request_id, …).
//
// Levels: info for < 400, warn for 4xx/5xx or slow calls, error for transport
// errors (timeout, connection refused, …). Request/response bodies are never
// read or logged here.
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

// WrapHTTPClient returns a shallow copy of c (or a new client if nil) whose
// transport is wrapped with NewHTTPTransport. c itself is not modified.
func WrapHTTPClient(c *http.Client, opts HTTPClientOptions) *http.Client {
	var cp http.Client
	if c != nil {
		cp = *c
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
	d := time.Since(start)

	fields := make([]zap.Field, 0, 10)
	if !hasKey(FieldsFromContext(ctx), KeyLayer) {
		fields = append(fields, zap.String(KeyLayer, LayerClient))
	}
	fields = append(fields,
		zap.String("method", req.Method),
		zap.String("url", t.opts.Redactor.URL(req.URL.String())),
		zap.String("host", req.URL.Host),
		zap.Float64(KeyDurationMs, ms(d)),
	)
	if t.opts.Peer != "" {
		fields = append(fields, zap.String(KeyPeer, t.opts.Peer))
	}
	if t.opts.LogHeaders {
		fields = append(fields, zap.Any("request_headers", t.opts.Redactor.Headers(req.Header)))
	}

	// caller would point into net/http internals; the op field identifies the call site.
	l := FromContext(ctx).WithOptions(zap.WithCaller(false))
	lvl, msg := zapcore.InfoLevel, "http outbound"
	switch {
	case err != nil:
		fields = append(fields, zap.Error(err))
		lvl, msg = zapcore.ErrorLevel, "http outbound failed"
		if !MarkErrorLogged(ctx, err) {
			lvl = zapcore.DebugLevel
		}
	case resp.StatusCode >= 400:
		fields = append(fields, zap.Int("status_code", resp.StatusCode))
		lvl, msg = zapcore.WarnLevel, "http outbound error status"
	default:
		fields = append(fields, zap.Int("status_code", resp.StatusCode))
		if slow := slowOpThreshold(); slow > 0 && d >= slow {
			lvl, msg = zapcore.WarnLevel, "http outbound slow"
		}
	}
	if ce := l.Check(lvl, msg); ce != nil {
		ce.Write(fields...)
	}
	return resp, err
}
