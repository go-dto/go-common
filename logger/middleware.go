package logger

import (
	"net/http"
	"strings"
	"time"

	"github.com/felixge/httpsnoop"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger/ctxid"
)

// HTTPMiddlewareOptions configures HTTPMiddleware.
type HTTPMiddlewareOptions struct {
	// RequestIDHeader defaults to X-Request-ID.
	RequestIDHeader string

	// Route optionally returns the framework route template (for example,
	// "/users/:id"). Prefer it over aggregating on the raw path.
	Route func(*http.Request) string

	// LogRawPath includes URL.EscapedPath when Route returns empty. It is false
	// by default because paths often contain patient, account, or resource IDs.
	LogRawPath bool

	// Skip disables the access-log line for matching requests. Request-ID
	// validation and propagation still run.
	Skip func(*http.Request) bool

	// SlowThreshold overrides Config.SlowOpThreshold. Zero uses the global
	// value; a negative value disables slow-request promotion.
	SlowThreshold time.Duration

	// SuccessLevel is the level for successful, non-slow requests. Nil means
	// info. StatusLevel can override classification for any status code.
	SuccessLevel *zapcore.Level
	StatusLevel  func(statusCode int) zapcore.Level

	// DisableSuccessLog suppresses ordinary successful requests. Slow requests,
	// errors, and panics are still logged.
	DisableSuccessLog bool
}

// HTTPMiddleware validates or creates a request ID, returns it in the response,
// stores it in the request context, and writes one structured access-log line.
// Panics are logged without their potentially sensitive value and then rethrown
// for the application's recovery middleware.
func HTTPMiddleware(opts HTTPMiddlewareOptions) func(http.Handler) http.Handler {
	header := strings.TrimSpace(opts.RequestIDHeader)
	if header == "" {
		header = ctxid.Header
	}

	return func(next http.Handler) http.Handler {
		if next == nil {
			next = http.NotFoundHandler()
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimSpace(r.Header.Get(header))
			if !ctxid.Valid(id) {
				id = ctxid.New()
			}
			ctx := ctxid.WithContext(r.Context(), id)
			r = r.Clone(ctx)
			r.Header.Set(header, id)
			w.Header().Set(header, id)

			if opts.Skip != nil && opts.Skip(r) {
				next.ServeHTTP(w, r)
				return
			}

			metrics := httpsnoop.Metrics{Code: http.StatusOK}
			start := time.Now()
			defer func() {
				recovered := recover()
				d := time.Since(start)
				status := metrics.Code
				if recovered != nil && status < http.StatusInternalServerError {
					status = http.StatusInternalServerError
				}

				fields := make([]zap.Field, 0, 9)
				if !hasKey(FieldsFromContext(ctx), KeyLayer) {
					fields = append(fields, zap.String(KeyLayer, LayerHandler))
				}
				fields = append(fields,
					zap.String(KeyMethod, r.Method),
					zap.Int(KeyStatusCode, status),
					zap.Int64(KeyRespBytes, metrics.Written),
					zap.Float64(KeyDurationMs, ms(d)),
				)
				route := ""
				if opts.Route != nil {
					route = strings.TrimSpace(opts.Route(r))
				}
				if route != "" {
					// Route templates avoid leaking identifiers and reduce cardinality.
					fields = append(fields, zap.String(KeyRoute, route))
				} else if opts.LogRawPath {
					fields = append(fields, zap.String(KeyPath, r.URL.EscapedPath()))
				}

				l := FromContext(ctx).WithOptions(zap.WithCaller(false))
				lvl, msg, write := inboundLevel(opts, status, d)
				if recovered != nil {
					lvl, msg, write = zapcore.ErrorLevel, "http inbound panic", true
				}
				if write {
					if ce := l.Check(lvl, msg); ce != nil {
						ce.Write(fields...)
					}
				}
				if recovered != nil {
					panic(recovered)
				}
			}()

			metrics.CaptureMetrics(w, func(ww http.ResponseWriter) {
				next.ServeHTTP(ww, r)
			})
		})
	}
}

func inboundLevel(opts HTTPMiddlewareOptions, status int, d time.Duration) (zapcore.Level, string, bool) {
	lvl, msg := zapcore.InfoLevel, "http inbound"
	if opts.SuccessLevel != nil {
		lvl = *opts.SuccessLevel
	}
	if opts.StatusLevel != nil {
		lvl = opts.StatusLevel(status)
	} else if status >= http.StatusInternalServerError {
		lvl, msg = zapcore.ErrorLevel, "http inbound server error"
	} else if status >= http.StatusBadRequest {
		lvl, msg = zapcore.WarnLevel, "http inbound client error"
	}

	slow := opts.SlowThreshold
	if slow == 0 {
		slow = slowOpThreshold()
	}
	if slow > 0 && d >= slow && status < http.StatusBadRequest {
		return zapcore.WarnLevel, "http inbound slow", true
	}
	if opts.DisableSuccessLog && status < http.StatusBadRequest {
		return lvl, msg, false
	}
	return lvl, msg, true
}
