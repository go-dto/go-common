package httpclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/go-dto/go-common/logger"
)

// KeyAttempt is the log field set on retried attempts (2, 3, …).
const KeyAttempt = "attempt"

// maxDrainBytes bounds how much of a discarded response is read so the
// connection can be reused; larger bodies just close the connection.
const maxDrainBytes = 64 << 10

// maxRetryAfterSeconds is the largest Retry-After that fits in a Duration.
const maxRetryAfterSeconds = int64(math.MaxInt64 / time.Second)

// RetryEvent describes a failed attempt that is about to be retried; see
// Config.OnRetry. Err is for programmatic classification and should not be
// logged directly because transport errors can contain destination details.
type RetryEvent struct {
	Peer       string
	Method     Method
	Operation  string
	Attempt    int
	StatusCode int
	Err        error
	Wait       time.Duration
}

// Overridable in tests.
var (
	sleep  = sleepCtx
	jitter = func(n int64) int64 { return rand.Int64N(n) }
)

type retryTransport struct {
	next http.RoundTripper
	opts Config
}

func newRetryTransport(next http.RoundTripper, opts Config) *retryTransport {
	return &retryTransport{next: next, opts: opts}
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	maxAttempts := 1
	if t.canRetry(req) {
		maxAttempts += t.opts.MaxRetries
	}

	for attempt := 1; ; attempt++ {
		if state := stateFromContext(ctx); state != nil {
			state.attempts.Store(int32(attempt))
		}
		areq := req
		if attempt > 1 {
			areq = req.Clone(logger.WithFields(ctx, zap.Int(KeyAttempt, attempt)))
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, err
				}
				areq.Body = body
			}
		}

		resp, attemptTimedOut, err := t.roundTripOnce(areq)
		if attempt >= maxAttempts || ctx.Err() != nil {
			return resp, err
		}
		if !attemptTimedOut && !t.shouldRetry(resp, err) {
			return resp, err
		}

		wait := t.backoff(attempt)
		if resp != nil {
			if ra, ok := retryAfter(resp); ok {
				if ra > t.opts.MaxBackoff {
					return resp, err // server asks for longer than we are willing to wait
				}
				wait = ra
			}
		}
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= wait {
			return resp, err // not enough budget left for another attempt
		}

		if resp != nil {
			// Release the connection before invoking application hooks. A slow
			// hook must not keep the previous attempt's body open.
			drainAndClose(resp.Body)
		}
		if !t.opts.DisableLogging {
			logCtx := logger.WithFields(ctx, zap.String(logger.KeyLayer, logger.LayerClient))
			logger.FromContext(logCtx).Debug("http outbound retrying",
				zap.String(logger.KeyPeer, t.opts.Peer),
				zap.Int(KeyAttempt, attempt+1),
				zap.Float64("backoff_ms", float64(wait)/float64(time.Millisecond)),
			)
		}
		if t.opts.OnRetry != nil {
			ev := RetryEvent{Peer: t.opts.Peer, Attempt: attempt, Err: err, Wait: wait}
			if state := stateFromContext(ctx); state != nil {
				ev.Method = state.method
				ev.Operation = state.operation
			}
			if resp != nil {
				ev.StatusCode = resp.StatusCode
			}
			invokeHook(ctx, t.opts.DisableLogging, t.opts.Peer, "OnRetry", func() {
				t.opts.OnRetry(ev)
			})
		}
		if serr := sleep(ctx, wait); serr != nil {
			if err == nil {
				err = serr
			}
			return nil, err
		}
	}
}

// roundTripOnce runs one attempt, applying PerAttemptTimeout. The attempt's
// context is released when the response body is closed.
func (t *retryTransport) roundTripOnce(req *http.Request) (_ *http.Response, timedOut bool, _ error) {
	if t.opts.PerAttemptTimeout <= 0 {
		resp, err := t.next.RoundTrip(req)
		if err == nil && resp == nil {
			return nil, false, errors.New("httpclient: transport returned a nil response")
		}
		if err == nil && resp.Body == nil {
			resp.Body = http.NoBody
		}
		return resp, false, err
	}
	actx, cancel := context.WithTimeout(req.Context(), t.opts.PerAttemptTimeout)
	resp, err := t.next.RoundTrip(req.WithContext(actx))
	if err != nil {
		timedOut = errors.Is(actx.Err(), context.DeadlineExceeded) && req.Context().Err() == nil
		cancel()
		return nil, timedOut, err
	}
	if resp == nil {
		cancel()
		return nil, false, errors.New("httpclient: transport returned a nil response")
	}
	if resp.Body == nil {
		resp.Body = http.NoBody
	}
	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, false, nil
}

// canRetry reports whether req may be sent more than once.
func (t *retryTransport) canRetry(req *http.Request) bool {
	if t.opts.MaxRetries <= 0 {
		return false
	}
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return false // body cannot be replayed
	}
	if t.opts.RetryNonIdempotent || isIdempotent(req) {
		return true
	}
	return false
}

func isIdempotent(req *http.Request) bool {
	switch req.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace,
		http.MethodPut, http.MethodDelete:
		return true
	}
	return req.Header.Get("Idempotency-Key") != "" || req.Header.Get("X-Idempotency-Key") != ""
}

func (t *retryTransport) shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		return isRetryableError(err)
	}
	if resp == nil {
		return false
	}
	return slices.Contains(t.opts.RetryStatuses, resp.StatusCode)
}

// isRetryableError is deliberately conservative: deterministic configuration,
// TLS and permanent DNS errors should not be amplified with retries.
func isRetryableError(err error) bool {
	var (
		certErr     *tls.CertificateVerificationError
		unknownAuth x509.UnknownAuthorityError
		hostErr     x509.HostnameError
		invalidErr  x509.CertificateInvalidError
		dnsErr      *net.DNSError
		netErr      net.Error
	)
	switch {
	case errors.Is(err, context.Canceled):
		return false
	case errors.As(err, &certErr), errors.As(err, &unknownAuth),
		errors.As(err, &hostErr), errors.As(err, &invalidErr):
		return false
	case errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return true
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ETIMEDOUT),
		errors.Is(err, syscall.ENETUNREACH), errors.Is(err, syscall.EHOSTUNREACH):
		return true
	case errors.As(err, &dnsErr):
		return dnsErr.Timeout()
	case errors.As(err, &netErr):
		return netErr.Timeout()
	}
	return false
}

// backoff returns the wait before attempt+1: exponential from InitialBackoff,
// capped at MaxBackoff, with "equal jitter" (between d/2 and d).
func (t *retryTransport) backoff(attempt int) time.Duration {
	d := t.opts.InitialBackoff
	for i := 1; i < attempt && d < t.opts.MaxBackoff; i++ {
		if d >= t.opts.MaxBackoff-d {
			d = t.opts.MaxBackoff
			break
		}
		d *= 2
	}
	d = min(d, t.opts.MaxBackoff)
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(jitter(int64(half)+1))
}

// retryAfter parses Retry-After (seconds or HTTP date) on 429/503 responses.
func retryAfter(resp *http.Response) (time.Duration, bool) {
	if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
		return 0, false
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if s, err := strconv.ParseInt(v, 10, 64); err == nil {
		if s > maxRetryAfterSeconds {
			return time.Duration(math.MaxInt64), true // would overflow; treat as "very long"
		}
		return max(time.Duration(s)*time.Second, 0), true
	}
	if at, err := http.ParseTime(v); err == nil {
		return max(time.Until(at), 0), true
	}
	return 0, false
}

func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.CopyN(io.Discard, body, maxDrainBytes)
	_ = body.Close()
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}
