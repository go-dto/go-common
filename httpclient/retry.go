package httpclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
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
// Options.OnRetry.
type RetryEvent struct {
	Request    *http.Request // the attempt's request; read-only
	Peer       string        // Options.Peer
	Attempt    int           // the attempt that just failed (1 = first)
	StatusCode int           // response status, 0 when Err is set
	Err        error         // transport error, nil when a response was received
	Wait       time.Duration // backoff (or Retry-After) before the next attempt
}

// Overridable in tests.
var (
	sleep  = sleepCtx
	jitter = func(n int64) int64 { return rand.Int64N(n) }
)

type retryTransport struct {
	next http.RoundTripper
	opts Options
}

func newRetryTransport(next http.RoundTripper, opts Options) *retryTransport {
	return &retryTransport{next: next, opts: opts}
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	maxAttempts := 1
	if t.canRetry(req) {
		maxAttempts += t.opts.MaxRetries
	}

	for attempt := 1; ; attempt++ {
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

		logger.FromContext(ctx).Debug("http outbound retrying",
			zap.String(logger.KeyPeer, t.opts.Peer),
			zap.Int(KeyAttempt, attempt+1),
			zap.Float64("backoff_ms", float64(wait)/float64(time.Millisecond)),
		)
		if t.opts.OnRetry != nil {
			ev := RetryEvent{Request: areq, Peer: t.opts.Peer, Attempt: attempt, Err: err, Wait: wait}
			if resp != nil {
				ev.StatusCode = resp.StatusCode
			}
			t.opts.OnRetry(ev)
		}
		if resp != nil {
			drainAndClose(resp.Body)
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
		return resp, false, err
	}
	actx, cancel := context.WithTimeout(req.Context(), t.opts.PerAttemptTimeout)
	resp, err := t.next.RoundTrip(req.WithContext(actx))
	if err != nil {
		timedOut = errors.Is(actx.Err(), context.DeadlineExceeded) && req.Context().Err() == nil
		cancel()
		return nil, timedOut, err
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
	if t.opts.ShouldRetry != nil {
		return t.opts.ShouldRetry(resp, err)
	}
	if err != nil {
		return isRetryableError(err)
	}
	return slices.Contains(t.opts.RetryStatuses, resp.StatusCode)
}

// isRetryableError treats transport errors as transient except certificate
// problems, which will not fix themselves on the next attempt.
func isRetryableError(err error) bool {
	var (
		certErr     *tls.CertificateVerificationError
		unknownAuth x509.UnknownAuthorityError
		hostErr     x509.HostnameError
		invalidErr  x509.CertificateInvalidError
	)
	switch {
	case errors.As(err, &certErr), errors.As(err, &unknownAuth),
		errors.As(err, &hostErr), errors.As(err, &invalidErr):
		return false
	}
	return true
}

// backoff returns the wait before attempt+1: exponential from InitialBackoff,
// capped at MaxBackoff, with "equal jitter" (between d/2 and d).
func (t *retryTransport) backoff(attempt int) time.Duration {
	d := t.opts.InitialBackoff
	for i := 1; i < attempt && d < t.opts.MaxBackoff; i++ {
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
