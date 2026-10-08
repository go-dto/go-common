package httpclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

// noSleep records requested waits instead of sleeping.
func noSleep(t *testing.T) *[]time.Duration {
	t.Helper()
	var waits []time.Duration
	prevSleep, prevJitter := sleep, jitter
	sleep = func(ctx context.Context, d time.Duration) error {
		waits = append(waits, d)
		return ctx.Err()
	}
	jitter = func(n int64) int64 { return n - 1 } // deterministic: always the upper bound
	t.Cleanup(func() { sleep, jitter = prevSleep, prevJitter })
	return &waits
}

func observe(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(logger.ReplaceForTest(zap.New(core)))
	return logs
}

// statusServer replies with codes[i] on the i-th call (last code repeats).
func statusServer(t *testing.T, codes ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1)) - 1
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Body", string(body))
		w.Header().Set("X-Got-Request-ID", r.Header.Get(ctxid.Header))
		w.WriteHeader(codes[min(n, len(codes)-1)])
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestRetriesThenSucceeds(t *testing.T) {
	waits := noSleep(t)
	logs := observe(t)
	srv, calls := statusServer(t, 503, 502, 200)

	c := New(Options{Peer: "upstream"})
	ctx := ctxid.WithContext(context.Background(), "trace-1")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 || calls.Load() != 3 {
		t.Fatalf("status=%d calls=%d", resp.StatusCode, calls.Load())
	}
	if resp.Header.Get("X-Got-Request-ID") != "trace-1" {
		t.Errorf("X-Request-ID not propagated on retry")
	}
	if want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}; !equalDur(*waits, want) {
		t.Errorf("waits = %v, want %v", *waits, want)
	}

	var attempts []any
	for _, e := range logs.FilterMessageSnippet("http outbound").All() {
		if e.Message == "http outbound retrying" {
			continue
		}
		cm := e.ContextMap()
		if cm[logger.KeyPeer] != "upstream" || cm[logger.KeyTraceID] != "trace-1" {
			t.Errorf("missing fields: %v", cm)
		}
		attempts = append(attempts, cm[KeyAttempt])
	}
	if len(attempts) != 3 || attempts[0] != nil || attempts[1] != int64(2) || attempts[2] != int64(3) {
		t.Errorf("attempt fields = %v", attempts)
	}
}

func TestGivesUpAndReturnsLastResponse(t *testing.T) {
	noSleep(t)
	srv, calls := statusServer(t, 500)

	c := New(Options{MaxRetries: 1})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 500 || calls.Load() != 2 || string(body) != "ok" {
		t.Fatalf("status=%d calls=%d body=%q", resp.StatusCode, calls.Load(), body)
	}
}

func TestNoRetryOnClientError(t *testing.T) {
	noSleep(t)
	srv, calls := statusServer(t, 400)
	resp, err := New(Options{}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
}

func TestDisableRetry(t *testing.T) {
	noSleep(t)
	srv, calls := statusServer(t, 503)
	resp, err := New(Options{DisableRetry: true}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
}

func TestPostRetryRules(t *testing.T) {
	tests := []struct {
		name      string
		opts      Options
		idemKey   string // header name carrying an idempotency key, "" = none
		body      io.Reader
		wantCalls int32
	}{
		{"post not retried by default", Options{}, "", strings.NewReader("x"), 1},
		{"post with idempotency key", Options{}, "Idempotency-Key", strings.NewReader("x"), 3},
		{"post with x-idempotency key", Options{}, "X-Idempotency-Key", strings.NewReader("x"), 3},
		{"post with RetryNonIdempotent", Options{RetryNonIdempotent: true}, "", strings.NewReader("x"), 3},
		{"unreplayable body", Options{RetryNonIdempotent: true}, "", io.NopCloser(strings.NewReader("x")), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noSleep(t)
			srv, calls := statusServer(t, 503)
			req, _ := http.NewRequest(http.MethodPost, srv.URL, tt.body)
			if tt.idemKey != "" {
				req.Header.Set(tt.idemKey, "k1")
			}
			resp, err := New(tt.opts).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if calls.Load() != tt.wantCalls {
				t.Fatalf("calls=%d, want %d", calls.Load(), tt.wantCalls)
			}
			if got := resp.Header.Get("X-Body"); got != "x" {
				t.Errorf("last attempt body = %q, want replayed body", got)
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	waits := noSleep(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	resp, err := New(Options{MaxBackoff: 2 * time.Second}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || !equalDur(*waits, []time.Duration{time.Second}) {
		t.Fatalf("status=%d waits=%v", resp.StatusCode, *waits)
	}

	// Retry-After longer than MaxBackoff: give up and return the 429.
	calls.Store(0)
	*waits = nil
	resp, err = New(Options{MaxBackoff: 500 * time.Millisecond}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || calls.Load() != 1 || len(*waits) != 0 {
		t.Fatalf("status=%d calls=%d waits=%v", resp.StatusCode, calls.Load(), *waits)
	}
}

func TestTransportErrorRetried(t *testing.T) {
	noSleep(t)
	observe(t)
	var calls atomic.Int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) < 3 {
			return nil, errors.New("connection reset by peer")
		}
		return &http.Response{StatusCode: 200, Body: http.NoBody, Request: r}, nil
	})
	resp, err := New(Options{Transport: rt}).Get("http://example.invalid/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 3 {
		t.Fatalf("calls=%d, want 3", calls.Load())
	}
}

func TestPerAttemptTimeoutRetries(t *testing.T) {
	noSleep(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done() // hang until the attempt times out
			return
		}
		_, _ = io.WriteString(w, "fresh")
	}))
	defer srv.Close()

	c := New(Options{PerAttemptTimeout: 50 * time.Millisecond})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body) // must not be cut off by the attempt context
	resp.Body.Close()
	if err != nil || string(body) != "fresh" || calls.Load() != 2 {
		t.Fatalf("body=%q err=%v calls=%d", body, err, calls.Load())
	}
}

func TestContextCanceledStopsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	prev := sleep
	sleep = func(ctx context.Context, d time.Duration) error { cancel(); return ctx.Err() }
	t.Cleanup(func() { sleep = prev })

	srv, calls := statusServer(t, 503)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	_, err := New(Options{}).Do(req)
	if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestTotalTimeoutBudget(t *testing.T) {
	waits := noSleep(t)
	srv, calls := statusServer(t, 503)
	// Backoff (1s) exceeds the remaining budget (200ms): stop after one attempt.
	c := New(Options{Timeout: 200 * time.Millisecond, InitialBackoff: time.Second, MaxBackoff: time.Second})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 || calls.Load() != 1 || len(*waits) != 0 {
		t.Fatalf("status=%d calls=%d waits=%v", resp.StatusCode, calls.Load(), *waits)
	}
}

func TestBackoffCapped(t *testing.T) {
	noSleep(t)
	rt := &retryTransport{opts: Options{InitialBackoff: 100 * time.Millisecond, MaxBackoff: 300 * time.Millisecond}}
	want := []time.Duration{100, 200, 300, 300}
	for i, w := range want {
		if got := rt.backoff(i + 1); got != w*time.Millisecond {
			t.Errorf("backoff(%d) = %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
}

func TestDefaultsApplied(t *testing.T) {
	c := New(Options{})
	if c.Timeout != DefaultTimeout {
		t.Errorf("Timeout = %v", c.Timeout)
	}
	if c := New(Options{Timeout: -1}); c.Timeout != 0 {
		t.Errorf("negative Timeout should disable, got %v", c.Timeout)
	}
	tr := newBaseTransport(Options{}.withDefaults())
	if tr.MaxIdleConnsPerHost != DefaultMaxIdleConnsPerHost || tr.TLSHandshakeTimeout != DefaultTLSHandshakeTimeout ||
		tr.ResponseHeaderTimeout != DefaultResponseHeaderTimeout {
		t.Errorf("base transport not tuned: %+v", tr)
	}
	if tr.Proxy == nil {
		t.Error("Proxy should default to http.ProxyFromEnvironment")
	}
	if tr := newBaseTransport(Options{ResponseHeaderTimeout: -1}.withDefaults()); tr.ResponseHeaderTimeout != 0 {
		t.Errorf("negative ResponseHeaderTimeout should disable, got %v", tr.ResponseHeaderTimeout)
	}
}

func TestResponseHeaderTimeoutRetries(t *testing.T) {
	noSleep(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done() // never send headers on the first attempt
			return
		}
		_, _ = io.WriteString(w, "fresh")
	}))
	defer srv.Close()

	resp, err := New(Options{ResponseHeaderTimeout: 50 * time.Millisecond}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "fresh" || calls.Load() != 2 {
		t.Fatalf("body=%q calls=%d", body, calls.Load())
	}
}

func TestOnRetry(t *testing.T) {
	noSleep(t)
	srv, _ := statusServer(t, 503, 502, 200)
	var events []RetryEvent
	resp, err := New(Options{Peer: "upstream", OnRetry: func(e RetryEvent) { events = append(events, e) }}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if len(events) != 2 {
		t.Fatalf("events = %+v", events)
	}
	want := []struct {
		attempt, status int
		wait            time.Duration
	}{{1, 503, 100 * time.Millisecond}, {2, 502, 200 * time.Millisecond}}
	for i, w := range want {
		e := events[i]
		if e.Attempt != w.attempt || e.StatusCode != w.status || e.Wait != w.wait || e.Err != nil ||
			e.Peer != "upstream" || e.Request == nil {
			t.Errorf("event %d = %+v, want attempt=%d status=%d wait=%v", i, e, w.attempt, w.status, w.wait)
		}
	}
}

func TestOnRetryTransportError(t *testing.T) {
	noSleep(t)
	observe(t)
	boom := errors.New("connection reset by peer")
	var calls atomic.Int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, boom
		}
		return &http.Response{StatusCode: 200, Body: http.NoBody, Request: r}, nil
	})
	var events []RetryEvent
	resp, err := New(Options{Transport: rt, OnRetry: func(e RetryEvent) { events = append(events, e) }}).Get("http://example.invalid/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(events) != 1 || !errors.Is(events[0].Err, boom) || events[0].StatusCode != 0 {
		t.Fatalf("events = %+v", events)
	}
}

// tlsServer starts a TLS test server that does not log handshake errors.
func tlsServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "secure")
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestTLSClientConfig(t *testing.T) {
	srv := tlsServer(t)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())

	resp, err := New(Options{TLSClientConfig: &tls.Config{RootCAs: pool}}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "secure" {
		t.Fatalf("body = %q", body)
	}
}

func TestCertificateErrorNotRetried(t *testing.T) {
	noSleep(t)
	srv := tlsServer(t)
	retried := false
	_, err := New(Options{OnRetry: func(RetryEvent) { retried = true }}).Get(srv.URL) // self-signed, not trusted
	if err == nil {
		t.Fatal("expected certificate error")
	}
	if retried {
		t.Errorf("certificate error was retried: %v", err)
	}
}

func TestProxy(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proxied-Host", r.URL.Host)
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)

	resp, err := New(Options{Proxy: http.ProxyURL(proxyURL)}).Get("http://upstream.invalid/path")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Proxied-Host"); got != "upstream.invalid" {
		t.Fatalf("request did not go through proxy, X-Proxied-Host = %q", got)
	}
}

func TestRetryAfterHTTPDate(t *testing.T) {
	waits := noSleep(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", time.Now().Add(2*time.Second).UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	resp, err := New(Options{MaxBackoff: 5 * time.Second}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// HTTP dates have 1s resolution, so the wait is somewhere in (0, 2s].
	if resp.StatusCode != 200 || len(*waits) != 1 || (*waits)[0] <= 0 || (*waits)[0] > 2*time.Second {
		t.Fatalf("status=%d waits=%v", resp.StatusCode, *waits)
	}
}

func TestRetryAfterIgnoredOnOtherStatuses(t *testing.T) {
	waits := noSleep(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	resp, err := New(Options{}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !equalDur(*waits, []time.Duration{DefaultInitialBackoff}) {
		t.Fatalf("waits = %v, want normal backoff", *waits)
	}
}

func TestRetryAfterOverflow(t *testing.T) {
	waits := noSleep(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "99999999999999")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	resp, err := New(Options{}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// A huge value must be treated as "too long", not overflow into an immediate retry.
	if resp.StatusCode != 429 || calls.Load() != 1 || len(*waits) != 0 {
		t.Fatalf("status=%d calls=%d waits=%v", resp.StatusCode, calls.Load(), *waits)
	}
}

func TestEmptyRetryStatuses(t *testing.T) {
	noSleep(t)
	srv, calls := statusServer(t, 503)
	resp, err := New(Options{RetryStatuses: []int{}}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
}

func TestCustomShouldRetry(t *testing.T) {
	noSleep(t)
	only404 := func(resp *http.Response, err error) bool { return resp != nil && resp.StatusCode == 404 }

	srv, calls := statusServer(t, 404)
	resp, err := New(Options{ShouldRetry: only404}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 3 {
		t.Errorf("404: calls=%d, want 3", calls.Load())
	}

	srv, calls = statusServer(t, 503)
	resp, err = New(Options{ShouldRetry: only404}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Errorf("503: calls=%d, want 1 (ShouldRetry replaces RetryStatuses)", calls.Load())
	}
}

func TestGetBodyError(t *testing.T) {
	noSleep(t)
	srv, calls := statusServer(t, 503)
	boom := errors.New("cannot rewind")
	req, _ := http.NewRequest(http.MethodPut, srv.URL, strings.NewReader("x"))
	req.GetBody = func() (io.ReadCloser, error) { return nil, boom }

	_, err := New(Options{}).Do(req)
	if !errors.Is(err, boom) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestDisableLogging(t *testing.T) {
	logs := observe(t)
	srv, _ := statusServer(t, 200)
	resp, err := New(Options{DisableLogging: true}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n := logs.FilterMessageSnippet("http outbound").Len(); n != 0 {
		t.Fatalf("got %d log lines, want 0", n)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func equalDur(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
