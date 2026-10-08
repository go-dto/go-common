package database

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/logger"
)

func observe(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(logger.ReplaceForTest(zap.New(core)))
	return logs
}

func noSleep(t *testing.T) *[]time.Duration {
	t.Helper()
	var waits []time.Duration
	prev := sleep
	sleep = func(ctx context.Context, d time.Duration) error { waits = append(waits, d); return ctx.Err() }
	t.Cleanup(func() { sleep = prev })
	return &waits
}

// flakyPing fails the first n calls.
func flakyPing(n int) (func(context.Context) error, *int) {
	calls := 0
	return func(ctx context.Context) error {
		calls++
		if calls <= n {
			return errors.New("connection refused")
		}
		return nil
	}, &calls
}

func TestWaitReadyFirstTry(t *testing.T) {
	logs := observe(t)
	waits := noSleep(t)
	ping, calls := flakyPing(0)
	if err := WaitReady(context.Background(), Retry{}, ping, zap.String(logger.KeyDBSystem, "mysql")); err != nil {
		t.Fatal(err)
	}
	if *calls != 1 || len(*waits) != 0 {
		t.Fatalf("calls=%d waits=%v", *calls, *waits)
	}
	e := logs.FilterMessage("database connected").All()
	if len(e) != 1 || e[0].ContextMap()[logger.KeyDBSystem] != "mysql" {
		t.Errorf("connected line = %v", e)
	}
}

func TestWaitReadyRetriesWithBackoff(t *testing.T) {
	logs := observe(t)
	waits := noSleep(t)
	ping, calls := flakyPing(3)
	r := Retry{Attempts: 5, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 300 * time.Millisecond}
	if err := WaitReady(context.Background(), r, ping); err != nil {
		t.Fatal(err)
	}
	if *calls != 4 || len(*waits) != 3 {
		t.Fatalf("calls=%d waits=%v", *calls, *waits)
	}
	// Equal jitter: each wait is within [d/2, d] for d = 100ms, 200ms, 300ms (capped).
	for i, d := range []time.Duration{100, 200, 300} {
		d *= time.Millisecond
		if w := (*waits)[i]; w < d/2 || w > d {
			t.Errorf("wait %d = %v, want within [%v, %v]", i, w, d/2, d)
		}
	}
	if n := logs.FilterMessage("database not ready, retrying").FilterLevelExact(zapcore.WarnLevel).Len(); n != 3 {
		t.Errorf("retry warn lines = %d", n)
	}
}

func TestWaitReadyGivesUp(t *testing.T) {
	logs := observe(t)
	noSleep(t)
	ping, calls := flakyPing(100)
	err := WaitReady(context.Background(), Retry{Attempts: 3}, ping)
	if err == nil || !strings.Contains(err.Error(), "connection refused") || *calls != 3 {
		t.Fatalf("err=%v calls=%d", err, *calls)
	}
	if logs.FilterMessage("database unreachable").FilterLevelExact(zapcore.ErrorLevel).Len() != 1 {
		t.Error("missing unreachable error line")
	}
}

func TestWaitReadyStopsOnContextCancel(t *testing.T) {
	observe(t)
	ctx, cancel := context.WithCancel(context.Background())
	prev := sleep
	sleep = func(ctx context.Context, d time.Duration) error { cancel(); return ctx.Err() }
	t.Cleanup(func() { sleep = prev })

	ping, calls := flakyPing(100)
	err := WaitReady(ctx, Retry{Attempts: 10}, ping)
	if !errors.Is(err, context.Canceled) || *calls != 1 {
		t.Fatalf("err=%v calls=%d", err, *calls)
	}
}

func TestWaitReadyPingTimeout(t *testing.T) {
	observe(t)
	noSleep(t)
	var deadline time.Duration
	ping := func(ctx context.Context) error {
		dl, _ := ctx.Deadline()
		deadline = time.Until(dl)
		return nil
	}
	if err := WaitReady(context.Background(), Retry{PingTimeout: time.Second}, ping); err != nil {
		t.Fatal(err)
	}
	if deadline <= 0 || deadline > time.Second {
		t.Fatalf("ping deadline = %v, want ≤ 1s", deadline)
	}
}

func TestHealthHandler(t *testing.T) {
	logs := observe(t)
	ok := Check{Name: "mysql", Ping: func(context.Context) error { return nil }}
	bad := Check{Name: "mongo", Ping: func(context.Context) error { return errors.New("dial tcp 10.1.2.3:27017: refused") }}

	tests := []struct {
		name       string
		checks     []Check
		wantStatus int
		want       HealthResponse
	}{
		{"all ok", []Check{ok}, 200, HealthResponse{"ok", map[string]string{"mysql": "ok"}}},
		{"one down", []Check{ok, bad}, 503, HealthResponse{"unavailable", map[string]string{"mysql": "ok", "mongo": "unavailable"}}},
		{"no checks", nil, 200, HealthResponse{"ok", map[string]string{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			HealthHandler(0, tt.checks...).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d", rec.Code)
			}
			if strings.Contains(rec.Body.String(), "10.1.2.3") {
				t.Error("response leaks error details")
			}
			var got HealthResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.Status != tt.want.Status || len(got.Checks) != len(tt.want.Checks) {
				t.Fatalf("body = %+v", got)
			}
			for k, v := range tt.want.Checks {
				if got.Checks[k] != v {
					t.Errorf("check %s = %q, want %q", k, got.Checks[k], v)
				}
			}
		})
	}
	if logs.FilterMessage("health check failed").FilterFieldKey("check").Len() != 1 {
		t.Error("failed check should be logged with its error")
	}
}

func TestHealthHandlerTimeout(t *testing.T) {
	observe(t)
	hang := Check{Name: "slow", Ping: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	rec := httptest.NewRecorder()
	start := time.Now()
	HealthHandler(50*time.Millisecond, hang).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != 503 || time.Since(start) > time.Second {
		t.Fatalf("status=%d after %v", rec.Code, time.Since(start))
	}
}
