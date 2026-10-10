package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-dto/go-common/logger"
)

// DefaultHealthTimeout bounds all checks of one HealthHandler request.
const DefaultHealthTimeout = 2 * time.Second

// Check is one dependency probed by HealthHandler.
type Check struct {
	Name string
	Ping func(ctx context.Context) error
}

// HealthResponse is the JSON body written by HealthHandler.
type HealthResponse struct {
	Status string            `json:"status"` // "ok" | "unavailable"
	Checks map[string]string `json:"checks"` // name → "ok" | "unavailable"
}

// HealthHandler runs checks concurrently and answers 200 when all pass, 503
// otherwise — suitable for a Kubernetes readiness probe. Failures are logged
// once per unhealthy transition using a stable error kind; raw driver errors
// are neither logged nor returned. timeout <= 0 uses DefaultHealthTimeout.
//
// Use it for readiness only: a liveness probe that fails while the database
// is down makes Kubernetes restart healthy pods for no benefit.
func HealthHandler(timeout time.Duration, checks ...Check) http.Handler {
	if timeout <= 0 {
		timeout = DefaultHealthTimeout
	}
	prepared := prepareChecks(checks)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		resp := HealthResponse{Status: "ok", Checks: make(map[string]string, len(prepared))}
		results := make(chan healthResult, len(prepared))
		completed := make([]bool, len(prepared))
		for i, c := range prepared {
			resp.Checks[c.name] = "unavailable"
			go func() {
				err := c.configErr
				if err == nil {
					err = c.run(ctx)
				}
				results <- healthResult{index: i, err: err}
			}()
		}

		for remaining := len(prepared); remaining > 0; {
			select {
			case result := <-results:
				if completed[result.index] {
					continue
				}
				completed[result.index] = true
				remaining--
				check := prepared[result.index]
				if result.err != nil {
					resp.Status = "unavailable"
					if check.failed.CompareAndSwap(false, true) {
						logger.FromContext(r.Context()).Warn("health check failed",
							zap.String("check", check.name), zap.String("error_kind", healthErrorKind(result.err)))
					}
					continue
				}
				resp.Checks[check.name] = "ok"
				if check.failed.CompareAndSwap(true, false) {
					logger.FromContext(r.Context()).Info("health check recovered", zap.String("check", check.name))
				}
			case <-ctx.Done():
				resp.Status = "unavailable"
				for i, check := range prepared {
					if completed[i] {
						continue
					}
					if check.failed.CompareAndSwap(false, true) {
						logger.FromContext(r.Context()).Warn("health check timed out",
							zap.String("check", check.name), zap.String("error_kind", healthErrorKind(ctx.Err())))
					}
				}
				remaining = 0
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if resp.Status != "ok" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}

type preparedCheck struct {
	name      string
	ping      func(context.Context) error
	configErr error
	gate      chan struct{}
	failed    *atomic.Bool
}

// run prevents a non-cooperative dependency check from accumulating an
// unbounded number of concurrent calls across repeated readiness requests.
func (c preparedCheck) run(ctx context.Context) error {
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	return safeHealthPing(ctx, c.ping)
}

type healthResult struct {
	index int
	err   error
}

func prepareChecks(checks []Check) []preparedCheck {
	prepared := make([]preparedCheck, len(checks))
	seen := make(map[string]struct{}, len(checks))
	for i, check := range checks {
		name := strings.TrimSpace(check.Name)
		var configErr error
		if name == "" {
			name = fmt.Sprintf("check_%d", i+1)
			configErr = errors.New("database: health check name is required")
		}
		if _, duplicate := seen[name]; duplicate {
			original := name
			for suffix := i + 1; ; suffix++ {
				candidate := fmt.Sprintf("%s_%d", original, suffix)
				if _, exists := seen[candidate]; !exists {
					name = candidate
					break
				}
			}
			configErr = fmt.Errorf("database: duplicate health check name %q", original)
		}
		seen[name] = struct{}{}
		if check.Ping == nil {
			configErr = errors.New("database: health check ping function is required")
		}
		prepared[i] = preparedCheck{
			name: name, ping: check.Ping, configErr: configErr,
			gate: make(chan struct{}, 1), failed: &atomic.Bool{},
		}
	}
	return prepared
}

func healthErrorKind(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "dependency"
	}
}

func safeHealthPing(ctx context.Context, ping func(context.Context) error) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("database: health check panicked")
		}
	}()
	return ping(ctx)
}
