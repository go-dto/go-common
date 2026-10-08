package database

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
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
// otherwise — suitable for a Kubernetes readiness probe. Error details are
// logged at warn, not returned, so the endpoint does not leak hostnames or
// driver messages. timeout <= 0 uses DefaultHealthTimeout.
//
// Use it for readiness only: a liveness probe that fails while the database
// is down makes Kubernetes restart healthy pods for no benefit.
func HealthHandler(timeout time.Duration, checks ...Check) http.Handler {
	if timeout <= 0 {
		timeout = DefaultHealthTimeout
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		resp := HealthResponse{Status: "ok", Checks: make(map[string]string, len(checks))}
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, c := range checks {
			wg.Go(func() {
				err := c.Ping(ctx)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					resp.Status = "unavailable"
					resp.Checks[c.Name] = "unavailable"
					logger.FromContext(ctx).Warn("health check failed", zap.String("check", c.Name), zap.Error(err))
					return
				}
				resp.Checks[c.Name] = "ok"
			})
		}
		wg.Wait()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if resp.Status != "ok" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}
