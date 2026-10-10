package singleton

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

func TestShimDelegatesAndKeepsCaller(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	defer logger.ReplaceForTest(zap.New(core, zap.AddCaller()))()

	Info("hello")
	Warnf("n=%d", 1)
	Error(nil) // must not panic anymore
	WithContext(ctxid.WithContext(context.Background(), "t1")).Info("ctx")
	req := httptest.NewRequest("GET", "/patients/secret-id", nil)
	req.Pattern = "/patients/{id}"
	WithRequest(req).Info("request")

	all := logs.All()
	if len(all) != 5 {
		t.Fatalf("want 5 entries, got %d", len(all))
	}
	for _, e := range all[:3] {
		if !strings.HasSuffix(e.Caller.File, "singleton_test.go") {
			t.Errorf("caller should be the test file, got %s", e.Caller.File)
		}
	}
	if all[3].ContextMap()[logger.KeyTraceID] != "t1" {
		t.Errorf("trace_id missing: %v", all[3].ContextMap())
	}
	if fields := all[4].ContextMap(); fields[logger.KeyRoute] != "/patients/{id}" || fields[logger.KeyPath] != nil {
		t.Errorf("request must use route template without raw path: %v", fields)
	}

	Debug("debug")
	Warn("warn")
	Debugf("debug %d", 1)
	Infof("info %d", 1)
	Errorf("error %d", 1)
	GetSugaredLogger().Info("sugared")
	WithRequestID("t2").Info("request id")
	SugaredWithContext(ctxid.WithContext(context.Background(), "t3")).Info("context sugar")
	SugaredWithRequest(req).Info("request sugar")
	Error(errors.New("expected error"), zap.String(logger.KeyError, "classified"))

	var hookCalls atomic.Int64
	AddHook(func(zapcore.Entry) error {
		hookCalls.Add(1)
		return nil
	})
	Info("hooked")
	if hookCalls.Load() != 1 {
		t.Fatalf("hook calls = %d, want 1", hookCalls.Load())
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Panic must rethrow")
			}
		}()
		Panic("expected panic")
	}()
}

func TestInitLoggerWithDir(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { logger.MustInit(logger.DefaultConfig()) })

	InitLoggerWithDir(dir, "", &Options{Level: LLvlDevelopment})
	Info("compatibility file output")
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "compatibility file output") {
		t.Fatalf("compatibility log missing: %s", body)
	}

	// Also cover the stdout-only/default-options compatibility path.
	InitLoggerWithDir("", "", nil)
}
