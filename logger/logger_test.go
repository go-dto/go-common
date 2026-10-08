package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/logger/ctxid"
)

func observerCore() (zapcore.Core, *observer.ObservedLogs) { return observer.New(zapcore.DebugLevel) }

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func observe(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	restore := ReplaceForTest(zap.New(core))
	t.Cleanup(restore)
	return logs
}

func TestConfigFromEnv(t *testing.T) {
	env := map[string]string{
		"SERVICE_NAME":          "webhook",
		"APP_ENV":               "prod",
		"LOG_LEVEL":             "WARN",
		"LOG_FORMAT":            "console",
		"LOG_OUTPUT":            "stdout, file",
		"LOG_FILE_PATH":         "/tmp/x.log",
		"LOG_FILE_MAX_AGE_DAYS": "3",
		"LOG_SAMPLING":          "100,10",
		"LOG_CALLER":            "false",
		"LOG_FUNCTION":          "true",
		"LOG_SLOW_OP_MS":        "250",
	}
	cfg, err := ConfigFromLookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Level != "warn" || cfg.Format != "console" || cfg.ServiceName != "webhook" ||
		len(cfg.Outputs) != 2 || cfg.File.MaxAge != 72*time.Hour || cfg.Sampling.Thereafter != 10 || cfg.EnableCaller ||
		!cfg.EnableFunction || cfg.SlowOpThreshold != 250*time.Millisecond {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []map[string]string{
		{"LOG_LEVEL": "verbose"},
		{"LOG_FORMAT": "xml"},
		{"LOG_OUTPUT": "file"}, // missing path
		{"LOG_OUTPUT": "kafka"},
		{"LOG_FILE_MAX_SIZE_MB": "abc"},
		{"LOG_SAMPLING": "100"},
		{"LOG_CALLER": "maybe"},
	}
	for _, env := range bad {
		if _, err := ConfigFromLookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok }); err == nil {
			t.Errorf("expected error for %v", env)
		}
	}
}

func TestJSONOutputShape(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.ServiceName, cfg.Environment, cfg.Version = "webhook", "test", "1.2.3"
	cfg.Outputs = []string{OutputFile}
	cfg.File.Path = filepath.Join(dir, "app.log")
	inst, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	inst.Logger.Info("hello", zap.Duration("d", 1500*time.Millisecond))
	inst.Logger.Debug("hidden")
	if err := inst.Close(); err != nil {
		t.Fatal(err)
	}

	raw, _ := os.ReadFile(cfg.File.Path)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("want 1 line (debug filtered), got %d: %s", len(lines), raw)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"time", "level", "msg", "caller", "service", "env", "version"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q in %s", k, lines[0])
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, m["time"].(string)); err != nil {
		t.Errorf("time not RFC3339: %v", m["time"])
	}
	if m["level"] != "info" || m["d"].(float64) != 1500 {
		t.Errorf("unexpected entry: %s", lines[0])
	}
}

func TestFromContextAddsTraceIDAndFields(t *testing.T) {
	logs := observe(t)

	ctx := ctxid.WithContext(context.Background(), "trace-123")
	ctx = WithFields(ctx, zap.String("org_id", "o1"), zap.String(KeyRequestID, "r1"))
	ctx = WithFields(ctx, zap.String("org_id", "o2")) // override, not duplicate
	FromContext(ctx).Info("msg")

	e := logs.All()[0]
	keys := map[string]int{}
	for _, f := range e.Context {
		keys[f.Key]++
	}
	if keys["org_id"] != 1 || keys[KeyTraceID] != 1 || keys[KeyRequestID] != 1 {
		t.Fatalf("unexpected fields: %v", e.ContextMap())
	}
	cm := e.ContextMap()
	if cm["org_id"] != "o2" || cm[KeyTraceID] != "trace-123" {
		t.Fatalf("unexpected values: %v", cm)
	}
}

func TestFromContextNil(t *testing.T) {
	observe(t)
	//nolint:staticcheck // deliberately passing nil
	FromContext(nil).Info("ok")
	_ = WithFields(nil, zap.String("a", "b"))
}

func TestSetLevelAndHandler(t *testing.T) {
	cfg := DefaultConfig()
	if err := Init(cfg); err != nil {
		t.Fatal(err)
	}
	if err := SetLevel("debug"); err != nil || GetLevel() != zapcore.DebugLevel {
		t.Fatalf("SetLevel failed: %v %v", err, GetLevel())
	}
	if SetLevel("nope") == nil {
		t.Fatal("expected error")
	}

	req := httptest.NewRequest(http.MethodPut, "/log/level", strings.NewReader(`{"level":"error"}`))
	rec := httptest.NewRecorder()
	LevelHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || GetLevel() != zapcore.ErrorLevel {
		t.Fatalf("handler: %d %s level=%v", rec.Code, rec.Body, GetLevel())
	}
	_ = SetLevel("info")
}

func TestReInitAndStdlogRedirect(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Outputs = []string{OutputFile}
	cfg.File.Path = filepath.Join(dir, "a.log")
	MustInit(cfg)
	log.Print("from stdlib")

	cfg2 := cfg
	cfg2.File.Path = filepath.Join(dir, "b.log")
	MustInit(cfg2) // closes a.log
	L().Info("second")
	_ = Close()
	MustInit(DefaultConfig())

	a, _ := os.ReadFile(cfg.File.Path)
	b, _ := os.ReadFile(cfg2.File.Path)
	if !bytes.Contains(a, []byte("from stdlib")) || !bytes.Contains(a, []byte(`"logger":"stdlog"`)) {
		t.Errorf("stdlib log not redirected: %s", a)
	}
	if !bytes.Contains(b, []byte("second")) || bytes.Contains(a, []byte("second")) {
		t.Errorf("re-init did not switch file: a=%s b=%s", a, b)
	}
}

func TestConcurrentUse(t *testing.T) {
	// Run with -race: Init, AddHook, SetLevel and logging must not race.
	cfg := DefaultConfig()
	cfg.Outputs = []string{OutputFile}
	cfg.File.Path = filepath.Join(t.TempDir(), "c.log")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(4)
		go func() { defer wg.Done(); _ = Init(cfg) }()
		go func() { defer wg.Done(); AddHook(func(zapcore.Entry) error { return nil }) }()
		go func() { defer wg.Done(); _ = SetLevel("debug") }()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				FromContext(context.Background()).Info("x")
				S().Infof("y %d", j)
			}
		}()
	}
	wg.Wait()
	MustInit(DefaultConfig())
}

func TestIgnoreBenignSyncErr(t *testing.T) {
	inst, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Sync(); err != nil {
		t.Fatalf("stdout sync should not error: %v", err)
	}
}
