package singleton

import (
	"context"
	"strings"
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

	all := logs.All()
	if len(all) != 4 {
		t.Fatalf("want 4 entries, got %d", len(all))
	}
	for _, e := range all[:3] {
		if !strings.HasSuffix(e.Caller.File, "singleton_test.go") {
			t.Errorf("caller should be the test file, got %s", e.Caller.File)
		}
	}
	if all[3].ContextMap()[logger.KeyTraceID] != "t1" {
		t.Errorf("trace_id missing: %v", all[3].ContextMap())
	}
}
