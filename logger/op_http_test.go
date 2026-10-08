package logger

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger/ctxid"
)

func TestStartOpFieldsAndCaller(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core, zap.AddCaller()))()

	ctx := ctxid.WithContext(context.Background(), "trace-1")
	ctx = WithFields(ctx, zap.String(KeyRequestID, "req-1"))

	ctx, op := StartOp(ctx, LayerService, "webhook.Send", zap.String("org_id", "o1"))
	FromContext(ctx).Info("inside")
	op.End(nil)

	all := logs.All()
	if len(all) != 3 { // started, inside, finished
		t.Fatalf("want 3 entries, got %d", len(all))
	}
	for _, e := range all {
		cm := e.ContextMap()
		if cm[KeyLayer] != LayerService || cm[KeyOperation] != "webhook.Send" || cm[KeyTraceID] != "trace-1" ||
			cm[KeyRequestID] != "req-1" || cm["org_id"] != "o1" {
			t.Errorf("missing fields in %q: %v", e.Message, cm)
		}
		if !strings.HasSuffix(e.Caller.File, "op_http_test.go") {
			t.Errorf("caller of %q should be the test, got %s", e.Message, e.Caller.File)
		}
	}
	if _, ok := all[2].ContextMap()[KeyDurationMs]; !ok || all[2].Message != "op finished" {
		t.Errorf("bad end entry: %v", all[2])
	}
}

func TestSpanIDPairsAndNests(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	ctx, svc := StartOp(context.Background(), LayerService, "svc")
	for i := 0; i < 2; i++ { // same op twice: spans must differ
		_, repo := StartOp(ctx, LayerRepository, "repo")
		repo.End(nil)
	}
	FromContext(ctx).Info("after repo") // back in svc span
	svc.End(nil)

	bySpan := map[string][]string{}
	for _, e := range logs.All() {
		cm := e.ContextMap()
		bySpan[cm[KeySpanID].(string)] = append(bySpan[cm[KeySpanID].(string)], e.Message)
		if cm[KeyOperation] == "repo" && cm[KeyParentSpanID] != svc.SpanID() {
			t.Errorf("repo parent_span_id = %v, want %s", cm[KeyParentSpanID], svc.SpanID())
		}
		if cm[KeyOperation] == "svc" {
			if _, ok := cm[KeyParentSpanID]; ok {
				t.Errorf("root op must not have parent_span_id: %v", cm)
			}
		}
	}
	if len(bySpan) != 3 {
		t.Fatalf("want 3 spans, got %v", bySpan)
	}
	if got := bySpan[svc.SpanID()]; len(got) != 3 || got[0] != "op started" || got[2] != "op finished" {
		t.Fatalf("svc span lines: %v", got)
	}
}

func TestOpErrorLoggedOnce(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	base := errors.New("no rows")
	handler := func(ctx context.Context) (err error) {
		ctx, op := StartOp(ctx, LayerHandler, "h")
		defer op.Done(&err)
		return func(ctx context.Context) (err error) {
			ctx, op := StartOp(ctx, LayerService, "s")
			defer op.Done(&err)
			return func(ctx context.Context) (err error) {
				_, op := StartOp(ctx, LayerRepository, "r")
				defer op.Done(&err)
				return base
			}(ctx)
		}(ctx)
	}
	err := handler(context.Background())
	_ = fmt.Errorf("wrap: %w", err)

	errs := logs.FilterLevelExact(zapcore.ErrorLevel).All()
	if len(errs) != 1 || errs[0].ContextMap()[KeyLayer] != LayerRepository {
		t.Fatalf("want exactly one error log from repository, got %v", errs)
	}
	if n := logs.FilterMessage("op failed").Len(); n != 3 {
		t.Fatalf("want 3 'op failed' entries (1 error + 2 debug), got %d", n)
	}
}

func TestOpSlow(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()
	global.Load().inst.slowOp = 1 // 1ns: everything is slow
	_, op := StartOp(context.Background(), LayerRepository, "slow")
	op.End(nil)
	if logs.FilterMessage("op slow").Len() != 1 {
		t.Fatal("expected op slow")
	}
}

func TestHTTPTransportPropagatesAndLogs(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(ctxid.Header)
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()

	client := WrapHTTPClient(nil, HTTPClientOptions{Peer: "client_webhook"})
	ctx := ctxid.WithContext(context.Background(), "trace-9")
	ctx = WithFields(ctx, zap.String(KeyRequestID, "req-9"))

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/hook?token=secret", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotHeader != "trace-9" {
		t.Fatalf("X-Request-ID not propagated: %q", gotHeader)
	}
	if req.Header.Get(ctxid.Header) != "" {
		t.Fatal("caller's request must not be modified")
	}
	e := logs.FilterMessage("http outbound").All()
	if len(e) != 1 {
		t.Fatalf("want 1 outbound log, got %d", logs.Len())
	}
	cm := e[0].ContextMap()
	if cm[KeyTraceID] != "trace-9" || cm[KeyRequestID] != "req-9" || cm[KeyPeer] != "client_webhook" ||
		cm[KeyLayer] != LayerClient || cm["status_code"] != int64(200) || strings.Contains(cm["url"].(string), "secret") {
		t.Fatalf("bad fields: %v", cm)
	}

	req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/fail", nil)
	resp, _ = client.Do(req2)
	resp.Body.Close()
	if logs.FilterMessage("http outbound error status").FilterLevelExact(zapcore.WarnLevel).Len() != 1 {
		t.Fatal("want warn for 502")
	}

	_, op := StartOp(ctx, LayerClient, "send")
	req3, _ := http.NewRequestWithContext(op.Ctx(), http.MethodGet, "http://127.0.0.1:1/unreachable", nil)
	_, err = client.Do(req3)
	op.End(err)
	if logs.FilterLevelExact(zapcore.ErrorLevel).Len() != 1 {
		t.Fatalf("transport error must be logged at error exactly once, got %d", logs.FilterLevelExact(zapcore.ErrorLevel).Len())
	}
}

func TestEnsureTraceID(t *testing.T) {
	ctx, id := ctxid.Ensure(context.Background())
	if len(id) != 32 {
		t.Fatalf("bad id %q", id)
	}
	if _, id2 := ctxid.Ensure(ctx); id2 != id {
		t.Fatal("Ensure must keep existing id")
	}
}

func TestFunctionKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableFunction = true
	cfg.Outputs = []string{OutputFile}
	cfg.File.Path = t.TempDir() + "/f.log"
	inst, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	inst.Logger.Info("x")
	_ = inst.Close()
	raw := readFile(t, cfg.File.Path)
	if !strings.Contains(raw, `"func":"github.com/go-dto/go-common/logger.TestFunctionKey"`) {
		t.Fatalf("func missing: %s", raw)
	}
}
