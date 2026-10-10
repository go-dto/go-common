package logger

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestSpanIDFallbackRemainsUnique(t *testing.T) {
	old := readSpanRandom
	t.Cleanup(func() { readSpanRandom = old })

	readSpanRandom = func([]byte) (int, error) { return 0, errors.New("entropy unavailable") }
	first, second := newSpanID(), newSpanID()
	if len(first) != 16 || len(second) != 16 || first == second {
		t.Fatalf("fallback span IDs must be unique 16-character hex strings: %q %q", first, second)
	}

	readSpanRandom = func(dst []byte) (int, error) {
		dst[0] = 1
		return 1, nil
	}
	partial := newSpanID()
	if len(partial) != 16 || partial == "0100000000000000" {
		t.Fatalf("partial entropy must use the fallback: %q", partial)
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

	client := WrapHTTPClient(nil, HTTPClientOptions{
		Peer:  "client_webhook",
		Route: func(*http.Request) string { return "/hook" },
	})
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
		cm[KeyLayer] != LayerClient || cm["status_code"] != int64(200) || cm[KeyRoute] != "/hook" {
		t.Fatalf("bad fields: %v", cm)
	}
	if _, ok := cm["url"]; ok {
		t.Fatalf("raw URL must be omitted by default: %v", cm)
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

func TestHTTPTransportProductionOptions(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := WrapHTTPClient(nil, HTTPClientOptions{DisableSuccessLog: true, SlowThreshold: -1})
	if client.Timeout != DefaultHTTPClientTimeout {
		t.Fatalf("default timeout = %v", client.Timeout)
	}
	resp, err := client.Get(srv.URL + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if logs.Len() != 0 {
		t.Fatalf("successful call should be suppressed: %v", logs.All())
	}

	resp, err = client.Get(srv.URL + "/fail")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if logs.FilterMessage("http outbound error status").Len() != 1 {
		t.Fatal("error status must still be logged")
	}
}

func TestHTTPMiddleware(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	var gotTrace, gotHeader string
	h := HTTPMiddleware(HTTPMiddlewareOptions{
		Route: func(*http.Request) string { return "/users/:id" },
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTrace, _ = ctxid.FromContext(r.Context())
		gotHeader = r.Header.Get(ctxid.Header)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	}))
	req := httptest.NewRequest(http.MethodPost, "/users/123?token=secret", nil)
	req.Header.Set(ctxid.Header, "request-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if gotTrace != "request-123" || gotHeader != "request-123" || rec.Header().Get(ctxid.Header) != "request-123" {
		t.Fatalf("request id propagation failed: ctx=%q request=%q response=%q", gotTrace, gotHeader, rec.Header().Get(ctxid.Header))
	}
	e := logs.FilterMessage("http inbound").All()
	if len(e) != 1 {
		t.Fatalf("want one inbound log, got %v", logs.All())
	}
	fields := e[0].ContextMap()
	if fields[KeyTraceID] != "request-123" || fields[KeyRoute] != "/users/:id" ||
		fields[KeyStatusCode] != int64(http.StatusCreated) ||
		fields[KeyRespBytes] != int64(2) {
		t.Fatalf("unexpected access fields: %v", fields)
	}
	if _, ok := fields[KeyPath]; ok {
		t.Fatalf("raw path must be omitted when a route template is available: %v", fields)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/", nil)
	badReq.Header.Set(ctxid.Header, strings.Repeat("x", ctxid.MaxLength+1))
	badRec := httptest.NewRecorder()
	h.ServeHTTP(badRec, badReq)
	if id := badRec.Header().Get(ctxid.Header); len(id) != 32 || id == badReq.Header.Get(ctxid.Header) || gotHeader != id {
		t.Fatalf("invalid request id was not replaced consistently: response=%q request=%q", id, gotHeader)
	}
	last := logs.FilterMessage("http inbound").All()[1].ContextMap()
	if _, ok := last[KeyPath]; ok {
		t.Fatalf("raw path must be omitted by default: %v", last)
	}
}

func TestHTTPLoggingRawURLAndPathRequireOptIn(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	transport := NewHTTPTransport(testRoundTripper(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
	}), HTTPClientOptions{LogURL: true})
	req := httptest.NewRequest(http.MethodGet, "https://example.test/patients/123?token=secret", nil)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	fields := logs.FilterMessage("http outbound").All()[0].ContextMap()
	if !strings.Contains(fields["url"].(string), "/patients/123") || strings.Contains(fields["url"].(string), "secret") {
		t.Fatalf("explicit URL log must still redact query values: %v", fields)
	}

	h := HTTPMiddleware(HTTPMiddlewareOptions{LogRawPath: true})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/patients/123", nil))
	fields = logs.FilterMessage("http inbound").All()[0].ContextMap()
	if fields[KeyPath] != "/patients/123" {
		t.Fatalf("explicit raw path opt-in missing: %v", fields)
	}
}

func TestHTTPTransportDoesNotLogRawTransportError(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	transport := NewHTTPTransport(testRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("secret patient transport detail")
	}), HTTPClientOptions{})
	req := httptest.NewRequest(http.MethodGet, "https://example.test/patients/123", nil)
	if _, err := transport.RoundTrip(req); err == nil {
		t.Fatal("expected transport error")
	}

	entries := logs.FilterMessage("http outbound failed").All()
	if len(entries) != 1 {
		t.Fatalf("want one failure log, got %v", logs.All())
	}
	fields := entries[0].ContextMap()
	if fields["error_kind"] != "transport" {
		t.Fatalf("stable error kind missing: %v", fields)
	}
	if strings.Contains(fmt.Sprint(fields), "secret patient transport detail") {
		t.Fatalf("raw transport error leaked: %v", fields)
	}
}

func TestHTTPTransportHandlesMalformedRoundTripper(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()

	transport := NewHTTPTransport(testRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, nil
	}), HTTPClientOptions{})
	req := httptest.NewRequest(http.MethodGet, "https://example.test/health", nil)
	resp, err := transport.RoundTrip(req)
	if resp != nil || err == nil {
		t.Fatalf("want a safe error for malformed transport, response=%v error=%v", resp, err)
	}
	if logs.FilterMessage("http outbound failed").Len() != 1 {
		t.Fatalf("malformed transport failure not logged: %v", logs.All())
	}
}

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestHTTPMiddlewareLogsAndRethrowsPanic(t *testing.T) {
	core, logs := observerCore()
	defer ReplaceForTest(zap.New(core))()
	h := HTTPMiddleware(HTTPMiddlewareOptions{})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("sensitive panic value")
	}))

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic must be rethrown")
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/panic", nil))
	}()

	e := logs.FilterMessage("http inbound panic").All()
	if len(e) != 1 || e[0].Level != zapcore.ErrorLevel {
		t.Fatalf("panic log missing: %v", logs.All())
	}
	if strings.Contains(fmt.Sprint(e[0].ContextMap()), "sensitive panic value") {
		t.Fatalf("panic value leaked: %v", e[0].ContextMap())
	}
}

func TestLongOpCompletesOnLoggerInstalledAfterReInit(t *testing.T) {
	oldGrace := closeGracePeriod
	closeGracePeriod = 10 * time.Millisecond
	t.Cleanup(func() { closeGracePeriod = oldGrace })

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Outputs = []string{OutputFile}
	cfg.File.Path = filepath.Join(dir, "old.log")
	MustInit(cfg)
	_, op := StartOp(context.Background(), LayerService, "long.op")

	cfg.File.Path = filepath.Join(dir, "new.log")
	MustInit(cfg)
	time.Sleep(30 * time.Millisecond)
	op.End(errors.New("late failure"))
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	MustInit(DefaultConfig())

	newLog, err := os.ReadFile(filepath.Join(dir, "new.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(newLog), `"msg":"op failed"`) || !strings.Contains(string(newLog), "late failure") {
		t.Fatalf("completion did not use new logger: %s", newLog)
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
