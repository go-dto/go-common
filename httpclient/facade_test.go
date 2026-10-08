package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

func observeFacade(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(logger.ReplaceForTest(zap.New(core)))
	return logs
}

func TestFacadeBuildsSafeRequestAndBuffersResponse(t *testing.T) {
	logs := observeFacade(t)
	type requestSnapshot struct {
		Method, EscapedPath, Query, DefaultHeader, RequestHeader, Authorization, RequestID string
	}
	got := make(chan requestSnapshot, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- requestSnapshot{
			Method: r.Method, EscapedPath: r.URL.EscapedPath(), Query: r.URL.RawQuery,
			DefaultHeader: r.Header.Get("X-Default"), RequestHeader: r.Header.Get("X-Request"),
			Authorization: r.Header.Get("Authorization"), RequestID: r.Header.Get(ctxid.Header),
		}
		w.Header().Add("X-Result", "one")
		w.Header().Add("X-Result", "two")
		_ = json.NewEncoder(w).Encode(map[string]string{"name": "Rasyid"})
	}))
	defer srv.Close()

	client, err := New(Config{
		Peer: "profile-api", BaseURL: srv.URL + "/api",
		DefaultHeaders: HeaderMap{"X-Default": "configured"},
		Auth:           BearerToken("secret-token"),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := ctxid.WithContext(context.Background(), "trace-123")
	ctx = logger.WithFields(ctx, zap.String(logger.KeyLayer, logger.LayerService))
	response, err := client.Get(ctx, "/users/{id}",
		Path("id", "a/b"),
		Query("search", "space & plus+"),
		QueryValues("tag", "one", "two"),
		Header("X-Request", "value"),
		Operation("profile.get"),
	)
	if err != nil {
		t.Fatal(err)
	}

	snapshot := <-got
	if snapshot.Method != "GET" || snapshot.EscapedPath != "/api/users/a%2Fb" {
		t.Fatalf("unexpected request: %+v", snapshot)
	}
	if snapshot.Query != "search=space+%26+plus%2B&tag=one&tag=two" {
		t.Fatalf("query = %q", snapshot.Query)
	}
	if snapshot.DefaultHeader != "configured" || snapshot.RequestHeader != "value" ||
		snapshot.Authorization != "Bearer secret-token" || snapshot.RequestID != "trace-123" {
		t.Fatalf("headers = %+v", snapshot)
	}

	if !response.IsSuccess() || response.StatusCode != 200 || response.Attempts != 1 {
		t.Fatalf("response = %+v", response)
	}
	if got := response.HeaderValues("X-Result"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("response headers = %v", got)
	}
	var decoded map[string]string
	if err := response.JSON(&decoded); err != nil || decoded["name"] != "Rasyid" {
		t.Fatalf("decoded=%v err=%v", decoded, err)
	}
	copyBody := response.Bytes()
	copyBody[0] = 'X'
	if response.Bytes()[0] == 'X' {
		t.Fatal("Response.Bytes must return a copy")
	}

	entries := logs.FilterMessage("http outbound completed").All()
	if len(entries) != 1 {
		t.Fatalf("final logs = %d, all=%v", len(entries), logs.All())
	}
	fields := entries[0].ContextMap()
	if fields[logger.KeyLayer] != logger.LayerClient || fields[logger.KeyPeer] != "profile-api" || fields["operation"] != "profile.get" ||
		fields["attempt_count"] != int64(1) || fields["outcome"] != "success" || fields[logger.KeyTraceID] != "trace-123" {
		t.Fatalf("log fields = %v", fields)
	}
	serialized := entries[0].Message + strings.TrimSpace(strings.Join([]string{
		string(mustJSON(t, fields)),
	}, ""))
	for _, secret := range []string{"secret-token", "space & plus+", "a/b", srv.URL} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("log leaked %q: %s", secret, serialized)
		}
	}
}

func TestFacadeRetriesAndLogsOneFinalResult(t *testing.T) {
	noSleep(t)
	logs := observeFacade(t)
	var calls atomic.Int32
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"p-1"}`)
	}))
	defer srv.Close()

	var retryEvents []RetryEvent
	var resultEvents []ResultEvent
	client := MustNew(Config{
		Peer: "payment-api", BaseURL: srv.URL,
		OnRetry:  func(event RetryEvent) { retryEvents = append(retryEvents, event) },
		OnResult: func(_ context.Context, event ResultEvent) { resultEvents = append(resultEvents, event) },
	})
	response, err := client.Post(context.Background(), "/payments",
		JSON(map[string]int{"amount": 1000}),
		IdempotencyKey("request-1"),
		Operation("payment.create"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 201 || response.Attempts != 2 || calls.Load() != 2 || len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatalf("response=%+v calls=%d bodies=%v", response, calls.Load(), bodies)
	}
	if len(retryEvents) != 1 || retryEvents[0].Method != MethodPost ||
		retryEvents[0].Operation != "payment.create" || retryEvents[0].StatusCode != 503 {
		t.Fatalf("retry events = %+v", retryEvents)
	}
	if len(resultEvents) != 1 || resultEvents[0].Attempts != 2 ||
		resultEvents[0].Outcome != "success_after_retry" || resultEvents[0].Operation != "payment.create" {
		t.Fatalf("result events = %+v", resultEvents)
	}
	if logs.FilterMessage("http outbound recovered").Len() != 1 ||
		logs.FilterMessage("http outbound error status").Len() != 0 ||
		logs.FilterMessage("http outbound completed").Len() != 0 {
		t.Fatalf("unexpected final logs: %v", logs.All())
	}
}

func TestFacadeStatusErrorsAndRetryRules(t *testing.T) {
	noSleep(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/server-error" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "safe for caller")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	client := MustNew(Config{Peer: "upstream", BaseURL: srv.URL, DisableLogging: true})

	response, err := client.Get(context.Background(), "/server-error")
	if !IsStatus(err, 500) || IsRetryExhausted(err) || response == nil || response.Attempts != 1 || response.Text() != "safe for caller" {
		t.Fatalf("500 response=%+v err=%v calls=%d", response, err, calls.Load())
	}

	calls.Store(0)
	response, err = client.Post(context.Background(), "/unavailable", JSON(map[string]string{"x": "y"}))
	if !IsStatus(err, 503) || IsRetryExhausted(err) || response.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("POST without key response=%+v err=%v calls=%d", response, err, calls.Load())
	}

	calls.Store(0)
	response, err = client.Post(context.Background(), "/unavailable",
		JSON(map[string]string{"x": "y"}), IdempotencyKey("same-request"))
	if !IsStatus(err, 503) || !IsRetryExhausted(err) || response.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("POST with key response=%+v err=%v calls=%d", response, err, calls.Load())
	}
}

func TestFacadeAcceptedStatusAndResponseLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cached" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, "12345")
	}))
	defer srv.Close()
	client := MustNew(Config{
		Peer: "cache-api", BaseURL: srv.URL, MaxResponseBodyBytes: 4,
		DisableLogging: true,
	})

	response, err := client.Get(context.Background(), "/cached", AcceptStatus(304))
	if err != nil || !response.IsSuccess() || response.StatusCode != 304 {
		t.Fatalf("accepted response=%+v err=%v", response, err)
	}

	response, err = client.Get(context.Background(), "/large")
	if KindOf(err) != KindResponseTooLarge || response == nil || len(response.Bytes()) != 4 {
		t.Fatalf("large response=%+v err=%v", response, err)
	}
}

func TestFacadeTimeoutAndSafeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	client := MustNew(Config{
		Peer: "slow-api", BaseURL: srv.URL, DisableRetry: true, DisableLogging: true,
	})

	_, err := client.Get(context.Background(), "/patients/{id}",
		Path("id", "sensitive-patient-id"), RequestTimeout(20*time.Millisecond), Operation("patient.get"))
	if !IsTimeout(err) {
		t.Fatalf("err=%v kind=%s", err, KindOf(err))
	}
	if text := err.Error(); strings.Contains(text, srv.URL) || strings.Contains(text, "sensitive-patient-id") {
		t.Fatalf("unsafe error: %s", text)
	}
	encoded, marshalErr := json.Marshal(err)
	if marshalErr != nil || strings.Contains(string(encoded), srv.URL) || strings.Contains(string(encoded), "sensitive-patient-id") {
		t.Fatalf("unsafe serialized error: %s, marshalErr=%v", encoded, marshalErr)
	}
}

func TestFacadeAuthAndBodyHelpers(t *testing.T) {
	tests := []struct {
		name     string
		auth     Authenticator
		body     RequestOption
		wantAuth string
		wantType string
		wantBody string
	}{
		{"basic", BasicAuth("user", "pass"), Text("hello"), "Basic dXNlcjpwYXNz", "text/plain; charset=utf-8", "hello"},
		{"api key", APIKey("X-API-Key", "key-value"), Form(map[string]string{"b": "2", "a": "1"}), "key-value", "application/x-www-form-urlencoded", "a=1&b=2"},
		{"bytes", nil, Bytes("application/octet-stream", []byte{1, 2}), "", "application/octet-stream", string([]byte{1, 2})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth := r.Header.Get("Authorization")
				if tt.name == "api key" {
					auth = r.Header.Get("X-API-Key")
				}
				body, _ := io.ReadAll(r.Body)
				if auth != tt.wantAuth || r.Header.Get("Content-Type") != tt.wantType || string(body) != tt.wantBody {
					t.Errorf("auth=%q type=%q body=%q", auth, r.Header.Get("Content-Type"), body)
				}
				w.WriteHeader(204)
			}))
			defer srv.Close()
			client := MustNew(Config{Peer: "auth-api", BaseURL: srv.URL, Auth: tt.auth, DisableLogging: true})
			if _, err := client.Post(context.Background(), "/body", tt.body); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFacadeInvalidConfigurationAndOptions(t *testing.T) {
	tests := []Config{
		{BaseURL: "https://example.com"},
		{Peer: "peer"},
		{Peer: "peer", BaseURL: "ftp://example.com"},
		{Peer: "peer", BaseURL: "https://user:pass@example.com"},
		{Peer: "peer", BaseURL: "https://example.com?q=secret"},
		{Peer: "peer", BaseURL: "https://example.com", DefaultHeaders: HeaderMap{"Host": "evil"}},
		{Peer: "peer", BaseURL: "https://example.com", MaxResponseBodyBytes: -1},
		{Peer: "peer", BaseURL: "https://example.com", MaxRetries: -1},
		{Peer: "peer", BaseURL: "https://example.com", RetryStatuses: []int{999}},
		{Peer: "peer", BaseURL: "https://example.com", Auth: BearerToken("")},
	}
	for i, cfg := range tests {
		if _, err := New(cfg); KindOf(err) != KindInvalidConfig {
			t.Errorf("case %d: err=%v kind=%s", i, err, KindOf(err))
		}
	}

	client := MustNew(Config{Peer: "peer", BaseURL: "https://example.com", DisableLogging: true})
	requestTests := []struct {
		name    string
		method  Method
		path    string
		options []RequestOption
	}{
		{"path without slash", MethodGet, "users", nil},
		{"absolute URL", MethodGet, "https://evil.example/x", nil},
		{"query in path", MethodGet, "/users?q=x", nil},
		{"missing path value", MethodGet, "/users/{id}", nil},
		{"unknown path value", MethodGet, "/users", []RequestOption{Path("id", "1")}},
		{"traversal", MethodGet, "/../admin", nil},
		{"GET body", MethodGet, "/users", []RequestOption{JSON(map[string]string{})}},
		{"duplicate body", MethodPost, "/users", []RequestOption{Text("a"), Text("b")}},
		{"forbidden header", MethodGet, "/users", []RequestOption{Header("Content-Length", "10")}},
		{"invalid operation", MethodGet, "/users", []RequestOption{Operation("bad\noperation")}},
		{"nil option", MethodGet, "/users", []RequestOption{nil}},
	}
	for _, tt := range requestTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.Request(context.Background(), tt.method, tt.path, tt.options...)
			if KindOf(err) != KindInvalidRequest {
				t.Fatalf("err=%v kind=%s", err, KindOf(err))
			}
		})
	}
	if _, err := client.Get(nil, "/users"); KindOf(err) != KindInvalidRequest {
		t.Fatalf("nil context err=%v", err)
	}
}

func TestFacadeJSONDecodeErrors(t *testing.T) {
	response := &Response{
		StatusCode: 200, Attempts: 1, peer: "peer", method: MethodGet,
		operation: "decode", body: []byte(`{"a":1}{"b":2}`),
	}
	var decoded map[string]int
	if err := response.JSON(&decoded); KindOf(err) != KindDecode {
		t.Fatalf("trailing JSON err=%v", err)
	}
	if err := response.JSON(nil); KindOf(err) != KindDecode {
		t.Fatalf("nil destination err=%v", err)
	}
	var nilResponse *Response
	if err := nilResponse.JSON(&decoded); KindOf(err) != KindDecode {
		t.Fatalf("nil response err=%v", err)
	}
}

func TestFacadeSnapshotsInputsAndReusesOptionsConcurrently(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Default") != "configured" || r.Header.Get("X-Option") != "original" ||
			r.URL.Query().Get("q") != "original" || r.URL.EscapedPath() != "/items/a%2Fb" {
			t.Errorf("unexpected request path=%q query=%q headers=%v", r.URL.EscapedPath(), r.URL.RawQuery, r.Header)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	defaultHeaders := HeaderMap{"X-Default": "configured"}
	retryStatuses := []int{http.StatusServiceUnavailable}
	optionHeaders := map[string]string{"X-Option": "original"}
	optionQueries := map[string]string{"q": "original"}
	sharedOptions := []RequestOption{
		Path("id", "a/b"), Headers(optionHeaders), Queries(optionQueries), Operation("items.get"),
	}
	client := MustNew(Config{
		Peer: "items-api", BaseURL: srv.URL, DefaultHeaders: defaultHeaders,
		RetryStatuses: retryStatuses, DisableLogging: true,
	})

	defaultHeaders["X-Default"] = "mutated"
	retryStatuses[0] = http.StatusInternalServerError
	optionHeaders["X-Option"] = "mutated"
	optionQueries["q"] = "mutated"
	if client.cfg.DefaultHeaders["X-Default"] != "configured" || client.cfg.RetryStatuses[0] != http.StatusServiceUnavailable {
		t.Fatalf("client config was not snapshotted: %+v", client.cfg)
	}

	const workers = 16
	var wait sync.WaitGroup
	errorsFound := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := client.Get(context.Background(), "/items/{id}", sharedOptions...)
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Error(err)
		}
	}
	if calls.Load() != workers {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestFacadeDynamicTokenError(t *testing.T) {
	logs := observeFacade(t)
	client := MustNew(Config{
		Peer: "auth-api", BaseURL: "https://example.com",
		Auth: BearerTokenProvider(func(context.Context) (string, error) {
			return "", errors.New("credential provider unavailable")
		}),
	})
	_, err := client.Get(context.Background(), "/users", Operation("users.list"))
	if KindOf(err) != KindAuth {
		t.Fatalf("err=%v kind=%s", err, KindOf(err))
	}
	if logs.FilterMessage("http outbound failed").Len() != 1 {
		t.Fatalf("logs=%v", logs.All())
	}
}

func TestFacadeRedirectPolicy(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var sourceURL string
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/same-host":
			http.Redirect(w, r, sourceURL+"/final", http.StatusFound)
		case "/cross-host":
			http.Redirect(w, r, target.URL+"/secret", http.StatusFound)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer source.Close()
	sourceURL = source.URL

	noFollow := MustNew(Config{Peer: "redirect-api", BaseURL: source.URL, Auth: APIKey("X-API-Key", "secret"), DisableLogging: true})
	response, err := noFollow.Get(context.Background(), "/cross-host")
	if !IsStatus(err, http.StatusFound) || response.StatusCode != http.StatusFound || targetCalls.Load() != 0 {
		t.Fatalf("default redirect response=%+v err=%v targetCalls=%d", response, err, targetCalls.Load())
	}

	follow := MustNew(Config{
		Peer: "redirect-api", BaseURL: source.URL, FollowRedirects: true,
		Auth: APIKey("X-API-Key", "secret"), DisableLogging: true,
	})
	response, err = follow.Get(context.Background(), "/same-host")
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("same-host redirect response=%+v err=%v", response, err)
	}
	_, err = follow.Get(context.Background(), "/cross-host")
	if KindOf(err) != KindTransport || targetCalls.Load() != 0 {
		t.Fatalf("cross-host redirect err=%v targetCalls=%d", err, targetCalls.Load())
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
