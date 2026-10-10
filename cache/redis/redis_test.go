package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/go-dto/go-common/cache"
)

func TestStoreLifecycleTTLBoundsAndCopies(t *testing.T) {
	m := miniredis.RunT(t)
	s, err := New(context.Background(), Config{
		Addrs: []string{m.Addr()}, KeyPrefix: "test:", DefaultTTL: time.Minute,
		MaxKeyBytes: 8, MaxValueBytes: 8, StartupAttempts: 1, DisableLogging: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	value := []byte("value")
	if err := s.Set(ctx, "key", value, 0); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	got, err := s.Get(ctx, "key")
	if err != nil || string(got) != "value" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	got[0] = 'Y'
	again, _ := s.Get(ctx, "key")
	if string(again) != "value" {
		t.Fatal("Get did not return an independent value")
	}
	m.FastForward(time.Minute)
	if _, err := s.Get(ctx, "key"); !cache.IsMiss(err) {
		t.Fatalf("expired Get = %v", err)
	}
	if err := s.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "key", "missing"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "key"); !cache.IsMiss(err) {
		t.Fatalf("deleted Get = %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil || s.Close() != nil {
		t.Fatal("Close must be idempotent")
	}
	if _, err := s.Get(ctx, "key"); !errors.Is(err, cache.ErrClosed) {
		t.Fatalf("closed Get = %v", err)
	}
}

func TestConfigProductionAndInputValidation(t *testing.T) {
	valid := Config{
		Addrs: []string{"cache.example:6379"}, KeyPrefix: "orders:",
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if err := valid.ValidateProduction(); err != nil {
		t.Fatalf("valid production config: %v", err)
	}
	for _, cfg := range []Config{
		{Addrs: []string{"cache.example:6379"}, TLSConfig: &tls.Config{}},
		{Addrs: []string{"cache.example:6379"}, KeyPrefix: "x", TLSConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec -- validation test
		{Addrs: []string{"cache.example:6379"}, KeyPrefix: "x", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS10}},
		{Addrs: []string{"cache.example:6379"}, KeyPrefix: "bad\n", TLSConfig: &tls.Config{}},
		{Addrs: []string{"cache.example:6379"}, KeyPrefix: "x", OperationTimeout: -1, TLSConfig: &tls.Config{}},
		{Addrs: []string{"cache.example:6379"}, KeyPrefix: "x", MaxDeleteKeys: -1, TLSConfig: &tls.Config{}},
	} {
		if err := cfg.ValidateProduction(); err == nil {
			t.Fatalf("unsafe production config passed: %+v", cfg)
		}
	}
	m := miniredis.RunT(t)
	s, err := New(context.Background(), Config{Addrs: []string{m.Addr()}, MaxKeyBytes: 3, MaxValueBytes: 3, MaxDeleteKeys: 1, DisableLogging: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "long", "a\n"} {
		if err := s.Set(context.Background(), key, nil, 0); cache.KindOf(err) != cache.KindInvalid {
			t.Fatalf("key %q error = %v", key, err)
		}
	}
	if err := s.Set(context.Background(), "key", []byte("four"), 0); cache.KindOf(err) != cache.KindTooLarge {
		t.Fatalf("large value = %v", err)
	}
	if err := s.Set(context.Background(), "key", nil, -1); cache.KindOf(err) != cache.KindInvalid {
		t.Fatalf("negative TTL = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Get(ctx, "key"); cache.KindOf(err) != cache.KindCanceled {
		t.Fatalf("canceled Get = %v", err)
	}
	//lint:ignore SA1012 exercising the public nil-context guard
	if _, err := s.Get(nil, "key"); cache.KindOf(err) != cache.KindInvalid {
		t.Fatalf("nil-context Get = %v", err)
	}
	if err := s.Delete(context.Background()); err != nil {
		t.Fatalf("empty Delete = %v", err)
	}
	if err := s.Delete(context.Background(), "a", "b"); cache.KindOf(err) != cache.KindTooLarge {
		t.Fatalf("oversized Delete batch = %v", err)
	}
}

func TestInjectedClientOwnershipAndSanitizedStartupError(t *testing.T) {
	m := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	s, err := New(context.Background(), Config{Client: client, SkipPing: true, DisableLogging: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("injected client was closed: %v", err)
	}
	_ = client.Close()

	m2 := miniredis.RunT(t)
	m2.RequireAuth("correct-secret")
	_, err = New(context.Background(), Config{
		Addrs: []string{m2.Addr()}, Password: "do-not-leak", StartupAttempts: 1, DisableLogging: true,
	})
	if err == nil || strings.Contains(err.Error(), "do-not-leak") || strings.Contains(err.Error(), m2.Addr()) {
		t.Fatalf("startup error is not sanitized: %v", err)
	}
}

func TestReadinessBackendFailureLoggingAndConcurrency(t *testing.T) {
	m := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	s, err := New(context.Background(), Config{
		Client: client, SkipPing: true, KeyPrefix: "concurrent:",
		OperationTimeout: time.Second, SlowThreshold: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.cfg.Client != nil || s.cfg.Password != "" || s.cfg.SentinelPassword != "" || len(s.cfg.Addrs) != 0 || s.cfg.Username != "" || s.cfg.TLSConfig != nil {
		t.Fatal("runtime config retained connection metadata or credential")
	}
	if err := Check("redis_cache", s).Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := Check("nil", nil).Ping(context.Background()); !errors.Is(err, cache.ErrClosed) {
		t.Fatalf("nil readiness check = %v", err)
	}

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := string(rune('a' + i))
			if err := s.Set(context.Background(), key, []byte("value"), time.Minute); err != nil {
				t.Errorf("Set(%q): %v", key, err)
				return
			}
			if got, err := s.Get(context.Background(), key); err != nil || string(got) != "value" {
				t.Errorf("Get(%q) = %q, %v", key, got, err)
			}
		}()
	}
	wg.Wait()

	// Exercise stable error classification and logging without returning raw
	// backend details to callers.
	s.observe(context.Background(), "get", "error", time.Millisecond, cache.NewError("get", cache.KindUnavailable, nil))
	_ = client.Close()
	for operation, call := range map[string]func() error{
		"get":    func() error { _, err := s.Get(context.Background(), "a"); return err },
		"set":    func() error { return s.Set(context.Background(), "a", []byte("v"), time.Minute) },
		"delete": func() error { return s.Delete(context.Background(), "a") },
		"ping":   func() error { return s.Ping(context.Background()) },
	} {
		if err := call(); err == nil || strings.Contains(err.Error(), m.Addr()) {
			t.Fatalf("%s backend error = %v", operation, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInternalErrorKindsAndProductionRestrictions(t *testing.T) {
	if kind(context.Canceled) != cache.KindCanceled ||
		kind(context.DeadlineExceeded) != cache.KindTimeout ||
		kind(goredis.ErrClosed) != cache.KindClosed ||
		kind(errors.New("backend")) != cache.KindUnavailable {
		t.Fatal("unexpected Redis cache error classification")
	}
	m := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	defer client.Close()
	for _, cfg := range []Config{
		{Client: client, KeyPrefix: "x:", TLSConfig: &tls.Config{}},
		{Addrs: []string{"cache.example:6379"}, SkipPing: true, KeyPrefix: "x:", TLSConfig: &tls.Config{}},
	} {
		if err := cfg.ValidateProduction(); err == nil {
			t.Fatalf("unsafe production config passed: %+v", cfg)
		}
	}
	var nilStore *Store
	if err := nilStore.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRealRedisIntegration(t *testing.T) {
	addr := testEnv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR is not set")
	}
	prefix := "go-common:cache:test:" + time.Now().UTC().Format("150405.000000000") + ":"
	password, tlsConfig := redisIntegrationSecurity(t)
	s, err := New(context.Background(), Config{
		Addrs: []string{addr}, Password: password, TLSConfig: tlsConfig,
		KeyPrefix: prefix, StartupAttempts: 1, DisableLogging: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Set(context.Background(), "key", []byte("value"), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(context.Background(), "key")
	if err != nil || string(got) != "value" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if err := s.Delete(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}
}

func testEnv(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func redisIntegrationSecurity(t *testing.T) (string, *tls.Config) {
	t.Helper()
	password := testEnv("REDIS_TEST_PASSWORD")
	caPath := testEnv("REDIS_TEST_TLS_CA")
	if caPath == "" {
		return password, nil
	}
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read Redis test CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("Redis test CA is not valid PEM")
	}
	serverName := testEnv("REDIS_TEST_TLS_SERVER_NAME")
	if serverName == "" {
		serverName = "localhost"
	}
	return password, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: serverName,
	}
}
