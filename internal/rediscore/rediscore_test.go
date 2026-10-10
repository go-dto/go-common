package rediscore

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestValidateAndProduction(t *testing.T) {
	valid := Config{Addrs: []string{"cache.example:6379"}, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	if err := ValidateProduction(valid); err != nil {
		t.Fatalf("valid production config: %v", err)
	}
	bad := []Config{
		{},
		{Addrs: []string{"not-host-port"}},
		{Addrs: []string{"cache.example:6379"}, DB: -1},
		{Addrs: []string{"a:1", "b:2"}, DB: 1},
		{Addrs: []string{"a:1"}, ClientName: "bad\nname"},
		{Addrs: []string{"a:1"}, PoolSize: 2, MinIdleConns: 3},
		{Addrs: []string{"a:1"}, MaxRetries: -1},
		{Addrs: []string{"a:1"}, TLSConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec -- validation test
		{Addrs: []string{"a:1"}, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS10}},
		{Addrs: []string{"a:1"}, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS12}},
		{Addrs: []string{"a:1"}, SkipPing: true, TLSConfig: &tls.Config{}},
	}
	for i, cfg := range bad {
		if err := ValidateProduction(cfg); err == nil {
			t.Fatalf("case %d must fail", i)
		}
	}
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	if err := ValidateProduction(Config{Client: client, TLSConfig: &tls.Config{}}); err == nil {
		t.Fatal("injected client must fail production validation")
	}
}

func TestOpenPingCloseAndSafeFailure(t *testing.T) {
	m := miniredis.RunT(t)
	h, err := Open(context.Background(), Config{Addrs: []string{m.Addr()}, StartupAttempts: 1, Role: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.Client() == nil {
		t.Fatal("Open returned a nil client")
	}
	if err := h.Close(); err != nil || h.Close() != nil {
		t.Fatal("Close must be idempotent")
	}
	if err := h.Ping(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ping after Close = %v", err)
	}

	_, err = Open(context.Background(), Config{
		Addrs: []string{"127.0.0.1:1"}, StartupAttempts: 1, PingTimeout: 10 * time.Millisecond,
	})
	if !errors.Is(err, ErrUnavailable) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("safe unavailable error = %v", err)
	}
	if err != nil && (contains(err.Error(), "127.0.0.1") || contains(err.Error(), "password")) {
		t.Fatalf("unsafe error: %v", err)
	}
	//lint:ignore SA1012 exercising the internal nil-context guard
	if _, err := Open(nil, Config{}); err == nil {
		t.Fatal("nil context must fail")
	}
	if (&Handle{}).Ping(context.Background()) != ErrClosed {
		t.Fatal("empty handle must be closed")
	}
	//lint:ignore SA1012 exercising the internal nil-context guard
	if err := (&Handle{}).Ping(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("empty handle nil-context Ping = %v", err)
	}
}

func TestKinds(t *testing.T) {
	if ErrorKind(nil) != "" || ErrorKind(context.Canceled) != "canceled" || ErrorKind(context.DeadlineExceeded) != "timeout" {
		t.Fatal("context classification failed")
	}
	if SafeCause(nil) != nil || !errors.Is(SafeCause(goredis.ErrClosed), ErrClosed) {
		t.Fatal("safe cause classification failed")
	}
	if ErrorKind(timeoutError{}) != "timeout" || !errors.Is(SafeCause(errors.New("backend detail")), ErrUnavailable) {
		t.Fatal("backend classification failed")
	}
}

func TestValidateInjectedOwnershipAndCanceledRetry(t *testing.T) {
	if err := Validate(Config{Addrs: []string{"127.0.0.1:6379"}}); err != nil {
		t.Fatal(err)
	}
	m := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	h, err := Open(context.Background(), Config{Client: client, SkipPing: true})
	if err != nil {
		t.Fatal(err)
	}
	if h.Client() != client {
		t.Fatal("injected client was not used")
	}
	//lint:ignore SA1012 exercising the internal nil-context guard
	if err := h.Ping(nil); err == nil {
		t.Fatal("nil Ping context passed")
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("injected client was closed: %v", err)
	}
	_ = client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Open(ctx, Config{
		Addrs: []string{"127.0.0.1:1"}, StartupAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond,
		PingTimeout: time.Millisecond,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup = %v", err)
	}

	for i, cfg := range []Config{
		{Addrs: []string{"127.0.0.1:6379"}, DialTimeout: -1},
		{Addrs: []string{"127.0.0.1:6379"}, ConnMaxLifetime: -1},
		{Addrs: []string{"127.0.0.1:6379"}, PoolSize: 2, MaxActiveConns: 1},
		{Addrs: []string{"127.0.0.1:6379"}, MinRetryBackoff: 2 * time.Second, MaxRetryBackoff: time.Second},
		{Addrs: []string{"127.0.0.1:6379"}, StartupAttempts: -1},
	} {
		if err := Validate(cfg); err == nil {
			t.Fatalf("invalid config case %d passed", i)
		}
	}
	var nilHandle *Handle
	if err := nilHandle.Close(); err != nil {
		t.Fatal(err)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "internal timeout detail" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func contains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
