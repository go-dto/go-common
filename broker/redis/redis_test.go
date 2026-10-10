package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/go-dto/go-common/broker"
)

func TestPublishReceiveAckAndTraceFields(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	b, err := New(context.Background(), cfg, broker.Options{HandlerTimeout: 100 * time.Millisecond, DisableLogging: true})
	if err != nil {
		t.Fatal(err)
	}
	d := b.Driver().(*Driver)
	t.Cleanup(func() { _ = b.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan *broker.Message, 1)
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, "worker", func(_ context.Context, msg *broker.Message) error {
			delivered <- msg
			return nil
		})
	}()
	id, err := b.Publish(context.Background(), "jobs", &broker.Message{
		Data: []byte("payload"), Attributes: map[string]string{"kind": "created"},
	})
	if err != nil || id == "" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	select {
	case msg := <-delivered:
		if msg.ID != id || string(msg.Data) != "payload" || msg.Attributes["kind"] != "created" || msg.DeliveryAttempt != 1 || msg.PublishTime.IsZero() {
			t.Fatalf("unexpected message: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("message was not delivered")
	}

	stream, _ := d.StreamKey("jobs")
	eventually(t, time.Second, func() bool {
		pending, err := d.client.XPending(context.Background(), stream, cfg.Subscriptions["worker"].Group).Result()
		return err == nil && pending.Count == 0
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Subscribe stop = %v", err)
	}
	if _, err := d.StreamKey("unknown"); err == nil {
		t.Fatal("unknown stream key must fail")
	}
	if _, err := d.DeadLetterKey("unknown"); err == nil {
		t.Fatal("unknown DLQ key must fail")
	}
}

func TestTransientFailuresAreReclaimedThenDeadLettered(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	cfg.MaxDeliveryAttempts = 2
	cfg.ClaimMinIdle = 10 * time.Millisecond
	b, err := New(context.Background(), cfg, broker.Options{HandlerTimeout: 5 * time.Millisecond, DisableLogging: true})
	if err != nil {
		t.Fatal(err)
	}
	d := b.Driver().(*Driver)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var attempts atomic.Int32
	go func() {
		done <- b.Subscribe(ctx, "worker", func(context.Context, *broker.Message) error {
			attempts.Add(1)
			return errors.New("transient")
		})
	}()
	if _, err := b.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return attempts.Load() >= 1 })
	m.FastForward(20 * time.Millisecond)
	eventually(t, 2*time.Second, func() bool { return attempts.Load() >= 2 })

	dlq, _ := d.DeadLetterKey("worker")
	eventually(t, time.Second, func() bool {
		count, err := d.client.XLen(context.Background(), dlq).Result()
		return err == nil && count == 1
	})
	stream, _ := d.StreamKey("jobs")
	eventually(t, time.Second, func() bool {
		pending, err := d.client.XPending(context.Background(), stream, cfg.Subscriptions["worker"].Group).Result()
		return err == nil && pending.Count == 0
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMalformedMessageGoesDirectlyToDeadLetter(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	d, err := NewDriver(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	stream, _ := d.StreamKey("jobs")
	if err := d.client.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: stream, Values: []any{fieldData, "payload", fieldAttributes, "not-json"},
	}).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var called atomic.Bool
	go func() {
		done <- d.Receive(ctx, "worker", func(context.Context, *broker.Message) error {
			called.Store(true)
			return nil
		})
	}()
	dlq, _ := d.DeadLetterKey("worker")
	eventually(t, time.Second, func() bool {
		count, err := d.client.XLen(context.Background(), dlq).Result()
		return err == nil && count == 1
	})
	if called.Load() {
		t.Fatal("malformed message reached handler")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPermanentErrorIsAckedWithoutDeadLetter(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	b, err := New(context.Background(), cfg, broker.Options{HandlerTimeout: 100 * time.Millisecond, DisableLogging: true})
	if err != nil {
		t.Fatal(err)
	}
	d := b.Driver().(*Driver)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, "worker", func(context.Context, *broker.Message) error {
			return broker.Permanent(errors.New("invalid payload"))
		})
	}()
	if _, err := b.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	stream, _ := d.StreamKey("jobs")
	eventually(t, time.Second, func() bool {
		pending, err := d.client.XPending(context.Background(), stream, cfg.Subscriptions["worker"].Group).Result()
		return err == nil && pending.Count == 0
	})
	dlq, _ := d.DeadLetterKey("worker")
	if count, _ := d.client.XLen(context.Background(), dlq).Result(); count != 0 {
		t.Fatalf("permanent error produced %d DLQ messages", count)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
}

func TestValidationPublishBoundsAndProvisioning(t *testing.T) {
	valid := Config{
		Addrs: []string{"queue.example:6379"}, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		KeyPrefix: "orders:", Topics: []string{"jobs"}, ConsumerName: "pod-1",
		Subscriptions: map[string]Subscription{"worker": {Topic: "jobs"}},
		StreamMaxLen:  100_000, DeadLetterMaxLen: 10_000,
	}
	if err := valid.ValidateProduction(); err != nil {
		t.Fatalf("valid production config: %v", err)
	}
	if err := valid.ValidateProductionWithOptions(broker.Options{HandlerTimeout: 30 * time.Second}); err != nil {
		t.Fatalf("valid production config and options: %v", err)
	}
	if err := valid.ValidateProductionWithOptions(broker.Options{}); err == nil {
		t.Fatal("production options without HandlerTimeout must fail")
	}
	tooLong := valid
	tooLong.ClaimMinIdle = time.Second
	if err := tooLong.ValidateProductionWithOptions(broker.Options{HandlerTimeout: time.Second}); err == nil {
		t.Fatal("ClaimMinIdle equal to HandlerTimeout must fail")
	}
	bad := []Config{
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, Topics: valid.Topics},
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, KeyPrefix: "x{", Topics: valid.Topics},
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, KeyPrefix: "x:", Topics: valid.Topics, AutoCreateGroups: true},
		{Addrs: valid.Addrs, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS10}, KeyPrefix: "x:", Topics: valid.Topics},
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, KeyPrefix: "x:", Topics: []string{"bad topic"}},
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, KeyPrefix: "x:", Topics: valid.Topics, Subscriptions: map[string]Subscription{"s": {Topic: "unknown"}}, ConsumerName: "c"},
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, KeyPrefix: "x:", Topics: valid.Topics, BlockTimeout: time.Second, ReadTimeout: time.Second},
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, KeyPrefix: "x:", Topics: valid.Topics, ReceiveRetryInitialBackoff: -time.Second},
		{Addrs: valid.Addrs, TLSConfig: valid.TLSConfig, KeyPrefix: "x:", Topics: valid.Topics, ReceiveRetryInitialBackoff: time.Second, ReceiveRetryMaxBackoff: time.Millisecond},
	}
	for i, cfg := range bad {
		if err := cfg.ValidateProduction(); err == nil {
			t.Fatalf("case %d must fail", i)
		}
	}
	m := miniredis.RunT(t)
	missing := testConfig(m)
	missing.AutoCreateGroups = false
	if _, err := NewDriver(context.Background(), missing); err == nil || strings.Contains(err.Error(), m.Addr()) {
		t.Fatalf("missing group error = %v", err)
	}
	capabilityClient := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	capabilityHook := &receiveFailureHook{}
	capabilityHook.fail.Store(true)
	capabilityClient.AddHook(capabilityHook)
	capabilityConfig := testConfig(m)
	capabilityConfig.Addrs = nil
	capabilityConfig.Client = capabilityClient
	capabilityConfig.SkipPing = true
	if _, err := NewDriver(context.Background(), capabilityConfig); err == nil || strings.Contains(err.Error(), "internal-host") {
		t.Fatalf("capability error = %v", err)
	}
	_ = capabilityClient.Close()

	d, err := NewDriver(context.Background(), testConfig(m))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Publish(context.Background(), "unknown", &broker.Message{}); err == nil {
		t.Fatal("unknown topic must fail")
	}
	if _, err := d.Publish(context.Background(), "jobs", &broker.Message{OrderingKey: "id"}); err == nil {
		t.Fatal("ordering key must fail")
	}
	d.cfg.MaxMessageBytes = 3
	if _, err := d.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("large")}); err == nil {
		t.Fatal("large message must fail")
	}
	//lint:ignore SA1012 exercising the public nil-context guard
	if _, err := d.Publish(nil, "jobs", &broker.Message{}); err == nil {
		t.Fatal("nil context must fail")
	}
}

func TestCloseDuplicateReceiveInjectedOwnershipAndCheck(t *testing.T) {
	m := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	cfg := testConfig(m)
	cfg.Addrs = nil
	cfg.Client = client
	cfg.SkipPing = true
	d, err := NewDriver(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := Check("redis_queue", d).Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := ConsumerCheck("redis_queue_consumer", d).Ping(context.Background()); err == nil {
		t.Fatal("consumer readiness passed without an active receiver")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Receive(ctx, "worker", func(context.Context, *broker.Message) error { return nil }) }()
	eventually(t, time.Second, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.receiving["worker"]
	})
	if err := ConsumerCheck("redis_queue_consumer", d).Ping(context.Background()); err != nil {
		t.Fatalf("active consumer readiness = %v", err)
	}
	if err := d.Receive(ctx, "worker", func(context.Context, *broker.Message) error { return nil }); err == nil {
		t.Fatal("duplicate receiver must fail")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	//lint:ignore SA1012 exercising the public nil-context guard
	if err := d.CloseContext(nil); err == nil {
		t.Fatal("nil close context must fail")
	}
	if err := d.Close(); err != nil || d.Close() != nil {
		t.Fatal("Close must be idempotent")
	}
	if _, err := d.Publish(context.Background(), "jobs", &broker.Message{}); !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("publish after close = %v", err)
	}
	if err := d.Receive(context.Background(), "worker", func(context.Context, *broker.Message) error { return nil }); !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("receive after close = %v", err)
	}
	if err := d.Ping(context.Background()); !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("ping after close = %v", err)
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("injected client was closed: %v", err)
	}
	_ = client.Close()
	if err := Check("nil", nil).Ping(context.Background()); !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("nil check = %v", err)
	}
	var nilDriver *Driver
	if err := nilDriver.Receive(context.Background(), "worker", func(context.Context, *broker.Message) error { return nil }); !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("nil driver Receive = %v", err)
	}
	if err := nilDriver.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveRetriesAndReadinessDetectsMissingGroup(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	cfg.ReceiveRetryInitialBackoff = 5 * time.Millisecond
	cfg.ReceiveRetryMaxBackoff = 20 * time.Millisecond
	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	hook := &receiveFailureHook{}
	client.AddHook(hook)
	cfg.Addrs = nil
	cfg.Client = client
	cfg.SkipPing = true
	d, err := NewDriver(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = d.Close()
		_ = client.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	delivered := make(chan struct{}, 1)
	go func() {
		done <- d.Receive(ctx, "worker", func(context.Context, *broker.Message) error {
			delivered <- struct{}{}
			return nil
		})
	}()
	eventually(t, time.Second, func() bool {
		return d.ConsumerReadiness(context.Background()) == nil
	})

	hook.fail.Store(true)
	eventually(t, time.Second, func() bool {
		return d.Readiness(context.Background()) != nil
	})
	select {
	case err := <-done:
		t.Fatalf("receiver exited during transient outage: %v", err)
	default:
	}
	hook.fail.Store(false)
	eventually(t, 2*time.Second, func() bool {
		return d.ConsumerReadiness(context.Background()) == nil
	})
	if _, err := d.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("after-restart")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
	case err := <-done:
		t.Fatalf("receiver exited after transient failure: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("message was not delivered after receive recovered")
	}

	stream, _ := d.StreamKey("jobs")
	if err := d.client.XGroupDestroy(context.Background(), stream, cfg.Subscriptions["worker"].Group).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool {
		return d.Readiness(context.Background()) != nil
	})
	eventually(t, time.Second, func() bool {
		select {
		case err := <-done:
			return err != nil && !strings.Contains(err.Error(), m.Addr())
		default:
			return false
		}
	})
	cancel()
}

type receiveFailureHook struct{ fail atomic.Bool }

func (h *receiveFailureHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *receiveFailureHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if h.fail.Load() && (cmd.Name() == "xautoclaim" || cmd.Name() == "xreadgroup") {
			return errors.New("backend at internal-host")
		}
		return next(ctx, cmd)
	}
}

func (h *receiveFailureHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func TestCodecValidationAndStableErrors(t *testing.T) {
	d := &Driver{cfg: Config{MaxMessageBytes: 16, MaxAttributes: 1, MaxAttributeBytes: 8}}
	invalidMessages := []*broker.Message{
		nil,
		{OrderingKey: "order"},
		{Attributes: map[string]string{"a": "1", "b": "2"}},
		{Attributes: map[string]string{"": "1"}},
		{Attributes: map[string]string{"a": string([]byte{0xff})}},
		{Attributes: map[string]string{"attribute": "large"}},
		{Data: []byte("this payload is too large")},
	}
	for i, msg := range invalidMessages {
		if _, err := d.encode(msg); err == nil {
			t.Fatalf("invalid encode case %d passed", i)
		}
	}
	encoded, err := d.encode(&broker.Message{Data: []byte("ok"), Attributes: map[string]string{"a": "1"}})
	if err != nil || len(encoded) != 4 {
		t.Fatalf("valid encode = %#v, %v", encoded, err)
	}

	invalidRaw := []goredis.XMessage{
		{ID: "1-0", Values: map[string]any{fieldAttributes: "{}"}},
		{ID: "1-0", Values: map[string]any{fieldData: "data"}},
		{ID: "1-0", Values: map[string]any{fieldData: 42, fieldAttributes: "{}"}},
		{ID: "1-0", Values: map[string]any{fieldData: "data", fieldAttributes: []byte{0xff}}},
		{ID: "1-0", Values: map[string]any{fieldData: "data", fieldAttributes: "{"}},
		{ID: "1-0", Values: map[string]any{fieldData: "data", fieldAttributes: `{"a":"1","b":"2"}`}},
		{ID: "1-0", Values: map[string]any{fieldData: "data", fieldAttributes: `{"attribute":"large"}`}},
	}
	for i, raw := range invalidRaw {
		if _, err := d.decode(raw, 1); err == nil {
			t.Fatalf("invalid decode case %d passed", i)
		}
	}
	decoded, err := d.decode(goredis.XMessage{
		ID: "1234-0", Values: map[string]any{fieldData: []byte("ok"), fieldAttributes: `{"a":"1"}`},
	}, 2)
	if err != nil || string(decoded.Data) != "ok" || decoded.DeliveryAttempt != 2 || decoded.PublishTime.UnixMilli() != 1234 {
		t.Fatalf("valid decode = %+v, %v", decoded, err)
	}
	if _, ok := fieldBytes(42); ok || !streamTime("bad").IsZero() || !streamTime("-1-0").IsZero() {
		t.Fatal("malformed stream fields were accepted")
	}
	if !errors.Is(safeError("publish", goredis.ErrClosed), broker.ErrClosed) {
		t.Fatal("closed backend error was not stable")
	}
	if got := safeSubscriptionError(errors.New("NOGROUP internal-host")); strings.Contains(got.Error(), "internal-host") {
		t.Fatalf("subscription error leaked backend details: %v", got)
	}
	if got := safeSubscriptionError(errors.New("backend at internal-host")); strings.Contains(got.Error(), "internal-host") {
		t.Fatalf("receive error leaked backend details: %v", got)
	}
}

func TestConcurrentPublishProvisionedGroupAndSanitizedFailure(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	bootstrap := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	stream := cfg.streamKey("jobs")
	if err := bootstrap.XGroupCreateMkStream(context.Background(), stream, "worker-group", "0").Err(); err != nil {
		t.Fatal(err)
	}
	_ = bootstrap.Close()
	cfg.AutoCreateGroups = false
	d, err := NewDriver(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if d.Client() == nil || d.cfg.Client != nil || d.cfg.Password != "" || d.cfg.SentinelPassword != "" || len(d.cfg.Addrs) != 0 || d.cfg.Username != "" || d.cfg.TLSConfig != nil {
		t.Fatal("driver retained connection metadata or credential")
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := d.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("payload")}); err != nil {
				t.Errorf("concurrent Publish: %v", err)
			}
		}()
	}
	wg.Wait()
	if count, err := d.client.XLen(context.Background(), stream).Result(); err != nil || count != 20 {
		t.Fatalf("stream length = %d, %v", count, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Publish(canceled, "jobs", &broker.Message{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Publish = %v", err)
	}
	//lint:ignore SA1012 exercising the public nil-context guard
	if err := d.Receive(nil, "worker", func(context.Context, *broker.Message) error { return nil }); err == nil {
		t.Fatal("nil receive context passed")
	}
	if err := d.Receive(context.Background(), "worker", nil); err == nil {
		t.Fatal("nil handler passed")
	}
	if err := d.Receive(context.Background(), "unknown", func(context.Context, *broker.Message) error { return nil }); err == nil {
		t.Fatal("unknown subscription passed")
	}
	d.logSettleFailure(context.Background(), "worker", "1-0", "ack", errors.New("safe"))
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	m2 := miniredis.RunT(t)
	m2.RequireAuth("correct-secret")
	_, err = NewDriver(context.Background(), Config{
		Addrs: []string{m2.Addr()}, Password: "do-not-leak", KeyPrefix: "test:",
		Topics: []string{"jobs"}, StartupAttempts: 1,
	})
	if err == nil || strings.Contains(err.Error(), "do-not-leak") || strings.Contains(err.Error(), m2.Addr()) {
		t.Fatalf("startup error is not sanitized: %v", err)
	}
}

func TestPanicIsReclaimedAndDeadLettered(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	cfg.MaxDeliveryAttempts = 2
	cfg.ClaimMinIdle = 10 * time.Millisecond
	b, err := New(context.Background(), cfg, broker.Options{HandlerTimeout: 5 * time.Millisecond, DisableLogging: true})
	if err != nil {
		t.Fatal(err)
	}
	d := b.Driver().(*Driver)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var attempts atomic.Int32
	go func() {
		done <- b.Subscribe(ctx, "worker", func(context.Context, *broker.Message) error {
			attempts.Add(1)
			panic("sensitive panic value")
		})
	}()
	if _, err := b.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return attempts.Load() >= 1 })
	m.FastForward(20 * time.Millisecond)
	eventually(t, 2*time.Second, func() bool { return attempts.Load() >= 2 })
	dlq, _ := d.DeadLetterKey("worker")
	eventually(t, time.Second, func() bool {
		count, err := d.client.XLen(context.Background(), dlq).Result()
		return err == nil && count == 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
}

func TestOversizedMalformedEntryIsBoundedInDeadLetter(t *testing.T) {
	m := miniredis.RunT(t)
	cfg := testConfig(m)
	cfg.MaxMessageBytes = 8
	d, err := NewDriver(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	stream, _ := d.StreamKey("jobs")
	if err := d.client.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: stream,
		Values: []any{fieldData, strings.Repeat("x", 1024), fieldAttributes, "{}"},
	}).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- d.Receive(ctx, "worker", func(context.Context, *broker.Message) error {
			t.Error("oversized entry reached handler")
			return nil
		})
	}()
	dlq, _ := d.DeadLetterKey("worker")
	eventually(t, time.Second, func() bool {
		return d.client.XLen(context.Background(), dlq).Val() == 1
	})
	entries, err := d.client.XRangeN(context.Background(), dlq, "-", "+", 1).Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("DLQ entries = %+v, %v", entries, err)
	}
	data, ok := fieldBytes(entries[0].Values[fieldData])
	if !ok || len(data) != 0 {
		t.Fatalf("oversized payload was copied to DLQ: %d bytes", len(data))
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRealRedisIntegration(t *testing.T) {
	addr := strings.TrimSpace(os.Getenv("REDIS_TEST_ADDR"))
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR is not set")
	}
	prefix := "go-common:queue:test:" + time.Now().UTC().Format("150405.000000000") + ":"
	password, tlsConfig := redisIntegrationSecurity(t)
	cfg := Config{
		Addrs: []string{addr}, Password: password, TLSConfig: tlsConfig,
		KeyPrefix: prefix, Topics: []string{"jobs"}, ConsumerName: "integration-1",
		Subscriptions: map[string]Subscription{"worker": {Topic: "jobs"}}, AutoCreateGroups: true,
		BlockTimeout: 50 * time.Millisecond, ClaimMinIdle: 50 * time.Millisecond,
		MaxDeliveryAttempts: 2, StartupAttempts: 1,
	}
	b, err := New(context.Background(), cfg, broker.Options{HandlerTimeout: 10 * time.Millisecond, DisableLogging: true})
	if err != nil {
		t.Fatal(err)
	}
	d := b.Driver().(*Driver)
	stream, _ := d.StreamKey("jobs")
	dlq, _ := d.DeadLetterKey("worker")
	t.Cleanup(func() {
		_ = d.client.Del(context.Background(), stream, dlq).Err()
		_ = b.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, "worker", func(context.Context, *broker.Message) error {
			delivered <- struct{}{}
			return nil
		})
	}()
	if _, err := b.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("integration")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("real Redis message was not delivered")
	}
	eventually(t, 2*time.Second, func() bool {
		pending, pendingErr := d.client.XPending(context.Background(), stream, d.cfg.Subscriptions["worker"].Group).Result()
		return pendingErr == nil && pending.Count == 0
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Real Redis must also validate pending reclaim and the atomic DLQ+XACK
	// script; miniredis remains the fast unit-test backend.
	retryCtx, retryCancel := context.WithCancel(context.Background())
	retryDone := make(chan error, 1)
	var attempts atomic.Int32
	go func() {
		retryDone <- b.Subscribe(retryCtx, "worker", func(context.Context, *broker.Message) error {
			attempts.Add(1)
			return errors.New("transient")
		})
	}()
	if _, err := b.Publish(context.Background(), "jobs", &broker.Message{Data: []byte("dead-letter")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		count, err := d.client.XLen(context.Background(), dlq).Result()
		return err == nil && attempts.Load() >= 2 && count == 1
	})
	eventually(t, 2*time.Second, func() bool {
		pending, pendingErr := d.client.XPending(context.Background(), stream, d.cfg.Subscriptions["worker"].Group).Result()
		return pendingErr == nil && pending.Count == 0
	})
	retryCancel()
	if err := <-retryDone; err != nil {
		t.Fatal(err)
	}
}

func redisIntegrationSecurity(t *testing.T) (string, *tls.Config) {
	t.Helper()
	password := strings.TrimSpace(os.Getenv("REDIS_TEST_PASSWORD"))
	caPath := strings.TrimSpace(os.Getenv("REDIS_TEST_TLS_CA"))
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
	serverName := strings.TrimSpace(os.Getenv("REDIS_TEST_TLS_SERVER_NAME"))
	if serverName == "" {
		serverName = "localhost"
	}
	return password, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: serverName,
	}
}

func testConfig(m *miniredis.Miniredis) Config {
	return Config{
		Addrs: []string{m.Addr()}, KeyPrefix: "test:", Topics: []string{"jobs"}, ConsumerName: "consumer-1",
		Subscriptions: map[string]Subscription{"worker": {Topic: "jobs", Group: "worker-group"}},
		Concurrency:   2, ReadBatchSize: 2, BlockTimeout: 10 * time.Millisecond,
		ClaimMinIdle: 200 * time.Millisecond, MaxDeliveryAttempts: 3,
		StreamMaxLen: 100, DeadLetterMaxLen: 10, AutoCreateGroups: true, StartupAttempts: 1,
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
