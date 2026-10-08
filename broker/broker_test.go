package broker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-dto/go-common/broker"
	"github.com/go-dto/go-common/broker/memory"
	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

func observe(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(logger.ReplaceForTest(zap.New(core)))
	return logs
}

// setup returns a broker over a memory driver with subscription "sub" on
// topic "topic", consuming with h in the background. The returned wait
// blocks until every published message has been handled.
func setup(t *testing.T, opts broker.Options, mopts memory.Options, h broker.Handler) (*broker.Broker, *memory.Driver, func()) {
	t.Helper()
	d := memory.New(mopts)
	if err := d.CreateSubscription("sub", "topic"); err != nil {
		t.Fatal(err)
	}
	opts.System = memory.System
	b := broker.New(d, opts)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Subscribe(ctx, "sub", h) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Subscribe: %v", err)
		}
		_ = b.Close()
	})

	wait := func() {
		t.Helper()
		wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer wcancel()
		if err := d.WaitIdle(wctx, "sub"); err != nil {
			t.Fatalf("WaitIdle: %v", err)
		}
	}
	return b, d, wait
}

func TestTracePropagation(t *testing.T) {
	logs := observe(t)
	type seen struct{ traceID, parentSpan, msgID string }
	got := make(chan seen, 1)
	b, _, wait := setup(t, broker.Options{}, memory.Options{}, func(ctx context.Context, msg *broker.Message) error {
		id, _ := ctxid.FromContext(ctx)
		var parent string
		for _, f := range logger.FieldsFromContext(ctx) {
			if f.Key == logger.KeyParentSpanID {
				parent = f.String
			}
		}
		got <- seen{id, parent, msg.ID}
		return nil
	})

	ctx := ctxid.WithContext(context.Background(), "trace-1")
	ctx, op := logger.StartOp(ctx, logger.LayerService, "webhook.Send")
	id, err := b.Publish(ctx, "topic", &broker.Message{Data: []byte("hi")})
	op.End(err)
	if err != nil {
		t.Fatal(err)
	}
	wait()

	s := <-got
	if s.traceID != "trace-1" || s.parentSpan != op.SpanID() || s.msgID != id {
		t.Fatalf("handler saw %+v, want trace-1 / %s / %s", s, op.SpanID(), id)
	}

	handled := logs.FilterMessage("broker message handled").All()
	if len(handled) != 1 {
		t.Fatalf("handled lines = %d", len(handled))
	}
	cm := handled[0].ContextMap()
	if cm[logger.KeyTraceID] != "trace-1" || cm[broker.KeySubscription] != "sub" ||
		cm[broker.KeyMessageID] != id || cm[broker.KeySystem] != memory.System || cm[broker.KeyDeliveryAttempt] != int64(1) {
		t.Errorf("handled fields = %v", cm)
	}
	pub := logs.FilterMessage("broker publish").All()
	if len(pub) != 1 || pub[0].ContextMap()[broker.KeyTopic] != "topic" || pub[0].ContextMap()[logger.KeyTraceID] != "trace-1" {
		t.Errorf("publish lines = %v", pub)
	}
}

func TestNewTraceIDWithoutPropagation(t *testing.T) {
	observe(t)
	got := make(chan string, 1)
	b, _, wait := setup(t, broker.Options{}, memory.Options{}, func(ctx context.Context, msg *broker.Message) error {
		id, _ := ctxid.FromContext(ctx)
		got <- id
		return nil
	})
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{}); err != nil {
		t.Fatal(err)
	}
	wait()
	if id := <-got; len(id) != 32 {
		t.Fatalf("consumer should get a fresh trace id, got %q", id)
	}
}

func TestPublishDoesNotModifyMessage(t *testing.T) {
	observe(t)
	b, d, _ := setup(t, broker.Options{}, memory.Options{}, func(context.Context, *broker.Message) error { return nil })
	msg := &broker.Message{Attributes: map[string]string{"type": "x"}}
	ctx := ctxid.WithContext(context.Background(), "trace-1")
	if _, err := b.Publish(ctx, "topic", msg); err != nil {
		t.Fatal(err)
	}
	if len(msg.Attributes) != 1 || msg.ID != "" {
		t.Errorf("caller's message modified: %+v", msg)
	}
	pub := d.Published("topic")
	if len(pub) != 1 || pub[0].Attributes[broker.AttrTraceID] != "trace-1" || pub[0].Attributes["type"] != "x" {
		t.Errorf("published = %+v", pub)
	}

	// An explicit attribute wins over the context.
	msg.Attributes[broker.AttrTraceID] = "explicit"
	_, _ = b.Publish(ctx, "topic", msg)
	if got := d.Published("topic")[1].Attributes[broker.AttrTraceID]; got != "explicit" {
		t.Errorf("trace attribute = %q, want explicit", got)
	}
}

func TestDisablePropagation(t *testing.T) {
	observe(t)
	b, d, _ := setup(t, broker.Options{DisablePropagation: true}, memory.Options{}, func(context.Context, *broker.Message) error { return nil })
	ctx := ctxid.WithContext(context.Background(), "trace-1")
	if _, err := b.Publish(ctx, "topic", &broker.Message{}); err != nil {
		t.Fatal(err)
	}
	if attrs := d.Published("topic")[0].Attributes; len(attrs) != 0 {
		t.Errorf("attributes = %v, want none", attrs)
	}
}

func TestNackRedeliversThenDeadLetters(t *testing.T) {
	logs := observe(t)
	var calls atomic.Int32
	b, d, wait := setup(t, broker.Options{}, memory.Options{MaxDeliveryAttempts: 3},
		func(ctx context.Context, msg *broker.Message) error {
			calls.Add(1)
			return errors.New("downstream unavailable")
		})
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	wait()

	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
	dead := d.DeadLetters("sub")
	if len(dead) != 1 || dead[0].DeliveryAttempt != 3 {
		t.Errorf("dead letters = %+v", dead)
	}
	if n := logs.FilterMessage("broker message nacked").FilterLevelExact(zapcore.WarnLevel).Len(); n != 3 {
		t.Errorf("nacked warn lines = %d, want 3", n)
	}
	if n := logs.FilterMessage("op failed").FilterLevelExact(zapcore.ErrorLevel).Len(); n != 3 {
		t.Errorf("op failed error lines = %d, want 3 (one per delivery)", n)
	}
}

func TestPermanentErrorAcks(t *testing.T) {
	logs := observe(t)
	var calls atomic.Int32
	b, d, wait := setup(t, broker.Options{}, memory.Options{}, func(ctx context.Context, msg *broker.Message) error {
		calls.Add(1)
		return broker.Permanent(errors.New("invalid payload"))
	})
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{}); err != nil {
		t.Fatal(err)
	}
	wait()
	if calls.Load() != 1 || len(d.DeadLetters("sub")) != 0 {
		t.Errorf("calls=%d dead=%d, want 1 / 0", calls.Load(), len(d.DeadLetters("sub")))
	}
	if n := logs.FilterMessage("broker message dropped").FilterLevelExact(zapcore.ErrorLevel).Len(); n != 1 {
		t.Errorf("dropped lines = %d", n)
	}
}

func TestPanicRecovered(t *testing.T) {
	logs := observe(t)
	var calls atomic.Int32
	b, _, wait := setup(t, broker.Options{}, memory.Options{MaxDeliveryAttempts: 2}, func(ctx context.Context, msg *broker.Message) error {
		calls.Add(1)
		panic("boom")
	})
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{}); err != nil {
		t.Fatal(err)
	}
	wait()
	if calls.Load() != 2 {
		t.Errorf("panicking message should be nacked and redelivered, calls = %d", calls.Load())
	}
	lines := logs.FilterMessage("broker message nacked").All()
	if len(lines) == 0 || lines[0].ContextMap()["stack"] == nil {
		t.Errorf("nacked line should carry the stack: %v", lines)
	}
}

func TestHandlerTimeout(t *testing.T) {
	observe(t)
	got := make(chan error, 1)
	b, _, wait := setup(t, broker.Options{HandlerTimeout: 20 * time.Millisecond}, memory.Options{MaxDeliveryAttempts: 1},
		func(ctx context.Context, msg *broker.Message) error {
			<-ctx.Done()
			got <- ctx.Err()
			return ctx.Err()
		})
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{}); err != nil {
		t.Fatal(err)
	}
	wait()
	if err := <-got; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handler ctx err = %v", err)
	}
}

func TestMiddlewareOrder(t *testing.T) {
	observe(t)
	var mu sync.Mutex
	var order []string
	mw := func(name string) broker.Middleware {
		return func(next broker.Handler) broker.Handler {
			return func(ctx context.Context, msg *broker.Message) error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				return next(ctx, msg)
			}
		}
	}
	b, _, wait := setup(t, broker.Options{Middleware: []broker.Middleware{mw("a"), mw("b")}}, memory.Options{},
		func(context.Context, *broker.Message) error {
			mu.Lock()
			order = append(order, "h")
			mu.Unlock()
			return nil
		})
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{}); err != nil {
		t.Fatal(err)
	}
	wait()
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "h" {
		t.Fatalf("order = %v", order)
	}
}

func TestDisableLogging(t *testing.T) {
	logs := observe(t)
	b, _, wait := setup(t, broker.Options{DisableLogging: true}, memory.Options{}, func(context.Context, *broker.Message) error { return nil })
	if _, err := b.Publish(context.Background(), "topic", &broker.Message{}); err != nil {
		t.Fatal(err)
	}
	wait()
	if n := logs.Len(); n != 0 {
		t.Fatalf("got %d log lines, want 0: %v", n, logs.All())
	}
}

func TestPublishErrors(t *testing.T) {
	logs := observe(t)
	d := memory.New(memory.Options{})
	b := broker.New(d, broker.Options{})
	ctx := context.Background()

	if _, err := b.Publish(ctx, "", &broker.Message{}); err == nil {
		t.Error("empty topic should fail")
	}
	if _, err := b.Publish(ctx, "topic", nil); err == nil {
		t.Error("nil message should fail")
	}
	_ = b.Close()
	if _, err := b.Publish(ctx, "topic", &broker.Message{}); !errors.Is(err, broker.ErrClosed) {
		t.Errorf("publish after close: %v", err)
	}
	if n := logs.FilterMessage("broker publish failed").FilterLevelExact(zapcore.ErrorLevel).Len(); n != 1 {
		t.Errorf("publish failed lines = %d", n)
	}
}

func TestSubscribeErrors(t *testing.T) {
	observe(t)
	b := broker.New(memory.New(memory.Options{}), broker.Options{})
	defer b.Close()
	ctx := context.Background()
	h := func(context.Context, *broker.Message) error { return nil }

	if err := b.Subscribe(ctx, "", h); err == nil {
		t.Error("empty subscription should fail")
	}
	if err := b.Subscribe(ctx, "sub", nil); err == nil {
		t.Error("nil handler should fail")
	}
	if err := b.Subscribe(ctx, "missing", h); !errors.Is(err, memory.ErrNotFound) {
		t.Errorf("missing subscription: %v", err)
	}
}

func TestCloseStopsSubscribe(t *testing.T) {
	observe(t)
	d := memory.New(memory.Options{})
	_ = d.CreateSubscription("sub", "topic")
	b := broker.New(d, broker.Options{})
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(context.Background(), "sub", func(context.Context, *broker.Message) error { return nil })
	}()
	time.Sleep(10 * time.Millisecond) // let Subscribe start
	_ = b.Close()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, broker.ErrClosed) {
			t.Fatalf("Subscribe = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not return after Close")
	}
}

func TestPermanentHelpers(t *testing.T) {
	if broker.Permanent(nil) != nil {
		t.Error("Permanent(nil) should be nil")
	}
	base := errors.New("bad")
	err := broker.Permanent(base)
	if !broker.IsPermanent(err) || !errors.Is(err, base) {
		t.Errorf("Permanent lost information: %v", err)
	}
	wrapped := errors.Join(errors.New("ctx"), err)
	if !broker.IsPermanent(wrapped) {
		t.Error("IsPermanent should see through wrapping")
	}
	if broker.IsPermanent(base) {
		t.Error("plain error is not permanent")
	}
}
