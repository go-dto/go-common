package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-dto/go-common/broker"
)

func receive(t *testing.T, d *Driver, sub string, h broker.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Receive(ctx, sub, h) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Receive: %v", err)
		}
	})
}

func waitIdle(t *testing.T, d *Driver, sub string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.WaitIdle(ctx, sub); err != nil {
		t.Fatal(err)
	}
}

func TestFanOut(t *testing.T) {
	d := New(Options{})
	defer d.Close()
	_ = d.CreateSubscription("a", "topic")
	_ = d.CreateSubscription("b", "topic")
	_ = d.CreateSubscription("other", "other-topic")

	var a, b, other atomic.Int32
	receive(t, d, "a", func(context.Context, *broker.Message) error { a.Add(1); return nil })
	receive(t, d, "b", func(context.Context, *broker.Message) error { b.Add(1); return nil })
	receive(t, d, "other", func(context.Context, *broker.Message) error { other.Add(1); return nil })

	for range 5 {
		if _, err := d.Publish(context.Background(), "topic", &broker.Message{Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	waitIdle(t, d, "a")
	waitIdle(t, d, "b")
	if a.Load() != 5 || b.Load() != 5 || other.Load() != 0 {
		t.Fatalf("a=%d b=%d other=%d", a.Load(), b.Load(), other.Load())
	}
	if n := len(d.Published("topic")); n != 5 {
		t.Errorf("Published = %d", n)
	}
}

func TestHandlerCannotCorruptStoredMessage(t *testing.T) {
	d := New(Options{MaxDeliveryAttempts: 2})
	defer d.Close()
	_ = d.CreateSubscription("sub", "topic")

	var mu sync.Mutex
	var seen []string
	receive(t, d, "sub", func(ctx context.Context, m *broker.Message) error {
		mu.Lock()
		seen = append(seen, string(m.Data)+m.Attributes["k"])
		mu.Unlock()
		m.Data[0] = 'X'
		m.Attributes["k"] = "mutated"
		return errors.New("retry")
	})
	_, _ = d.Publish(context.Background(), "topic", &broker.Message{Data: []byte("a"), Attributes: map[string]string{"k": "v"}})
	waitIdle(t, d, "sub")

	if len(seen) != 2 || seen[0] != "av" || seen[1] != "av" {
		t.Fatalf("seen = %v, redelivery must see the original message", seen)
	}
	if p := d.Published("topic"); string(p[0].Data) != "a" {
		t.Errorf("Published mutated: %q", p[0].Data)
	}
}

func TestDeliveryAttemptAndRedeliveryDelay(t *testing.T) {
	d := New(Options{RedeliveryDelay: 20 * time.Millisecond, MaxDeliveryAttempts: -1})
	defer d.Close()
	_ = d.CreateSubscription("sub", "topic")

	var mu sync.Mutex
	var attempts []int
	var times []time.Time
	receive(t, d, "sub", func(ctx context.Context, m *broker.Message) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, m.DeliveryAttempt)
		times = append(times, time.Now())
		if m.DeliveryAttempt < 3 {
			return errors.New("retry")
		}
		return nil
	})
	_, _ = d.Publish(context.Background(), "topic", &broker.Message{})
	waitIdle(t, d, "sub")

	if len(attempts) != 3 || attempts[0] != 1 || attempts[2] != 3 {
		t.Fatalf("attempts = %v", attempts)
	}
	if gap := times[1].Sub(times[0]); gap < 20*time.Millisecond {
		t.Errorf("redelivered after %v, want ≥ RedeliveryDelay", gap)
	}
	if len(d.DeadLetters("sub")) != 0 {
		t.Error("negative MaxDeliveryAttempts must never dead-letter")
	}
}

func TestConcurrencyLimit(t *testing.T) {
	d := New(Options{Concurrency: 3})
	defer d.Close()
	_ = d.CreateSubscription("sub", "topic")

	var cur, peak atomic.Int32
	receive(t, d, "sub", func(ctx context.Context, m *broker.Message) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		cur.Add(-1)
		return nil
	})
	for range 20 {
		_, _ = d.Publish(context.Background(), "topic", &broker.Message{})
	}
	waitIdle(t, d, "sub")
	if p := peak.Load(); p > 3 || p < 2 {
		t.Fatalf("peak concurrency = %d, want ≤ 3 (and > 1)", p)
	}
}

func TestReceiveWaitsForInFlightHandlers(t *testing.T) {
	d := New(Options{})
	defer d.Close()
	_ = d.CreateSubscription("sub", "topic")

	started := make(chan struct{})
	var finished atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- d.Receive(ctx, "sub", func(context.Context, *broker.Message) error {
			close(started)
			time.Sleep(30 * time.Millisecond)
			finished.Store(true)
			return nil
		})
	}()
	_, _ = d.Publish(context.Background(), "topic", &broker.Message{})
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("Receive returned before the in-flight handler finished")
	}
}

func TestClose(t *testing.T) {
	d := New(Options{})
	_ = d.CreateSubscription("sub", "topic")
	done := make(chan error, 1)
	go func() {
		done <- d.Receive(context.Background(), "sub", func(context.Context, *broker.Message) error { return nil })
	}()
	time.Sleep(10 * time.Millisecond)

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil && !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("Receive = %v", err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if _, err := d.Publish(context.Background(), "topic", &broker.Message{}); !errors.Is(err, broker.ErrClosed) {
		t.Errorf("Publish after Close = %v", err)
	}
	if err := d.CreateSubscription("x", "topic"); !errors.Is(err, broker.ErrClosed) {
		t.Errorf("CreateSubscription after Close = %v", err)
	}
}

func TestCloseCancelsPendingRedelivery(t *testing.T) {
	d := New(Options{RedeliveryDelay: time.Hour, MaxDeliveryAttempts: -1})
	_ = d.CreateSubscription("sub", "topic")
	handled := make(chan struct{})
	receive(t, d, "sub", func(context.Context, *broker.Message) error {
		close(handled)
		return errors.New("retry")
	})
	_, _ = d.Publish(context.Background(), "topic", &broker.Message{})
	<-handled

	deadline := time.Now().Add(time.Second)
	for {
		d.mu.Lock()
		delayed := d.subs["sub"].delayed
		d.mu.Unlock()
		if delayed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("message never entered delayed redelivery")
		}
		time.Sleep(time.Millisecond)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.subs["sub"].delayed != 0 || len(d.subs["sub"].queue) != 0 {
		t.Fatalf("pending redelivery survived Close: %+v", d.subs["sub"])
	}
}

func TestCloseContextBoundsUncooperativeHandler(t *testing.T) {
	d := New(Options{})
	_ = d.CreateSubscription("sub", "topic")
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- d.Receive(context.Background(), "sub", func(context.Context, *broker.Message) error {
			close(started)
			<-release
			return nil
		})
	}()
	_, _ = d.Publish(context.Background(), "topic", &broker.Message{})
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext error=%v", err)
	}
	close(release)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestErrors(t *testing.T) {
	d := New(Options{})
	defer d.Close()
	_ = d.CreateSubscription("sub", "topic")
	if err := d.CreateSubscription("sub", "topic"); err == nil {
		t.Error("duplicate subscription should fail")
	}
	if err := d.Receive(context.Background(), "missing", nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("Receive missing = %v", err)
	}
	if err := d.WaitIdle(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("WaitIdle missing = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Publish(ctx, "topic", &broker.Message{}); !errors.Is(err, context.Canceled) {
		t.Errorf("Publish with cancelled ctx = %v", err)
	}
}
