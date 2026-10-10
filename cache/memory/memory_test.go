package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-dto/go-common/cache"
)

func TestStoreLifecycleExpiryEvictionAndCopies(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s, err := New(Options{DefaultTTL: time.Minute, MaxEntries: 2, MaxValueBytes: 8, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	value := []byte("one")
	if err := s.Set(ctx, "a", value, 0); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	got, err := s.Get(ctx, "a")
	if err != nil || string(got) != "one" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	got[0] = 'Y'
	got, _ = s.Get(ctx, "a")
	if string(got) != "one" {
		t.Fatal("Get did not return a copy")
	}
	if err := s.Set(ctx, "b", []byte("two"), 0); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Get(ctx, "a") // a is most recently used; c evicts b.
	if err := s.Set(ctx, "c", []byte("three"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "b"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("evicted key error = %v", err)
	}
	now = now.Add(time.Minute)
	if _, err := s.Get(ctx, "a"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("expired key error = %v", err)
	}
	if err := s.Delete(ctx, "c", "missing"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "c"); !cache.IsMiss(err) {
		t.Fatalf("deleted key error = %v", err)
	}
	if err := s.Close(); err != nil || s.Close() != nil {
		t.Fatal("Close must be idempotent")
	}
	if _, err := s.Get(ctx, "a"); !errors.Is(err, cache.ErrClosed) {
		t.Fatalf("closed Get error = %v", err)
	}
}

func TestValidationAndContext(t *testing.T) {
	bad := []Options{
		{DefaultTTL: -1},
		{MaxEntries: -1},
		{MaxKeyBytes: -1},
		{MaxValueBytes: -1},
	}
	for _, opts := range bad {
		if _, err := New(opts); cache.KindOf(err) != cache.KindInvalid {
			t.Fatalf("New(%+v) error = %v", opts, err)
		}
	}
	s, _ := New(Options{MaxKeyBytes: 3, MaxValueBytes: 3})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Set(ctx, "a", nil, 0); cache.KindOf(err) != cache.KindCanceled {
		t.Fatalf("canceled Set = %v", err)
	}
	//lint:ignore SA1012 exercising the public nil-context guard
	if err := s.Set(nil, "a", nil, 0); cache.KindOf(err) != cache.KindInvalid {
		t.Fatalf("nil context Set = %v", err)
	}
	for _, key := range []string{"", "toolong", "a\n"} {
		if err := s.Set(context.Background(), key, nil, 0); cache.KindOf(err) != cache.KindInvalid {
			t.Fatalf("key %q error = %v", key, err)
		}
	}
	if err := s.Set(context.Background(), "a", []byte("four"), 0); cache.KindOf(err) != cache.KindTooLarge {
		t.Fatalf("large value error = %v", err)
	}
	if err := s.Set(context.Background(), "a", nil, -time.Second); cache.KindOf(err) != cache.KindInvalid {
		t.Fatalf("negative TTL error = %v", err)
	}
	if err := s.Delete(context.Background(), "toolong"); cache.KindOf(err) != cache.KindInvalid {
		t.Fatalf("invalid Delete error = %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s, err := New(Options{MaxEntries: 128})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				_ = s.Set(context.Background(), "shared", []byte("value"), 0)
				_, _ = s.Get(context.Background(), "shared")
			}
		})
	}
	wg.Wait()
}
