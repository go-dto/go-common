package memory

import (
	"container/list"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-dto/go-common/cache"
)

const (
	DefaultTTL           = 5 * time.Minute
	DefaultMaxEntries    = 10_000
	DefaultMaxKeyBytes   = 1024
	DefaultMaxValueBytes = 1 << 20
)

// Options configures Store. Zero values use bounded defaults.
type Options struct {
	DefaultTTL    time.Duration
	MaxEntries    int
	MaxKeyBytes   int
	MaxValueBytes int

	// Now is intended for deterministic tests. Nil uses time.Now.
	Now func() time.Time
}

type entry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

// Store is a concurrent bounded LRU cache.
type Store struct {
	mu     sync.Mutex
	opts   Options
	now    func() time.Time
	items  map[string]*list.Element
	lru    *list.List
	closed bool
}

var _ cache.Store = (*Store)(nil)

// New returns an empty cache.
func New(opts Options) (*Store, error) {
	opts = withDefaults(opts)
	if opts.DefaultTTL <= 0 {
		return nil, cache.NewError("new", cache.KindInvalid, errors.New("default TTL must be positive"))
	}
	if opts.MaxEntries <= 0 || opts.MaxKeyBytes <= 0 || opts.MaxValueBytes <= 0 {
		return nil, cache.NewError("new", cache.KindInvalid, errors.New("cache bounds must be positive"))
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Store{opts: opts, now: now, items: make(map[string]*list.Element), lru: list.New()}, nil
}

func withDefaults(opts Options) Options {
	if opts.DefaultTTL == 0 {
		opts.DefaultTTL = DefaultTTL
	}
	if opts.MaxEntries == 0 {
		opts.MaxEntries = DefaultMaxEntries
	}
	if opts.MaxKeyBytes == 0 {
		opts.MaxKeyBytes = DefaultMaxKeyBytes
	}
	if opts.MaxValueBytes == 0 {
		opts.MaxValueBytes = DefaultMaxValueBytes
	}
	return opts
}

// Get returns a copy of the cached value.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if err := contextError(ctx, "get"); err != nil {
		return nil, err
	}
	if err := validateKey(key, s.opts.MaxKeyBytes, "get"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, cache.ErrClosed
	}
	e, ok := s.items[key]
	if !ok {
		return nil, cache.ErrMiss
	}
	item := e.Value.(*entry)
	if !s.now().Before(item.expiresAt) {
		s.remove(e)
		return nil, cache.ErrMiss
	}
	s.lru.MoveToFront(e)
	return slices.Clone(item.value), nil
}

// Set stores a copy of value. A zero TTL uses Options.DefaultTTL.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := contextError(ctx, "set"); err != nil {
		return err
	}
	if err := validateKey(key, s.opts.MaxKeyBytes, "set"); err != nil {
		return err
	}
	if len(value) > s.opts.MaxValueBytes {
		return cache.NewError("set", cache.KindTooLarge, nil)
	}
	if ttl == 0 {
		ttl = s.opts.DefaultTTL
	}
	if ttl < 0 {
		return cache.NewError("set", cache.KindInvalid, errors.New("TTL must not be negative"))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return cache.ErrClosed
	}
	item := &entry{key: key, value: slices.Clone(value), expiresAt: s.now().Add(ttl)}
	if existing, ok := s.items[key]; ok {
		existing.Value = item
		s.lru.MoveToFront(existing)
		return nil
	}
	e := s.lru.PushFront(item)
	s.items[key] = e
	for len(s.items) > s.opts.MaxEntries {
		s.remove(s.lru.Back())
	}
	return nil
}

// Delete removes keys. Missing keys are ignored.
func (s *Store) Delete(ctx context.Context, keys ...string) error {
	if err := contextError(ctx, "delete"); err != nil {
		return err
	}
	for _, key := range keys {
		if err := validateKey(key, s.opts.MaxKeyBytes, "delete"); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return cache.ErrClosed
	}
	for _, key := range keys {
		if e, ok := s.items[key]; ok {
			s.remove(e)
		}
	}
	return nil
}

// Close clears the cache and is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	clear(s.items)
	s.lru.Init()
	return nil
}

func (s *Store) remove(e *list.Element) {
	if e == nil {
		return
	}
	delete(s.items, e.Value.(*entry).key)
	s.lru.Remove(e)
}

func validateKey(key string, max int, operation string) error {
	if key == "" || len(key) > max || !utf8.ValidString(key) || strings.ContainsAny(key, "\r\n\x00") {
		return cache.NewError(operation, cache.KindInvalid, errors.New("invalid cache key"))
	}
	return nil
}

func contextError(ctx context.Context, operation string) error {
	if ctx == nil {
		return cache.NewError(operation, cache.KindInvalid, errors.New("nil context"))
	}
	if err := ctx.Err(); err != nil {
		return cache.NewError(operation, cache.KindOf(err), err)
	}
	return nil
}
