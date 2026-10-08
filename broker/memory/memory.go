// Package memory is an in-process broker.Driver for unit tests and local
// development. It mimics Pub/Sub semantics: a topic fans out to every
// subscription attached to it, delivery is at-least-once, nacked messages are
// redelivered with a growing DeliveryAttempt and moved to a dead letter list
// after MaxDeliveryAttempts. Ordering keys are not honoured.
//
//	d := memory.New(memory.Options{})
//	_ = d.CreateSubscription("webhook-sub", "webhook")
//	b := broker.New(d, broker.Options{System: memory.System})
//	go b.Subscribe(ctx, "webhook-sub", handler)
//	_, _ = b.Publish(ctx, "webhook", &broker.Message{Data: payload})
//	_ = d.WaitIdle(ctx, "webhook-sub") // all messages handled
package memory

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-dto/go-common/broker"
)

// System is the messaging_system value for this driver.
const System = "memory"

// Defaults applied by New when the matching Options field is zero.
const (
	DefaultConcurrency         = 10
	DefaultMaxDeliveryAttempts = 5
)

// ErrNotFound is returned for an unknown subscription.
var ErrNotFound = errors.New("memory: subscription not found")

// Options configures New. The zero value is usable.
type Options struct {
	// Concurrency is the number of handler calls in flight per Receive.
	// Default 10.
	Concurrency int

	// RedeliveryDelay is the wait before a nacked message is queued again.
	// Default 0 (immediately).
	RedeliveryDelay time.Duration

	// MaxDeliveryAttempts moves a message to DeadLetters after this many
	// failed deliveries. Default 5; negative = redeliver forever.
	MaxDeliveryAttempts int
}

// Driver is safe for concurrent use.
type Driver struct {
	opts   Options
	ctx    context.Context // cancelled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup // active Receive calls

	mu        sync.Mutex
	closed    bool
	nextID    int64
	subs      map[string]*subscription
	topics    map[string][]*subscription
	published map[string][]broker.Message
}

type subscription struct {
	queue    []*broker.Message
	inflight int
	delayed  int // nacked messages waiting for RedeliveryDelay
	dead     []broker.Message
	changed  chan struct{} // closed and replaced on every state change
}

var _ broker.Driver = (*Driver)(nil)

// New returns an empty driver.
func New(opts Options) *Driver {
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultConcurrency
	}
	if opts.MaxDeliveryAttempts == 0 {
		opts.MaxDeliveryAttempts = DefaultMaxDeliveryAttempts
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Driver{
		opts:      opts,
		ctx:       ctx,
		cancel:    cancel,
		subs:      make(map[string]*subscription),
		topics:    make(map[string][]*subscription),
		published: make(map[string][]broker.Message),
	}
}

// CreateSubscription attaches a new subscription to topic. Only messages
// published after this call are delivered to it.
func (d *Driver) CreateSubscription(name, topic string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return broker.ErrClosed
	}
	if _, ok := d.subs[name]; ok {
		return fmt.Errorf("memory: subscription %q already exists", name)
	}
	s := &subscription{changed: make(chan struct{})}
	d.subs[name] = s
	d.topics[topic] = append(d.topics[topic], s)
	return nil
}

// Publish implements broker.Driver.
func (d *Driver) Publish(ctx context.Context, topic string, msg *broker.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return "", broker.ErrClosed
	}
	d.nextID++
	stored := clone(msg)
	stored.ID = strconv.FormatInt(d.nextID, 10)
	stored.PublishTime = time.Now()
	stored.DeliveryAttempt = 0

	d.published[topic] = append(d.published[topic], *clone(stored))
	for _, s := range d.topics[topic] {
		s.queue = append(s.queue, clone(stored))
		s.notify()
	}
	return stored.ID, nil
}

// Receive implements broker.Driver.
func (d *Driver) Receive(ctx context.Context, name string, h broker.Handler) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return broker.ErrClosed
	}
	s, ok := d.subs[name]
	if !ok {
		d.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	d.wg.Add(1)
	d.mu.Unlock()
	defer d.wg.Done()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(d.ctx, cancel)()

	var handlers sync.WaitGroup
	defer handlers.Wait()
	sem := make(chan struct{}, d.opts.Concurrency)
	for {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
		entry, ok := d.next(ctx, s)
		if !ok {
			return nil
		}
		handlers.Go(func() {
			defer func() { <-sem }()
			err := h(ctx, clone(entry))
			d.settle(s, entry, err)
		})
	}
}

// next blocks until a message is queued for s or ctx is done.
func (d *Driver) next(ctx context.Context, s *subscription) (*broker.Message, bool) {
	for {
		d.mu.Lock()
		if len(s.queue) > 0 {
			m := s.queue[0]
			s.queue[0] = nil
			s.queue = s.queue[1:]
			m.DeliveryAttempt++
			s.inflight++
			d.mu.Unlock()
			return m, true
		}
		ch := s.changed
		d.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, false
		}
	}
}

func (d *Driver) settle(s *subscription, m *broker.Message, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	defer s.notify()
	s.inflight--
	switch {
	case err == nil:
	case d.opts.MaxDeliveryAttempts > 0 && m.DeliveryAttempt >= d.opts.MaxDeliveryAttempts:
		s.dead = append(s.dead, *m)
	case d.opts.RedeliveryDelay <= 0:
		s.queue = append(s.queue, m)
	default:
		s.delayed++
		time.AfterFunc(d.opts.RedeliveryDelay, func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			s.delayed--
			s.queue = append(s.queue, m)
			s.notify()
		})
	}
}

// WaitIdle blocks until subscription has no queued, in-flight or
// pending-redelivery messages, or ctx is done.
func (d *Driver) WaitIdle(ctx context.Context, name string) error {
	for {
		d.mu.Lock()
		s, ok := d.subs[name]
		if !ok {
			d.mu.Unlock()
			return fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		if len(s.queue) == 0 && s.inflight == 0 && s.delayed == 0 {
			d.mu.Unlock()
			return nil
		}
		ch := s.changed
		d.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Published returns copies of every message published to topic, in order,
// including messages no subscription received.
func (d *Driver) Published(topic string) []broker.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	return cloneAll(d.published[topic])
}

// DeadLetters returns copies of the messages subscription gave up on after
// MaxDeliveryAttempts.
func (d *Driver) DeadLetters(name string) []broker.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s, ok := d.subs[name]; ok {
		return cloneAll(s.dead)
	}
	return nil
}

// Close stops every Receive (waiting for in-flight handlers) and makes
// further Publish/Receive calls return broker.ErrClosed.
func (d *Driver) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	d.cancel()
	d.wg.Wait()
	return nil
}

// notify wakes everything waiting on s. d.mu must be held.
func (s *subscription) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// clone deep-copies m so handlers and tests cannot alter stored messages.
func clone(m *broker.Message) *broker.Message {
	c := *m
	c.Data = slices.Clone(m.Data)
	c.Attributes = maps.Clone(m.Attributes)
	return &c
}

func cloneAll(ms []broker.Message) []broker.Message {
	out := make([]broker.Message, len(ms))
	for i := range ms {
		out[i] = *clone(&ms[i])
	}
	return out
}
