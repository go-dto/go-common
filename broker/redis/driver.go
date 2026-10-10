package redis

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/go-dto/go-common/broker"
	"github.com/go-dto/go-common/database"
	"github.com/go-dto/go-common/internal/rediscore"
	"github.com/go-dto/go-common/logger"
)

var deadLetterScript = goredis.NewScript(`
local dlq_id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[1], '*',
  'data', ARGV[2],
  'attributes', ARGV[3],
  'source_id', ARGV[4],
  'delivery_attempt', ARGV[5],
  'failure_kind', ARGV[6])
redis.call('XACK', KEYS[1], ARGV[7], ARGV[4])
return dlq_id
`)

// Driver implements broker.Driver. It is safe for concurrent publishers and
// supports one active Receive call per configured subscription per process.
type Driver struct {
	cfg    Config
	handle *rediscore.Handle
	client goredis.UniversalClient

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu            sync.Mutex
	closed        bool
	receiving     map[string]bool
	receiveErrors map[string]error

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

var _ broker.Driver = (*Driver)(nil)

// NewDriver creates a Redis Streams driver and verifies every configured
// consumer group. With AutoCreateGroups it creates missing streams/groups for
// controlled test/dev environments.
func NewDriver(ctx context.Context, cfg Config) (*Driver, error) {
	normalized, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	handle, err := rediscore.Open(ctx, normalized.core())
	if err != nil {
		return nil, safeError("connect", err)
	}
	dctx, cancel := context.WithCancel(context.Background())
	runtimeConfig := normalized
	runtimeConfig.Password = ""
	runtimeConfig.SentinelPassword = ""
	runtimeConfig.Addrs = nil
	runtimeConfig.ClientName = ""
	runtimeConfig.Username = ""
	runtimeConfig.SentinelUsername = ""
	runtimeConfig.MasterName = ""
	runtimeConfig.TLSConfig = nil
	runtimeConfig.Client = nil
	d := &Driver{
		cfg: runtimeConfig, handle: handle, client: handle.Client(), ctx: dctx, cancel: cancel,
		receiving: make(map[string]bool), receiveErrors: make(map[string]error), closeDone: make(chan struct{}),
	}
	if err := d.prepareSubscriptions(ctx); err != nil {
		cancel()
		_ = handle.Close()
		return nil, err
	}
	return d, nil
}

// New creates the instrumented broker used by application code.
func New(ctx context.Context, cfg Config, opts broker.Options) (*broker.Broker, error) {
	normalized, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	if opts.HandlerTimeout > 0 && normalized.ClaimMinIdle <= opts.HandlerTimeout {
		return nil, errors.New("broker/redis: ClaimMinIdle must exceed HandlerTimeout")
	}
	driver, err := NewDriver(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if opts.System == "" {
		opts.System = System
	}
	return broker.New(driver, opts), nil
}

// Client returns the underlying client for diagnostics and explicitly scoped
// administrative tooling. Application publish/consume code should use Broker.
func (d *Driver) Client() goredis.UniversalClient { return d.client }

// StreamKey returns the deterministic Redis stream key for a configured topic.
func (d *Driver) StreamKey(topic string) (string, error) {
	if d == nil {
		return "", broker.ErrClosed
	}
	if !d.hasTopic(topic) {
		return "", errors.New("broker/redis: unknown topic")
	}
	return d.cfg.streamKey(topic), nil
}

// DeadLetterKey returns the deterministic DLQ stream key for a subscription.
func (d *Driver) DeadLetterKey(subscription string) (string, error) {
	if d == nil {
		return "", broker.ErrClosed
	}
	sub, ok := d.cfg.Subscriptions[subscription]
	if !ok {
		return "", errors.New("broker/redis: unknown subscription")
	}
	return d.cfg.deadLetterKey(subscription, sub), nil
}

// Publish appends one bounded message to a configured topic stream.
func (d *Driver) Publish(ctx context.Context, topic string, msg *broker.Message) (string, error) {
	if ctx == nil {
		return "", errors.New("broker/redis: nil publish context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if d == nil || d.isClosed() {
		return "", broker.ErrClosed
	}
	if !d.hasTopic(topic) {
		return "", errors.New("broker/redis: unknown topic")
	}
	values, err := d.encode(msg)
	if err != nil {
		return "", err
	}
	id, err := d.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: d.cfg.streamKey(topic), MaxLen: d.cfg.StreamMaxLen, Approx: true, Values: values,
	}).Result()
	if err != nil {
		return "", safeError("publish", err)
	}
	return id, nil
}

// Receive consumes new and stale pending messages until cancellation/Close.
func (d *Driver) Receive(ctx context.Context, subscription string, h broker.Handler) (result error) {
	if ctx == nil {
		return errors.New("broker/redis: nil receive context")
	}
	if h == nil {
		return errors.New("broker/redis: nil handler")
	}
	if d == nil {
		return broker.ErrClosed
	}
	sub, ok := d.cfg.Subscriptions[subscription]
	if !ok {
		return errors.New("broker/redis: unknown subscription")
	}
	if err := d.beginReceive(subscription); err != nil {
		return err
	}
	defer func() { d.endReceive(subscription, result) }()

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopDriverCancel := context.AfterFunc(d.ctx, cancel)
	defer stopDriverCancel()

	stream := d.cfg.streamKey(sub.Topic)
	sem := make(chan struct{}, d.cfg.Concurrency)
	var handlers sync.WaitGroup
	defer handlers.Wait()
	claimCursor := "0-0"
	retryBackoff := d.cfg.ReceiveRetryInitialBackoff

	for {
		claimed, next, err := d.client.XAutoClaim(rctx, &goredis.XAutoClaimArgs{
			Stream: stream, Group: sub.Group, Consumer: d.cfg.ConsumerName,
			MinIdle: d.cfg.ClaimMinIdle, Start: claimCursor, Count: d.cfg.ReadBatchSize,
		}).Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			if rctx.Err() != nil {
				return nil
			}
			if isSubscriptionNotProvisioned(err) {
				return safeSubscriptionError(err)
			}
			if !d.waitReceiveRetry(rctx, subscription, retryBackoff, err) {
				return nil
			}
			retryBackoff = nextBackoff(retryBackoff, d.cfg.ReceiveRetryMaxBackoff)
			continue
		}
		d.markReceiveHealthy(subscription)
		retryBackoff = d.cfg.ReceiveRetryInitialBackoff
		claimCursor = next
		if claimCursor == "" || claimCursor == "0-0" {
			claimCursor = "0-0"
		}
		if !d.dispatch(rctx, subscription, sub, stream, claimed, h, sem, &handlers) {
			return nil
		}

		streams, err := d.client.XReadGroup(rctx, &goredis.XReadGroupArgs{
			Group: sub.Group, Consumer: d.cfg.ConsumerName,
			Streams: []string{stream, ">"}, Count: d.cfg.ReadBatchSize, Block: d.cfg.BlockTimeout,
		}).Result()
		if errors.Is(err, goredis.Nil) {
			d.markReceiveHealthy(subscription)
			retryBackoff = d.cfg.ReceiveRetryInitialBackoff
			continue
		}
		if err != nil {
			if rctx.Err() != nil {
				return nil
			}
			if isSubscriptionNotProvisioned(err) {
				return safeSubscriptionError(err)
			}
			if !d.waitReceiveRetry(rctx, subscription, retryBackoff, err) {
				return nil
			}
			retryBackoff = nextBackoff(retryBackoff, d.cfg.ReceiveRetryMaxBackoff)
			continue
		}
		d.markReceiveHealthy(subscription)
		retryBackoff = d.cfg.ReceiveRetryInitialBackoff
		for _, result := range streams {
			if !d.dispatch(rctx, subscription, sub, stream, result.Messages, h, sem, &handlers) {
				return nil
			}
		}
	}
}

func (d *Driver) dispatch(ctx context.Context, subscription string, sub Subscription, stream string, messages []goredis.XMessage, h broker.Handler, sem chan struct{}, handlers *sync.WaitGroup) bool {
	for i := range messages {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		raw := messages[i]
		handlers.Go(func() {
			defer func() { <-sem }()
			d.process(ctx, subscription, sub, stream, raw, h)
		})
	}
	return true
}

func (d *Driver) process(ctx context.Context, subscription string, sub Subscription, stream string, raw goredis.XMessage, h broker.Handler) {
	attempt, err := d.deliveryAttempt(ctx, stream, sub.Group, raw.ID)
	if err != nil {
		d.logSettleFailure(ctx, subscription, raw.ID, "inspect", err)
		return
	}
	msg, decodeErr := d.decode(raw, attempt)
	if decodeErr != nil {
		if err := d.deadLetter(ctx, subscription, sub, stream, raw, attempt, "malformed"); err != nil {
			d.logSettleFailure(ctx, subscription, raw.ID, "dead_letter", err)
		}
		return
	}
	if int64(attempt) > d.cfg.MaxDeliveryAttempts {
		if err := d.deadLetter(ctx, subscription, sub, stream, raw, attempt, "max_attempts"); err != nil {
			d.logSettleFailure(ctx, subscription, raw.ID, "dead_letter", err)
		}
		return
	}
	if err := h(ctx, msg); err != nil {
		if int64(attempt) >= d.cfg.MaxDeliveryAttempts {
			if dlqErr := d.deadLetter(ctx, subscription, sub, stream, raw, attempt, "max_attempts"); dlqErr != nil {
				d.logSettleFailure(ctx, subscription, raw.ID, "dead_letter", dlqErr)
			}
		}
		return // leave pending for XAUTOCLAIM when attempts remain
	}
	if err := d.client.XAck(ctx, stream, sub.Group, raw.ID).Err(); err != nil {
		d.logSettleFailure(ctx, subscription, raw.ID, "ack", safeError("ack", err))
	}
}

func (d *Driver) deliveryAttempt(ctx context.Context, stream, group, id string) (int, error) {
	pending, err := d.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: stream, Group: group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil {
		return 0, safeError("inspect pending message", err)
	}
	if len(pending) == 0 || pending[0].ID != id {
		return 1, nil
	}
	if pending[0].RetryCount < 1 {
		return 1, nil
	}
	return int(pending[0].RetryCount), nil
}

func (d *Driver) deadLetter(ctx context.Context, subscription string, sub Subscription, stream string, raw goredis.XMessage, attempt int, failureKind string) error {
	data, dataOK := fieldBytes(raw.Values[fieldData])
	attrs, attrsOK := fieldBytes(raw.Values[fieldAttributes])
	if !dataOK || !attrsOK || len(data) > d.cfg.MaxMessageBytes || len(attrs) > d.cfg.MaxMessageBytes-len(data) {
		data = nil
		attrs = []byte("{}")
	}
	dlq := d.cfg.deadLetterKey(subscription, sub)
	if _, err := deadLetterScript.Run(ctx, d.client, []string{stream, dlq},
		d.cfg.DeadLetterMaxLen, data, attrs, raw.ID, attempt, failureKind, sub.Group,
	).Result(); err != nil {
		return safeError("dead letter", err)
	}
	return nil
}

func (d *Driver) prepareSubscriptions(ctx context.Context) error {
	for _, sub := range d.cfg.Subscriptions {
		stream := d.cfg.streamKey(sub.Topic)
		if d.cfg.AutoCreateGroups {
			err := d.client.XGroupCreateMkStream(ctx, stream, sub.Group, "0").Err()
			if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
				return safeError("create subscription", err)
			}
		}
	}
	if err := d.verifySubscriptions(ctx); err != nil {
		return err
	}
	return d.verifyConsumerCapabilities(ctx)
}

func (d *Driver) verifyConsumerCapabilities(ctx context.Context) error {
	checked := make(map[string]struct{}, len(d.cfg.Subscriptions))
	for _, sub := range d.cfg.Subscriptions {
		stream := d.cfg.streamKey(sub.Topic)
		key := stream + "\x00" + sub.Group
		if _, ok := checked[key]; ok {
			continue
		}
		checked[key] = struct{}{}
		// The maximum duration prevents startup validation from reclaiming real
		// pending work while still proving Redis and its ACL support XAUTOCLAIM.
		_, _, err := d.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream: stream, Group: sub.Group, Consumer: d.cfg.ConsumerName,
			MinIdle: time.Duration(1<<63 - 1), Start: "0-0", Count: 1,
		}).Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return errors.New("broker/redis: required XAUTOCLAIM capability is unavailable")
		}
	}
	return nil
}

func (d *Driver) verifySubscriptions(ctx context.Context) error {
	for name, sub := range d.cfg.Subscriptions {
		groups, err := d.client.XInfoGroups(ctx, d.cfg.streamKey(sub.Topic)).Result()
		if err != nil {
			return fmt.Errorf("broker/redis: subscription %q is not provisioned", name)
		}
		found := false
		for _, group := range groups {
			if group.Name == sub.Group {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("broker/redis: subscription %q is not provisioned", name)
		}
	}
	return nil
}

func (d *Driver) beginReceive(subscription string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return broker.ErrClosed
	}
	if d.receiving[subscription] {
		return errors.New("broker/redis: subscription already has an active receiver in this process")
	}
	d.receiving[subscription] = true
	delete(d.receiveErrors, subscription)
	d.wg.Add(1)
	return nil
}

func (d *Driver) endReceive(subscription string, err error) {
	d.mu.Lock()
	delete(d.receiving, subscription)
	if err == nil {
		delete(d.receiveErrors, subscription)
	} else {
		d.receiveErrors[subscription] = safeSubscriptionError(err)
	}
	d.mu.Unlock()
	d.wg.Done()
}

func (d *Driver) waitReceiveRetry(ctx context.Context, subscription string, backoff time.Duration, err error) bool {
	safeErr := safeSubscriptionError(err)
	d.mu.Lock()
	d.receiveErrors[subscription] = safeErr
	d.mu.Unlock()

	// Jitter keeps replicas recovering at different times without allowing a
	// tight retry loop. Resource names are configured, bounded values.
	delay := backoff/2 + time.Duration(rand.Int64N(int64(backoff-backoff/2)+1))
	logger.FromContext(ctx).Warn("redis broker receive retry",
		zap.String(broker.KeySystem, System),
		zap.String(broker.KeySubscription, subscription),
		zap.String("error_kind", rediscore.ErrorKind(err)),
		zap.Duration("retry_backoff", delay),
	)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (d *Driver) markReceiveHealthy(subscription string) {
	d.mu.Lock()
	delete(d.receiveErrors, subscription)
	d.mu.Unlock()
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func (d *Driver) hasTopic(topic string) bool {
	for _, configured := range d.cfg.Topics {
		if configured == topic {
			return true
		}
	}
	return false
}

func (d *Driver) isClosed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closed
}

// Ping checks readiness without exposing endpoint or credential details.
func (d *Driver) Ping(ctx context.Context) error {
	if d == nil || d.isClosed() {
		return broker.ErrClosed
	}
	if err := d.handle.Ping(ctx); err != nil {
		return safeError("ping", err)
	}
	return nil
}

// Readiness verifies connectivity, configured consumer groups, and the latest
// state of active receivers. Returned errors never include Redis endpoints or
// credentials.
func (d *Driver) Readiness(ctx context.Context) error {
	if err := d.Ping(ctx); err != nil {
		return err
	}
	if err := d.verifySubscriptions(ctx); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, err := range d.receiveErrors {
		if err != nil {
			return errors.New("broker/redis: receiver is unhealthy")
		}
	}
	return nil
}

// ConsumerReadiness additionally requires one active Receive call for every
// configured subscription. Publisher-only processes should use Readiness.
func (d *Driver) ConsumerReadiness(ctx context.Context) error {
	if err := d.Readiness(ctx); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for subscription := range d.cfg.Subscriptions {
		if !d.receiving[subscription] {
			return errors.New("broker/redis: consumer is not running")
		}
	}
	return nil
}

// Check returns a readiness check compatible with database.HealthHandler.
func Check(name string, driver *Driver) database.Check {
	if driver == nil {
		return database.Check{Name: name, Ping: func(context.Context) error { return broker.ErrClosed }}
	}
	return database.Check{Name: name, Ping: driver.Readiness}
}

// ConsumerCheck returns a readiness check that also requires every configured
// subscription to have an active Receive loop.
func ConsumerCheck(name string, driver *Driver) database.Check {
	if driver == nil {
		return database.Check{Name: name, Ping: func(context.Context) error { return broker.ErrClosed }}
	}
	return database.Check{Name: name, Ping: driver.ConsumerReadiness}
}

// Close stops receivers, waits for handlers, then closes the package-owned
// Redis client. It is idempotent.
func (d *Driver) Close() error { return d.CloseContext(context.Background()) }

// CloseContext bounds the wait while cleanup continues in the background.
func (d *Driver) CloseContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("broker/redis: nil close context")
	}
	if d == nil {
		return nil
	}
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.mu.Unlock()
		d.cancel()
		go func() {
			d.wg.Wait()
			d.closeErr = d.handle.Close()
			close(d.closeDone)
		}()
	})
	select {
	case <-d.closeDone:
		return d.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Driver) logSettleFailure(ctx context.Context, subscription, messageID, operation string, err error) {
	logger.FromContext(ctx).Warn("redis broker settlement failed",
		zap.String(broker.KeySystem, System), zap.String(broker.KeySubscription, subscription),
		zap.String(broker.KeyMessageID, messageID), zap.String("broker_operation", operation),
		zap.String("error_kind", rediscore.ErrorKind(err)))
}

func safeSubscriptionError(err error) error {
	if isSubscriptionNotProvisioned(err) {
		return errors.New("broker/redis: subscription is not provisioned")
	}
	return safeError("receive", err)
}

func isSubscriptionNotProvisioned(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

func safeError(operation string, err error) error {
	if errors.Is(err, broker.ErrClosed) || rediscore.ErrorKind(err) == "closed" {
		return broker.ErrClosed
	}
	return fmt.Errorf("broker/redis: %s: %w", operation, rediscore.SafeCause(err))
}
