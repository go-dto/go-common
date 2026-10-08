package broker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger"
	"github.com/go-dto/go-common/logger/ctxid"
)

// Message attributes used for trace propagation between services.
const (
	AttrTraceID = ctxid.Header // trace_id of the publishing request
	AttrSpanID  = "X-Span-ID"  // span_id of the publishing Op; parent_span_id on the consumer
)

// Log field keys.
const (
	KeySystem          = "messaging_system" // driver name, e.g. "gcp_pubsub"
	KeyTopic           = "topic"
	KeySubscription    = "subscription"
	KeyMessageID       = "message_id"
	KeyDeliveryAttempt = "delivery_attempt"
	KeyOrderingKey     = "ordering_key"
	KeyPayloadBytes    = "payload_bytes"
	KeyLagMs           = "lag_ms" // publish → handler finished
)

// Message is a broker-neutral message.
type Message struct {
	// ID is assigned by the broker. Read-only; ignored on Publish.
	ID string

	Data       []byte
	Attributes map[string]string

	// OrderingKey groups messages that must be delivered in publish order.
	// Support depends on the driver (Pub/Sub needs EnableMessageOrdering).
	OrderingKey string

	// PublishTime is set by the broker on received messages. Read-only.
	PublishTime time.Time

	// DeliveryAttempt is 1 on first delivery and grows on redelivery; 0 when
	// the broker does not report it (Pub/Sub only reports it with a dead
	// letter policy). Read-only.
	DeliveryAttempt int
}

// Handler processes one message. Returning nil acks it; returning an error
// nacks it so the broker redelivers it, unless the error is wrapped with
// Permanent, in which case the message is acked and dropped.
//
// Handlers are called concurrently and may see the same message more than
// once (at-least-once delivery), so they must be idempotent.
type Handler func(ctx context.Context, msg *Message) error

// Middleware wraps a Handler.
type Middleware func(Handler) Handler

// Chain wraps h with mws; mws[0] is the outermost.
func Chain(h Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// Publisher is the publishing side of Broker; depend on it in services so
// tests can substitute a fake.
type Publisher interface {
	Publish(ctx context.Context, topic string, msg *Message) (id string, err error)
}

// Subscriber is the consuming side of Broker.
type Subscriber interface {
	Subscribe(ctx context.Context, subscription string, h Handler) error
}

// Driver is implemented by broker backends (see sub-packages gcppubsub and
// memory). Application code uses Broker, which adds tracing, logging and
// panic recovery on top of a Driver.
type Driver interface {
	// Publish sends msg and returns the broker-assigned id once the broker
	// has accepted it.
	Publish(ctx context.Context, topic string, msg *Message) (id string, err error)

	// Receive calls h concurrently for messages from subscription, acking
	// when h returns nil and nacking otherwise. It blocks until ctx is done
	// (then returns nil after in-flight calls to h have returned), the driver
	// is closed, or a non-retryable error occurs.
	Receive(ctx context.Context, subscription string, h Handler) error

	// Close stops in-flight Receive calls, flushes pending publishes and
	// releases resources. It is idempotent.
	Close() error
}

// Options configures New. The zero value is usable.
type Options struct {
	// System names the backend in logs ("messaging_system"). Drivers' New
	// helpers fill it in.
	System string

	// HandlerTimeout bounds one handler call; the handler's ctx is cancelled
	// when it expires. 0 = no limit (the driver may still enforce one, e.g.
	// Pub/Sub's MaxExtension).
	HandlerTimeout time.Duration

	// Middleware wraps every handler passed to Subscribe, mws[0] outermost.
	// Middleware runs inside tracing and panic recovery.
	Middleware []Middleware

	// DisableLogging stops Broker's own log lines (publish, consume, op).
	DisableLogging bool

	// DisablePropagation stops writing AttrTraceID/AttrSpanID on publish and
	// reading them on consume.
	DisablePropagation bool
}

// Broker is the instrumented client used by application code. It is safe for
// concurrent use; create one per process and share it.
type Broker struct {
	driver Driver
	opts   Options
}

var (
	_ Publisher  = (*Broker)(nil)
	_ Subscriber = (*Broker)(nil)
)

// New wraps d. The Broker owns d: Close closes it.
func New(d Driver, opts Options) *Broker {
	if d == nil {
		panic("broker: nil Driver")
	}
	return &Broker{driver: d, opts: opts}
}

// Driver returns the underlying driver, e.g. for driver-specific test helpers.
func (b *Broker) Driver() Driver { return b.driver }

// Close closes the driver: active Subscribe calls return and pending
// publishes are flushed.
func (b *Broker) Close() error { return b.driver.Close() }

// Publish sends msg to topic and returns the broker-assigned message id. The
// trace_id and span_id in ctx are added as attributes (msg itself is not
// modified). Publish blocks until the broker accepts the message or ctx is
// done; a message may still be delivered after ctx is cancelled.
func (b *Broker) Publish(ctx context.Context, topic string, msg *Message) (string, error) {
	if topic == "" {
		return "", errors.New("broker: empty topic")
	}
	if msg == nil {
		return "", errors.New("broker: nil message")
	}
	out := Message{Data: msg.Data, Attributes: maps.Clone(msg.Attributes), OrderingKey: msg.OrderingKey}
	if !b.opts.DisablePropagation {
		out.Attributes = inject(ctx, out.Attributes)
	}

	start := time.Now()
	id, err := b.driver.Publish(ctx, topic, &out)
	if !b.opts.DisableLogging {
		b.logPublish(ctx, topic, &out, id, time.Since(start), err)
	}
	if err != nil {
		return "", fmt.Errorf("broker: publish to %q: %w", topic, err)
	}
	return id, nil
}

// Subscribe consumes subscription with h until ctx is done or the broker is
// closed, then returns nil once in-flight handlers have finished. A non-nil
// error means the subscription could not be consumed (missing subscription,
// permission denied, …).
//
// Every message is handled with a context carrying the publisher's trace_id
// (or a new one), span_id/parent_span_id, subscription and message_id, so
// logs inside h are correlated with the publishing request.
func (b *Broker) Subscribe(ctx context.Context, subscription string, h Handler) error {
	if subscription == "" {
		return errors.New("broker: empty subscription")
	}
	if h == nil {
		return errors.New("broker: nil handler")
	}
	l := logger.FromContext(ctx).With(b.systemField(), zap.String(KeySubscription, subscription))
	if !b.opts.DisableLogging {
		l.Info("broker subscriber started")
	}
	err := b.driver.Receive(ctx, subscription, b.consumer(subscription, Chain(h, b.opts.Middleware...)))
	if !b.opts.DisableLogging {
		if err != nil {
			l.Error("broker subscriber failed", zap.Error(err))
		} else {
			l.Info("broker subscriber stopped")
		}
	}
	if err != nil {
		return fmt.Errorf("broker: subscribe %q: %w", subscription, err)
	}
	return nil
}

// consumer turns h into the driver-level handler: trace context, Op, timeout,
// panic recovery, one summary log line, and Permanent → ack.
func (b *Broker) consumer(subscription string, h Handler) Handler {
	return func(ctx context.Context, msg *Message) error {
		ctx = b.consumeContext(ctx, subscription, msg)
		var op *logger.Op
		if !b.opts.DisableLogging {
			ctx, op = logger.StartOp(ctx, logger.LayerWorker, "broker.consume")
		}
		hctx := ctx
		if b.opts.HandlerTimeout > 0 {
			var cancel context.CancelFunc
			hctx, cancel = context.WithTimeout(ctx, b.opts.HandlerTimeout)
			defer cancel()
		}

		start := time.Now()
		err := safeCall(hctx, h, msg)
		if op != nil {
			op.End(err)
			b.logConsume(ctx, msg, time.Since(start), err)
		}
		if IsPermanent(err) {
			return nil // ack: redelivery cannot help
		}
		return err
	}
}

func (b *Broker) consumeContext(ctx context.Context, subscription string, msg *Message) context.Context {
	fields := make([]zap.Field, 0, 5)
	if !b.opts.DisablePropagation {
		if id := msg.Attributes[AttrTraceID]; id != "" {
			ctx = ctxid.WithContext(ctx, id)
		}
		if span := msg.Attributes[AttrSpanID]; span != "" {
			fields = append(fields, zap.String(logger.KeyParentSpanID, span))
		}
	}
	ctx, _ = ctxid.Ensure(ctx)

	if b.opts.System != "" {
		fields = append(fields, b.systemField())
	}
	fields = append(fields, zap.String(KeySubscription, subscription), zap.String(KeyMessageID, msg.ID))
	if msg.DeliveryAttempt > 0 {
		fields = append(fields, zap.Int(KeyDeliveryAttempt, msg.DeliveryAttempt))
	}
	return logger.WithFields(ctx, fields...)
}

func (b *Broker) logPublish(ctx context.Context, topic string, msg *Message, id string, d time.Duration, err error) {
	fields := make([]zap.Field, 0, 8)
	if !hasKey(logger.FieldsFromContext(ctx), logger.KeyLayer) {
		fields = append(fields, zap.String(logger.KeyLayer, logger.LayerClient))
	}
	if b.opts.System != "" {
		fields = append(fields, b.systemField())
	}
	fields = append(fields,
		zap.String(KeyTopic, topic),
		zap.Int(KeyPayloadBytes, len(msg.Data)),
		zap.Float64(logger.KeyDurationMs, ms(d)),
	)
	if msg.OrderingKey != "" {
		fields = append(fields, zap.String(KeyOrderingKey, msg.OrderingKey))
	}

	l := logger.FromContext(ctx).WithOptions(zap.WithCaller(false))
	lvl, text := zapcore.InfoLevel, "broker publish"
	if err != nil {
		fields = append(fields, zap.Error(err))
		lvl, text = zapcore.ErrorLevel, "broker publish failed"
		if !logger.MarkErrorLogged(ctx, err) {
			lvl = zapcore.DebugLevel
		}
	} else {
		fields = append(fields, zap.String(KeyMessageID, id))
	}
	if ce := l.Check(lvl, text); ce != nil {
		ce.Write(fields...)
	}
}

// logConsume writes the outcome line. The error itself is reported at error
// level once by the broker.consume Op, so a nack is a warning here; a dropped
// message is always an error because it is data loss.
func (b *Broker) logConsume(ctx context.Context, msg *Message, d time.Duration, err error) {
	fields := []zap.Field{
		zap.Int(KeyPayloadBytes, len(msg.Data)),
		zap.Float64(logger.KeyDurationMs, ms(d)),
	}
	if !msg.PublishTime.IsZero() {
		fields = append(fields, zap.Float64(KeyLagMs, ms(time.Since(msg.PublishTime))))
	}
	lvl, text := zapcore.InfoLevel, "broker message handled"
	if err != nil {
		fields = append(fields, zap.Error(err))
		var pe *PanicError
		if errors.As(err, &pe) {
			fields = append(fields, zap.ByteString("stack", pe.Stack))
		}
		if IsPermanent(err) {
			lvl, text = zapcore.ErrorLevel, "broker message dropped"
		} else {
			lvl, text = zapcore.WarnLevel, "broker message nacked"
		}
	}
	l := logger.FromContext(ctx).WithOptions(zap.WithCaller(false))
	if ce := l.Check(lvl, text); ce != nil {
		ce.Write(fields...)
	}
}

func (b *Broker) systemField() zap.Field { return zap.String(KeySystem, b.opts.System) }

// inject adds the trace attributes from ctx without overriding existing ones.
func inject(ctx context.Context, attrs map[string]string) map[string]string {
	set := func(k, v string) {
		if v == "" || attrs[k] != "" {
			return
		}
		if attrs == nil {
			attrs = make(map[string]string, 2)
		}
		attrs[k] = v
	}
	if id, ok := ctxid.FromContext(ctx); ok {
		set(AttrTraceID, id)
	}
	set(AttrSpanID, logger.SpanIDFromContext(ctx))
	return attrs
}

func hasKey(fields []zap.Field, key string) bool {
	for _, f := range fields {
		if f.Key == key {
			return true
		}
	}
	return false
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
