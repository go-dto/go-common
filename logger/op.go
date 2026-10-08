package logger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Op traces one unit of work in one layer (a handler, a service method, a
// repository query, a call to a third party). It carries layer/op fields in the
// context so every log below it is tagged, and logs one summary line with
// duration_ms when it ends.
//
//	func (r *repo) GetByReference(ctx context.Context, ref string) (_ *Webhook, err error) {
//		ctx, op := logger.StartOp(ctx, logger.LayerRepository, "webhook_config.GetByReference",
//			zap.String(logger.KeyDBSystem, "mysql"))
//		defer op.Done(&err)
//		...
//	}
//
// Every Op gets a unique span_id (16 hex chars). "op started", "op finished"
// and every log written with the Op's ctx (including http outbound and mongo
// command lines) carry the same span_id, so one search on span_id returns the
// whole operation. Nested Ops also carry parent_span_id, which links a
// repository call to the service call that made it.
//
// End-of-op levels:
//   - success: debug ("op finished"), or warn ("op slow") when the duration is at
//     least Config.SlowOpThreshold;
//   - failure: error ("op failed") the first time an error is seen in the
//     request; when an outer layer ends with the same (or a wrapping) error it
//     is logged at debug with error_logged=true, so each error is reported at
//     error level exactly once, by the layer closest to where it happened.
type Op struct {
	spanID string
	ctx    context.Context
	log    *zap.Logger
	start  time.Time
	ended  atomic.Bool
}

// StartOp starts an operation. The returned ctx must be passed to everything
// the operation calls, so their logs inherit layer, op, trace_id and fields.
// Fields given here are attached to ctx (like WithFields).
func StartOp(ctx context.Context, layer, name string, fields ...zap.Field) (context.Context, *Op) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errStateFrom(ctx) == nil {
		ctx = context.WithValue(ctx, errStateKey{}, &errState{})
	}
	spanID := newSpanID()
	all := make([]zap.Field, 0, len(fields)+4)
	all = append(all, zap.String(KeySpanID, spanID))
	if parent := SpanIDFromContext(ctx); parent != "" {
		all = append(all, zap.String(KeyParentSpanID, parent))
	}
	all = append(all, zap.String(KeyLayer, layer), zap.String(KeyOperation, name))
	all = append(all, fields...)
	ctx = WithFields(ctx, all...)
	ctx = context.WithValue(ctx, spanKey{}, spanID)

	base := FromContext(ctx)
	// Caller skips: StartOp -> user (1); write -> end -> End/Done -> user (3).
	op := &Op{spanID: spanID, ctx: ctx, log: base.WithOptions(zap.AddCallerSkip(3)), start: time.Now()}
	if ce := base.WithOptions(zap.AddCallerSkip(1)).Check(zapcore.DebugLevel, "op started"); ce != nil {
		ce.Write()
	}
	return ctx, op
}

type spanKey struct{}

// SpanIDFromContext returns the span_id of the innermost Op in ctx ("" if none).
func SpanIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(spanKey{}).(string)
	return id
}

func newSpanID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// SpanID returns the operation's span_id.
func (o *Op) SpanID() string { return o.spanID }

// Ctx returns the operation's context.
func (o *Op) Ctx() context.Context { return o.ctx }

// L returns a logger carrying the operation's fields, for logs in the middle
// of the operation.
func (o *Op) L() *zap.Logger { return FromContext(o.ctx) }

// Elapsed returns the time since StartOp.
func (o *Op) Elapsed() time.Duration { return time.Since(o.start) }

// Done is End for use with a named error return: defer op.Done(&err).
func (o *Op) Done(errp *error, fields ...zap.Field) {
	var err error
	if errp != nil {
		err = *errp
	}
	o.end(err, fields)
}

// End logs the summary line. Only the first call has an effect.
func (o *Op) End(err error, fields ...zap.Field) { o.end(err, fields) }

func (o *Op) end(err error, fields []zap.Field) {
	if !o.ended.CompareAndSwap(false, true) {
		return
	}
	d := time.Since(o.start)
	fields = append(fields, zap.Float64(KeyDurationMs, ms(d)))

	if err != nil {
		fields = append(fields, zap.Error(err))
		if MarkErrorLogged(o.ctx, err) {
			o.write(zapcore.ErrorLevel, "op failed", fields)
		} else {
			o.write(zapcore.DebugLevel, "op failed", append(fields, zap.Bool("error_logged", true)))
		}
		return
	}
	if slow := slowOpThreshold(); slow > 0 && d >= slow {
		o.write(zapcore.WarnLevel, "op slow", fields)
		return
	}
	o.write(zapcore.DebugLevel, "op finished", fields)
}

func (o *Op) write(lvl zapcore.Level, msg string, fields []zap.Field) {
	if ce := o.log.Check(lvl, msg); ce != nil {
		ce.Write(fields...)
	}
}

// ---- error de-duplication --------------------------------------------------

type errStateKey struct{}

type errState struct {
	mu   sync.Mutex
	errs []error
}

func errStateFrom(ctx context.Context) *errState {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(errStateKey{}).(*errState)
	return s
}

// maxTrackedErrors bounds memory for long-lived contexts (e.g. a worker loop).
const maxTrackedErrors = 32

// MarkErrorLogged records that err has been logged at error level within this
// request and reports whether it is the first time. It returns true (log it)
// when ctx was not created by StartOp. Use it when you log an error yourself
// and want outer Ops not to repeat it.
func MarkErrorLogged(ctx context.Context, err error) bool {
	s := errStateFrom(ctx)
	if s == nil || err == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seen := range s.errs {
		if errors.Is(err, seen) {
			return false
		}
	}
	if len(s.errs) < maxTrackedErrors {
		s.errs = append(s.errs, err)
	}
	return true
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
