// Package mongolog logs MongoDB commands through github.com/go-dto/go-common/logger, using
// the command's context so each line carries trace_id, request_id, layer and
// op of the repository call that issued it.
//
//	clientOpts := options.Client().ApplyURI(uri).
//		SetMonitor(mongolog.NewMonitor(mongolog.Options{}))
//
// Command bodies and replies are never logged (they may contain payloads or
// secrets); only command name, database, collection, duration and failure.
// "caller" is omitted on these lines because it would point into the driver;
// the op field (from logger.StartOp in the repository) identifies the call site.
package mongolog

import (
	"context"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/event"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger"
)

// Options configures NewMonitor.
type Options struct {
	// SuccessLevel is the level for successful commands. Default debug, so
	// production (info) only shows slow and failed commands.
	SuccessLevel *zapcore.Level

	// SlowThreshold logs successful commands at warn when they take at least
	// this long. Default 500ms; negative disables.
	SlowThreshold time.Duration

	// IgnoreCommands are skipped entirely. Defaults to handshake/auth/heartbeat
	// commands (hello, isMaster, ping, saslStart, …).
	IgnoreCommands []string
}

var defaultIgnore = []string{
	"hello", "isMaster", "ismaster", "ping", "buildInfo", "buildinfo",
	"saslStart", "saslContinue", "getnonce", "authenticate", "endSessions",
}

type started struct {
	collection string
}

type monitor struct {
	successLevel zapcore.Level
	slow         time.Duration
	ignore       map[string]struct{}
	inflight     sync.Map // key(connID, reqID) -> started
}

// NewMonitor returns a CommandMonitor to pass to options.Client().SetMonitor.
func NewMonitor(opts Options) *event.CommandMonitor {
	m := &monitor{successLevel: zapcore.DebugLevel, slow: 500 * time.Millisecond, ignore: map[string]struct{}{}}
	if opts.SuccessLevel != nil {
		m.successLevel = *opts.SuccessLevel
	}
	if opts.SlowThreshold != 0 {
		m.slow = opts.SlowThreshold
	}
	ignore := opts.IgnoreCommands
	if ignore == nil {
		ignore = defaultIgnore
	}
	for _, c := range ignore {
		m.ignore[c] = struct{}{}
	}
	return &event.CommandMonitor{Started: m.started, Succeeded: m.succeeded, Failed: m.failed}
}

func key(connID string, reqID int64) string { return connID + "#" + strconv.FormatInt(reqID, 10) }

func (m *monitor) skip(cmd string) bool { _, ok := m.ignore[cmd]; return ok }

func (m *monitor) started(_ context.Context, e *event.CommandStartedEvent) {
	if m.skip(e.CommandName) {
		return
	}
	var coll string
	if v, err := e.Command.LookupErr(e.CommandName); err == nil {
		coll, _ = v.StringValueOK()
	}
	m.inflight.Store(key(e.ConnectionID, e.RequestID), started{collection: coll})
}

func (m *monitor) fields(e *event.CommandFinishedEvent) []zap.Field {
	var coll string
	if v, ok := m.inflight.LoadAndDelete(key(e.ConnectionID, e.RequestID)); ok {
		coll = v.(started).collection
	}
	f := []zap.Field{
		zap.String(logger.KeyDBSystem, "mongodb"),
		zap.String("db_command", e.CommandName),
		zap.String("db_name", e.DatabaseName),
		zap.Float64(logger.KeyDurationMs, float64(e.Duration)/float64(time.Millisecond)),
	}
	if coll != "" {
		f = append(f, zap.String("db_collection", coll))
	}
	return f
}

func (m *monitor) succeeded(ctx context.Context, e *event.CommandSucceededEvent) {
	if m.skip(e.CommandName) {
		return
	}
	lvl, msg := m.successLevel, "mongo command"
	if m.slow > 0 && e.Duration >= m.slow {
		lvl, msg = zapcore.WarnLevel, "mongo command slow"
	}
	l := logger.FromContext(ctx).WithOptions(zap.WithCaller(false))
	if ce := l.Check(lvl, msg); ce != nil {
		ce.Write(m.fields(&e.CommandFinishedEvent)...)
	} else {
		m.inflight.Delete(key(e.ConnectionID, e.RequestID))
	}
}

// failed logs at warn: the repository Op that receives the error logs it at
// error level, so it is not reported twice.
func (m *monitor) failed(ctx context.Context, e *event.CommandFailedEvent) {
	if m.skip(e.CommandName) {
		return
	}
	f := append(m.fields(&e.CommandFinishedEvent), zap.String(logger.KeyError, e.Failure))
	logger.FromContext(ctx).WithOptions(zap.WithCaller(false)).Warn("mongo command failed", f...)
}
