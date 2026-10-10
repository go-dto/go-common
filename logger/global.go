package logger

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type globalState struct {
	inst           *Instance
	sugar          *zap.SugaredLogger
	restore        func() // undo zap.ReplaceGlobals / RedirectStdLog
	redirectStdLog bool
	hooks          []func(zapcore.Entry) error
}

// closeGracePeriod is how long a replaced logger stays open after re-Init.
var closeGracePeriod = 5 * time.Second

var (
	global atomic.Pointer[globalState]
	initMu sync.Mutex // serialises Init/AddHook so they never interleave
)

func init() {
	// A safe default so logging works even before Init is called (e.g. in tests
	// or package init code). JSON to stdout, info level.
	inst, err := New(DefaultConfig())
	if err != nil {
		panic(err) // unreachable: DefaultConfig is always valid
	}
	global.Store(&globalState{inst: inst, sugar: inst.Sugar(), restore: func() {}})
}

// Init builds a logger from cfg and installs it as the process-wide logger.
// It is safe to call more than once (e.g. after reloading config); the previous
// logger is flushed and its files closed after a short grace period.
// Do not keep *zap.Logger values in long-lived structs; fetch them with L() or
// FromContext(ctx) when logging so they always follow the current logger. It also replaces zap's own globals
// (zap.L / zap.S) and, if cfg.RedirectStdLog is set, the standard "log" package.
func Init(cfg Config) error {
	inst, err := New(cfg)
	if err != nil {
		return err
	}

	initMu.Lock()
	defer initMu.Unlock()

	// Preserve process-wide hooks across configuration reloads.
	old := global.Load()
	hooks := append([]func(zapcore.Entry) error(nil), old.hooks...)
	if len(hooks) > 0 {
		inst.Logger = inst.Logger.WithOptions(zap.Hooks(hooks...))
	}

	// Undo the previous logger's hooks into zap/std log first, so the restore
	// functions unwind in the right order and nothing points at a closed file.
	old.restore()

	global.Store(&globalState{
		inst:           inst,
		sugar:          inst.Sugar(),
		restore:        installProcessGlobals(inst.Logger, cfg.RedirectStdLog),
		redirectStdLog: cfg.RedirectStdLog,
		hooks:          hooks,
	})
	// Loggers obtained from the old instance just before the swap may still be
	// writing; give them a grace period before closing its files.
	oldInst := old.inst
	time.AfterFunc(closeGracePeriod, func() { _ = oldInst.Close() })
	return nil
}

// MustInit is Init that panics on error; convenient in main().
func MustInit(cfg Config) {
	if err := Init(cfg); err != nil {
		panic(err)
	}
}

// L returns the global structured logger. Never nil.
func L() *zap.Logger { return global.Load().inst.Logger }

// S returns the global sugared logger (printf-style). Never nil.
func S() *zap.SugaredLogger { return global.Load().sugar }

// Named returns a child logger with a "logger" name, e.g. Named("retry-worker").
func Named(name string) *zap.Logger { return L().Named(name) }

// Sync flushes the global logger. Call it with defer in main().
func Sync() error { return global.Load().inst.Sync() }

// Close flushes the global logger and closes its log files. Call once at shutdown.
func Close() error { return global.Load().inst.Close() }

// SetLevel changes the minimum level at runtime without rebuilding the logger.
func SetLevel(level string) error {
	l, err := parseLevel(level)
	if err != nil {
		return err
	}
	global.Load().inst.Level.SetLevel(l)
	return nil
}

// GetLevel returns the current minimum level.
func GetLevel() zapcore.Level { return global.Load().inst.Level.Level() }

// slowOpThreshold returns the current Config.SlowOpThreshold (0 = disabled).
func slowOpThreshold() time.Duration { return global.Load().inst.slowOp }

// Enabled reports whether entries at lvl would be written; use it to skip
// building expensive fields.
func Enabled(lvl zapcore.Level) bool { return L().Core().Enabled(lvl) }

// LevelHandler returns an http.Handler for inspecting (GET) and changing (PUT
// {"level":"debug"}) the level at runtime. Mount it on an internal/admin route
// only — never expose it publicly.
func LevelHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Resolve on every request so it follows re-Init.
		global.Load().inst.Level.ServeHTTP(w, r)
	})
}

// AddHook registers callbacks invoked for every written entry (e.g. metrics
// counters per level). Hooks must be fast and must not log.
func AddHook(hooks ...func(zapcore.Entry) error) {
	if len(hooks) == 0 {
		return
	}
	hooks = protectHooks(hooks)
	if len(hooks) == 0 {
		return
	}
	initMu.Lock()
	defer initMu.Unlock()
	cur := global.Load()
	inst := *cur.inst
	inst.Logger = inst.Logger.WithOptions(zap.Hooks(hooks...))
	allHooks := append([]func(zapcore.Entry) error(nil), cur.hooks...)
	allHooks = append(allHooks, hooks...)

	// Keep logger.L/S, zap.L/S, and the redirected stdlib logger on the same
	// instance. Otherwise hooks would silently miss entries written via zap.L.
	cur.restore()
	global.Store(&globalState{
		inst:           &inst,
		sugar:          inst.Sugar(),
		restore:        installProcessGlobals(inst.Logger, cur.redirectStdLog),
		redirectStdLog: cur.redirectStdLog,
		hooks:          allHooks,
	})
}

var errHookPanicked = errors.New("logger: hook panicked")

func protectHooks(hooks []func(zapcore.Entry) error) []func(zapcore.Entry) error {
	protected := make([]func(zapcore.Entry) error, 0, len(hooks))
	for _, hook := range hooks {
		if hook == nil {
			continue
		}
		h := hook
		protected = append(protected, func(entry zapcore.Entry) (err error) {
			defer func() {
				if recover() != nil {
					err = errHookPanicked
				}
			}()
			return h(entry)
		})
	}
	return protected
}

func installProcessGlobals(l *zap.Logger, redirectStdLog bool) func() {
	undoGlobals := zap.ReplaceGlobals(l)
	undoStd := func() {}
	if redirectStdLog {
		undoStd = zap.RedirectStdLog(l.Named("stdlog"))
	}
	return func() {
		undoStd()
		undoGlobals()
	}
}

// ReplaceForTest installs inst as the global logger and returns a function that
// restores the previous one. Intended for tests (see zaptest/observer).
func ReplaceForTest(l *zap.Logger) (restore func()) {
	initMu.Lock()
	defer initMu.Unlock()
	inst := &Instance{Logger: l, Level: zap.NewAtomicLevelAt(zapcore.DebugLevel), closer: func() error { return nil }}
	old := global.Swap(&globalState{inst: inst, sugar: l.Sugar(), restore: func() {}})
	return func() {
		initMu.Lock()
		defer initMu.Unlock()
		global.Store(old)
	}
}
