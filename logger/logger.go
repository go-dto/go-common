package logger

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Standard field keys. Use these instead of ad-hoc strings so every service
// emits the same names and log queries stay simple.
const (
	KeyTraceID    = "trace_id"   // transport-level correlation id (X-Request-ID)
	KeyRequestID  = "request_id" // business request id (e.g. webhook request_id)
	KeyService    = "service"
	KeyEnv        = "env"
	KeyVersion    = "version"
	KeyError      = "error"
	KeyDurationMs = "duration_ms"
	KeyMethod     = "method"
	KeyPath       = "path"
	KeyRoute      = "route"
	KeyStatusCode = "status_code"
	KeyRespBytes  = "response_bytes"

	// Cross-layer tracing keys.
	KeyLayer         = "layer"           // handler | service | repository | client | worker
	KeyOperation     = "op"              // logical operation, e.g. "webhook_config.GetByReference"
	KeySpanID        = "span_id"         // unique per Op; pairs "op started" with "op finished"
	KeyParentSpanID  = "parent_span_id"  // span_id of the calling Op
	KeyOriginTraceID = "origin_trace_id" // trace_id of the request that created a background job
	KeyDBSystem      = "db_system"       // mysql | mongodb
	KeyPeer          = "peer"            // logical name of a third party, e.g. "client_webhook"

	redactedMarker = "[REDACTED]"
)

// Standard layer names for KeyLayer / StartOp.
const (
	LayerHandler    = "handler"
	LayerService    = "service"
	LayerRepository = "repository"
	LayerClient     = "client"
	LayerWorker     = "worker"
)

// Instance is a fully built logger with its runtime level and resources.
type Instance struct {
	Logger *zap.Logger
	Level  zap.AtomicLevel
	closer func() error
	slowOp time.Duration // Config.SlowOpThreshold; 0 = disabled
}

// Sugar returns the sugared variant of the logger.
func (i *Instance) Sugar() *zap.SugaredLogger { return i.Logger.Sugar() }

// Sync flushes buffered entries. Harmless "invalid argument"/"inappropriate ioctl"
// errors from syncing a terminal or pipe are ignored.
func (i *Instance) Sync() error { return ignoreBenignSyncErr(i.Logger.Sync()) }

// Close flushes and releases file handles. The instance must not be used afterwards.
func (i *Instance) Close() error {
	err := i.Sync()
	if i.closer != nil {
		err = multierr.Append(err, i.closer())
	}
	return err
}

// New builds a logger from cfg without touching the global logger.
func New(cfg Config) (*Instance, error) {
	if cfg.Format == "" {
		cfg.Format = FormatJSON
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	lvl, _ := parseLevel(cfg.Level)
	atomicLevel := zap.NewAtomicLevelAt(lvl)

	var (
		sinks   []zapcore.WriteSyncer
		closers []func() error
	)
	for _, o := range dedupe(cfg.Outputs) {
		switch o {
		case OutputStdout:
			sinks = append(sinks, zapcore.Lock(os.Stdout))
		case OutputStderr:
			sinks = append(sinks, zapcore.Lock(os.Stderr))
		case OutputFile:
			rf, err := NewRotatingFile(cfg.File)
			if err != nil {
				for _, c := range closers {
					_ = c()
				}
				return nil, err
			}
			sinks = append(sinks, rf)
			closers = append(closers, rf.Close)
		}
	}

	core := zapcore.NewCore(newEncoder(cfg.Format, cfg.EnableFunction), zapcore.NewMultiWriteSyncer(sinks...), atomicLevel)
	if cfg.Sampling != nil {
		core = zapcore.NewSamplerWithOptions(core, time.Second, cfg.Sampling.Initial, cfg.Sampling.Thereafter)
	}

	opts := []zap.Option{zap.ErrorOutput(zapcore.Lock(os.Stderr))}
	if cfg.EnableCaller {
		opts = append(opts, zap.AddCaller())
	}
	if cfg.StacktraceLevel != "" {
		st, _ := parseLevel(cfg.StacktraceLevel)
		opts = append(opts, zap.AddStacktrace(st))
	}

	var static []zap.Field
	if cfg.ServiceName != "" {
		static = append(static, zap.String(KeyService, cfg.ServiceName))
	}
	if cfg.Environment != "" {
		static = append(static, zap.String(KeyEnv, cfg.Environment))
	}
	if cfg.Version != "" {
		static = append(static, zap.String(KeyVersion, cfg.Version))
	}
	if len(static) > 0 {
		opts = append(opts, zap.Fields(static...))
	}

	return &Instance{
		Logger: zap.New(core, opts...),
		Level:  atomicLevel,
		slowOp: cfg.SlowOpThreshold,
		closer: func() error {
			var err error
			for _, c := range closers {
				err = multierr.Append(err, c())
			}
			return err
		},
	}, nil
}

func newEncoder(format string, withFunction bool) zapcore.Encoder {
	ec := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.RFC3339NanoTimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}
	if withFunction {
		ec.FunctionKey = "func"
	}
	if format == FormatConsole {
		ec.EncodeLevel = zapcore.CapitalColorLevelEncoder
		ec.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000")
		ec.EncodeDuration = zapcore.StringDurationEncoder
		return zapcore.NewConsoleEncoder(ec)
	}
	return zapcore.NewJSONEncoder(ec)
}

func parseLevel(s string) (zapcore.Level, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return zapcore.InfoLevel, nil
	}
	if s == "warning" {
		s = "warn"
	}
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return l, fmt.Errorf("logger: unknown level %q", s)
	}
	return l, nil
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// ignoreBenignSyncErr drops errors returned when fsync is called on stdout/stderr
// attached to a terminal or pipe; they are not real failures.
func ignoreBenignSyncErr(err error) error {
	if err == nil {
		return nil
	}
	var kept error
	for _, e := range multierr.Errors(err) {
		if errors.Is(e, syscall.EINVAL) || errors.Is(e, syscall.ENOTTY) || errors.Is(e, syscall.EBADF) {
			continue
		}
		kept = multierr.Append(kept, e)
	}
	return kept
}
