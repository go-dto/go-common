// Package singleton is a backward-compatible facade over github.com/go-dto/go-common/logger.
//
// Deprecated: new code should import "github.com/go-dto/go-common/logger" directly
// (logger.FromContext(ctx), logger.L(), logger.S()). This package only exists so
// existing call sites keep compiling; every function delegates to the new
// thread-safe global logger.
package singleton

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-dto/go-common/logger"
)

const (
	LLvlDevelopment = iota + 1
	LLvlProduction
	LStdDir  = "log"
	LStdFile = "2006-01-02"
)

// Options is kept for compatibility with InitLoggerWithDir.
type Options struct {
	Level      int
	OutputFile []string // ignored; use logger.Config.Outputs
}

// skip1 returns the global logger adjusted so "caller" points at the code that
// called this package, not at this wrapper.
func skip1() *zap.Logger { return logger.L().WithOptions(zap.AddCallerSkip(1)) }

func skip1Sugar() *zap.SugaredLogger { return skip1().Sugar() }

var errNil = errors.New("<nil error>")

func errOrNil(err error) error {
	if err == nil {
		return errNil
	}
	return err
}

func withErr(fields []zap.Field, err error) []zap.Field {
	for _, f := range fields {
		if f.Key == logger.KeyError {
			return fields
		}
	}
	return append(fields, zap.Error(err))
}

// InitLoggerWithDir keeps the old signature. dir == "" logs to stdout only;
// otherwise logs go to stdout and to a rotating file "<dir>/<prefix>.log".
//
// Deprecated: use logger.Init(logger.ConfigFromEnv()).
func InitLoggerWithDir(dir string, prefix string, opt *Options) {
	cfg := logger.DefaultConfig()
	if opt != nil && opt.Level == LLvlDevelopment {
		cfg.Level = "debug"
		cfg.Format = logger.FormatConsole
	}
	if dir != "" {
		name := prefix
		if name == "" {
			name = "app"
		}
		cfg.Outputs = []string{logger.OutputStdout, logger.OutputFile}
		cfg.File.Path = filepath.Join(dir, name+".log")
		cfg.File.RotateDaily = true
	}
	logger.MustInit(cfg)
}

// GetSugaredLogger returns the global sugared logger.
func GetSugaredLogger() *zap.SugaredLogger { return logger.S() }

func Debug(message string, fields ...zap.Field) { skip1().Debug(message, fields...) }
func Info(message string, fields ...zap.Field)  { skip1().Info(message, fields...) }
func Warn(message string, fields ...zap.Field)  { skip1().Warn(message, fields...) }

// Error logs err.Error() as the message. A nil err no longer panics.
func Error(err error, fields ...zap.Field) {
	err = errOrNil(err)
	skip1().Error(err.Error(), withErr(fields, err)...)
}

// Fatal logs and exits with status 1. A nil err no longer panics.
func Fatal(err error, fields ...zap.Field) {
	err = errOrNil(err)
	skip1().Fatal(err.Error(), withErr(fields, err)...)
}

// Panic logs at panic level (with stacktrace) and panics.
func Panic(message string, fields ...zap.Field) { skip1().Panic(message, fields...) }

func Debugf(template string, args ...interface{}) { skip1Sugar().Debugf(template, args...) }
func Infof(template string, args ...interface{})  { skip1Sugar().Infof(template, args...) }
func Warnf(template string, args ...interface{})  { skip1Sugar().Warnf(template, args...) }
func Errorf(template string, args ...interface{}) { skip1Sugar().Errorf(template, args...) }
func Fatalf(template string, args ...interface{}) { skip1Sugar().Fatalf(template, args...) }

// AddHook registers entry hooks on the global logger.
func AddHook(hook func(zapcore.Entry) error) { logger.AddHook(hook) }

// WithRequestID returns a logger tagged with the transport trace id.
func WithRequestID(reqID string) *zap.Logger {
	return logger.L().With(zap.String(logger.KeyTraceID, reqID))
}

func WithRequest(r *http.Request) *zap.Logger {
	l := logger.L().With(
		zap.String("method", r.Method),
		zap.String("host", r.Host),
	)
	if r.Pattern != "" {
		l = l.With(zap.String(logger.KeyRoute, r.Pattern))
	}
	return l
}

func SugaredWithRequest(r *http.Request) *zap.SugaredLogger { return WithRequest(r).Sugar() }

// WithContext returns the logger with trace_id and context fields.
func WithContext(ctx context.Context) *zap.Logger { return logger.FromContext(ctx) }

func SugaredWithContext(ctx context.Context) *zap.SugaredLogger {
	return logger.SugarFromContext(ctx)
}
