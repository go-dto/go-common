package logger

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Output target names.
const (
	OutputStdout = "stdout"
	OutputStderr = "stderr"
	OutputFile   = "file"
)

// Encoding format names.
const (
	FormatJSON    = "json"
	FormatConsole = "console"
)

// Config describes how the logger is built. The zero value is not valid on its
// own; start from DefaultConfig() or ConfigFromEnv().
type Config struct {
	// Static fields attached to every log line. Empty values are omitted.
	ServiceName string
	Environment string
	Version     string

	// Level is the minimum enabled level: debug, info, warn, error, dpanic, panic, fatal.
	// It can be changed at runtime with SetLevel or LevelHandler.
	Level string

	// Format is "json" (default, for production) or "console" (human readable, for local dev).
	Format string

	// Outputs lists where logs are written: any of "stdout", "stderr", "file".
	// When "file" is present, File.Path must be set.
	Outputs []string

	// File configures the rotating file writer used when Outputs contains "file".
	File FileConfig

	// EnableCaller adds "caller" (file:line) to each entry.
	EnableCaller bool

	// StacktraceLevel is the minimum level that records a stacktrace.
	// Default "panic" keeps error logs compact; set "error" while debugging.
	StacktraceLevel string

	// Sampling, when non-nil, enables zap sampling. Leave nil for audit-grade logs
	// so no entry is silently dropped under load.
	Sampling *SamplingConfig

	// EnableFunction adds "func" (fully qualified function name) next to caller.
	// Requires EnableCaller.
	EnableFunction bool

	// SlowOpThreshold makes Op.End / the HTTP transport / the Mongo monitor log
	// at warn level when an operation takes at least this long. 0 disables it.
	SlowOpThreshold time.Duration

	// RedirectStdLog routes the standard library "log" package into this logger
	// (at info level) so third-party and legacy log.Printf calls stay structured.
	RedirectStdLog bool
}

// FileConfig controls the built-in rotating file writer.
type FileConfig struct {
	Path        string        // e.g. "log/webhook.log"
	MaxSizeMB   int           // rotate when the file exceeds this size; 0 = 100MB
	MaxBackups  int           // rotated files to keep; 0 = keep all (bounded by MaxAge)
	MaxAge      time.Duration // delete rotated files older than this; 0 = no age limit
	RotateDaily bool          // additionally rotate when the local date changes
}

// SamplingConfig mirrors zap's sampler: per second, log the first Initial
// entries with the same level+message, then every Thereafter-th one.
type SamplingConfig struct {
	Initial    int
	Thereafter int
}

// DefaultConfig returns production defaults: JSON to stdout, info level, caller on,
// stacktrace only for panic/fatal, no sampling, stdlib log redirected.
func DefaultConfig() Config {
	return Config{
		Level:           "info",
		Format:          FormatJSON,
		Outputs:         []string{OutputStdout},
		EnableCaller:    true,
		StacktraceLevel: "panic",
		SlowOpThreshold: time.Second,
		RedirectStdLog:  true,
		File: FileConfig{
			MaxSizeMB:  100,
			MaxBackups: 10,
			MaxAge:     7 * 24 * time.Hour,
		},
	}
}

// ConfigFromEnv starts from DefaultConfig and overrides values from environment
// variables. Unknown or malformed values return an error instead of being ignored.
//
//	SERVICE_NAME, APP_ENV, APP_VERSION
//	LOG_LEVEL               debug|info|warn|error|dpanic|panic|fatal
//	LOG_FORMAT              json|console
//	LOG_OUTPUT              comma separated: stdout,stderr,file
//	LOG_FILE_PATH           path of the active log file
//	LOG_FILE_MAX_SIZE_MB    int
//	LOG_FILE_MAX_BACKUPS    int
//	LOG_FILE_MAX_AGE_DAYS   int
//	LOG_FILE_ROTATE_DAILY   bool
//	LOG_CALLER              bool
//	LOG_STACKTRACE_LEVEL    level name
//	LOG_SAMPLING            "initial,thereafter" e.g. "100,100"; empty = disabled
//	LOG_REDIRECT_STDLOG     bool
//	LOG_FUNCTION            bool
//	LOG_SLOW_OP_MS          int, 0 = disabled
func ConfigFromEnv() (Config, error) {
	return ConfigFromLookup(os.LookupEnv)
}

// ConfigFromLookup is ConfigFromEnv with a custom source, e.g. a function that
// checks os env first and then a .env file loaded by viper.
func ConfigFromLookup(lookup func(string) (string, bool)) (Config, error) {
	cfg := DefaultConfig()
	get := func(k string) (string, bool) {
		v, ok := lookup(k)
		v = strings.TrimSpace(v)
		return v, ok && v != ""
	}

	if v, ok := get("SERVICE_NAME"); ok {
		cfg.ServiceName = v
	}
	if v, ok := get("APP_ENV"); ok {
		cfg.Environment = v
	}
	if v, ok := get("APP_VERSION"); ok {
		cfg.Version = v
	}
	if v, ok := get("LOG_LEVEL"); ok {
		cfg.Level = strings.ToLower(v)
	}
	if v, ok := get("LOG_FORMAT"); ok {
		cfg.Format = strings.ToLower(v)
	}
	if v, ok := get("LOG_OUTPUT"); ok {
		cfg.Outputs = nil
		for _, o := range strings.Split(v, ",") {
			if o = strings.ToLower(strings.TrimSpace(o)); o != "" {
				cfg.Outputs = append(cfg.Outputs, o)
			}
		}
	}
	if v, ok := get("LOG_FILE_PATH"); ok {
		cfg.File.Path = v
	}
	if v, ok := get("LOG_STACKTRACE_LEVEL"); ok {
		cfg.StacktraceLevel = strings.ToLower(v)
	}

	ints := []struct {
		key string
		dst *int
	}{
		{"LOG_FILE_MAX_SIZE_MB", &cfg.File.MaxSizeMB},
		{"LOG_FILE_MAX_BACKUPS", &cfg.File.MaxBackups},
	}
	for _, it := range ints {
		if v, ok := get(it.key); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return cfg, fmt.Errorf("logger: invalid %s=%q", it.key, v)
			}
			*it.dst = n
		}
	}
	if v, ok := get("LOG_FILE_MAX_AGE_DAYS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return cfg, fmt.Errorf("logger: invalid LOG_FILE_MAX_AGE_DAYS=%q", v)
		}
		cfg.File.MaxAge = time.Duration(n) * 24 * time.Hour
	}

	if v, ok := get("LOG_SLOW_OP_MS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return cfg, fmt.Errorf("logger: invalid LOG_SLOW_OP_MS=%q", v)
		}
		cfg.SlowOpThreshold = time.Duration(n) * time.Millisecond
	}

	bools := []struct {
		key string
		dst *bool
	}{
		{"LOG_FILE_ROTATE_DAILY", &cfg.File.RotateDaily},
		{"LOG_CALLER", &cfg.EnableCaller},
		{"LOG_REDIRECT_STDLOG", &cfg.RedirectStdLog},
		{"LOG_FUNCTION", &cfg.EnableFunction},
	}
	for _, it := range bools {
		if v, ok := get(it.key); ok {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return cfg, fmt.Errorf("logger: invalid %s=%q", it.key, v)
			}
			*it.dst = b
		}
	}

	if v, ok := get("LOG_SAMPLING"); ok {
		parts := strings.Split(v, ",")
		if len(parts) != 2 {
			return cfg, fmt.Errorf("logger: invalid LOG_SAMPLING=%q, want \"initial,thereafter\"", v)
		}
		ini, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
		thr, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err1 != nil || err2 != nil || ini <= 0 || thr <= 0 {
			return cfg, fmt.Errorf("logger: invalid LOG_SAMPLING=%q", v)
		}
		cfg.Sampling = &SamplingConfig{Initial: ini, Thereafter: thr}
	}

	return cfg, cfg.Validate()
}

// Validate reports configuration errors early, before any file is opened.
func (c Config) Validate() error {
	if _, err := parseLevel(c.Level); err != nil {
		return err
	}
	if c.StacktraceLevel != "" {
		if _, err := parseLevel(c.StacktraceLevel); err != nil {
			return err
		}
	}
	switch c.Format {
	case "", FormatJSON, FormatConsole:
	default:
		return fmt.Errorf("logger: unknown format %q (want json|console)", c.Format)
	}
	if len(c.Outputs) == 0 {
		return fmt.Errorf("logger: at least one output is required")
	}
	if c.EnableFunction && !c.EnableCaller {
		return fmt.Errorf("logger: EnableFunction requires EnableCaller")
	}
	if c.SlowOpThreshold < 0 {
		return fmt.Errorf("logger: SlowOpThreshold must be >= 0")
	}
	if c.File.MaxSizeMB < 0 {
		return fmt.Errorf("logger: File.MaxSizeMB must be >= 0")
	}
	if c.File.MaxBackups < 0 {
		return fmt.Errorf("logger: File.MaxBackups must be >= 0")
	}
	if c.File.MaxAge < 0 {
		return fmt.Errorf("logger: File.MaxAge must be >= 0")
	}
	if c.Sampling != nil && (c.Sampling.Initial <= 0 || c.Sampling.Thereafter <= 0) {
		return fmt.Errorf("logger: Sampling.Initial and Sampling.Thereafter must be > 0")
	}
	for _, o := range c.Outputs {
		switch o {
		case OutputStdout, OutputStderr:
		case OutputFile:
			if strings.TrimSpace(c.File.Path) == "" {
				return fmt.Errorf("logger: output \"file\" requires File.Path (LOG_FILE_PATH)")
			}
		default:
			return fmt.Errorf("logger: unknown output %q (want stdout|stderr|file)", o)
		}
	}
	return nil
}

// ValidateProduction applies stricter invariants needed by centralized log
// ingestion. It is intentionally separate from Validate so local tools and
// tests can still build a logger without service metadata.
func (c Config) ValidateProduction() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.ServiceName) == "" {
		return fmt.Errorf("logger: ServiceName is required in production")
	}
	if strings.TrimSpace(c.Environment) == "" {
		return fmt.Errorf("logger: Environment is required in production")
	}
	if c.Format != "" && c.Format != FormatJSON {
		return fmt.Errorf("logger: production format must be json")
	}
	for _, output := range c.Outputs {
		if output == OutputFile && c.File.MaxBackups == 0 && c.File.MaxAge == 0 {
			return fmt.Errorf("logger: production file output requires bounded retention")
		}
	}
	return nil
}
