package logger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const backupTimeFormat = "2006-01-02T15-04-05.000"

// RotatingFile is a zapcore.WriteSyncer that writes to a file and rotates it by
// size and (optionally) by day, keeping a bounded number/age of backups.
// Backups are named "<name>-<timestamp><ext>", e.g. webhook-2026-10-06T13-00-00.000.log.
//
// When running in containers prefer stdout and let the platform ship logs;
// use this only on VMs/bare metal where files are collected by an agent.
type RotatingFile struct {
	mu         sync.Mutex
	path       string
	maxSize    int64
	maxBackups int
	maxAge     time.Duration
	daily      bool

	file     *os.File
	size     int64
	openedOn string // yyyy-mm-dd of the current file, for daily rotation
	now      func() time.Time
}

// NewRotatingFile opens (or creates) cfg.Path, creating parent directories.
func NewRotatingFile(cfg FileConfig) (*RotatingFile, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, errors.New("logger: file path is required")
	}
	maxMB := cfg.MaxSizeMB
	if maxMB <= 0 {
		maxMB = 100
	}
	rf := &RotatingFile{
		path:       cfg.Path,
		maxSize:    int64(maxMB) * 1024 * 1024,
		maxBackups: cfg.MaxBackups,
		maxAge:     cfg.MaxAge,
		daily:      cfg.RotateDaily,
		now:        time.Now,
	}
	if err := rf.openExisting(); err != nil {
		return nil, err
	}
	return rf, nil
}

func (rf *RotatingFile) openExisting() error {
	if err := os.MkdirAll(filepath.Dir(rf.path), 0o755); err != nil {
		return fmt.Errorf("logger: create log dir: %w", err)
	}
	f, err := os.OpenFile(rf.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("logger: open log file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("logger: stat log file: %w", err)
	}
	rf.file = f
	rf.size = info.Size()
	rf.openedOn = info.ModTime().Format("2006-01-02")
	if info.Size() == 0 {
		rf.openedOn = rf.now().Format("2006-01-02")
	}
	return nil
}

// Write implements io.Writer. Each zap entry is written in a single call, so
// entries are never split across files.
func (rf *RotatingFile) Write(p []byte) (int, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.file == nil {
		return 0, errors.New("logger: write to closed file")
	}
	needSize := rf.size > 0 && rf.size+int64(len(p)) > rf.maxSize
	needDay := rf.daily && rf.size > 0 && rf.now().Format("2006-01-02") != rf.openedOn
	if needSize || needDay {
		if err := rf.rotate(); err != nil {
			fmt.Fprintf(os.Stderr, "logger: rotate failed: %v\n", err)
			// rotate attempts to restore the previous active file. If that also
			// failed, return the error instead of dereferencing a nil file handle.
			if rf.file == nil {
				return 0, fmt.Errorf("logger: rotate log file: %w", err)
			}
		}
	}
	n, err := rf.file.Write(p)
	rf.size += int64(n)
	return n, err
}

// Sync implements zapcore.WriteSyncer.
func (rf *RotatingFile) Sync() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.file == nil {
		return nil
	}
	return rf.file.Sync()
}

// Close flushes and closes the file. Further writes return an error.
func (rf *RotatingFile) Close() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.file == nil {
		return nil
	}
	err := errors.Join(rf.file.Sync(), rf.file.Close())
	rf.file = nil
	return err
}

// Rotate forces a rotation (e.g. on SIGHUP).
func (rf *RotatingFile) Rotate() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.rotate()
}

func (rf *RotatingFile) rotate() error {
	if rf.file != nil {
		_ = rf.file.Sync()
		if err := rf.file.Close(); err != nil {
			return err
		}
		rf.file = nil
	}
	ext := filepath.Ext(rf.path)
	base := strings.TrimSuffix(rf.path, ext)
	backup := fmt.Sprintf("%s-%s%s", base, rf.now().Format(backupTimeFormat), ext)
	if err := os.Rename(rf.path, backup); err != nil && !os.IsNotExist(err) {
		reopenErr := rf.openExisting()
		return errors.Join(fmt.Errorf("rename active log: %w", err), reopenErr)
	}
	f, err := os.OpenFile(rf.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		// Restore the backup as the active file so logging can continue after a
		// transient filesystem failure. Write returns an error if restoration or
		// reopening fails, but it must never panic.
		restoreErr := os.Rename(backup, rf.path)
		var reopenErr error
		if restoreErr == nil {
			reopenErr = rf.openExisting()
		}
		return errors.Join(fmt.Errorf("create active log: %w", err), restoreErr, reopenErr)
	}
	rf.file = f
	rf.size = 0
	rf.openedOn = rf.now().Format("2006-01-02")
	rf.cleanup()
	return nil
}

// cleanup removes backups beyond maxBackups or older than maxAge.
func (rf *RotatingFile) cleanup() {
	if rf.maxBackups <= 0 && rf.maxAge <= 0 {
		return
	}
	ext := filepath.Ext(rf.path)
	base := strings.TrimSuffix(rf.path, ext)
	matches, err := filepath.Glob(base + "-*" + ext)
	if err != nil {
		return
	}
	type backup struct {
		path string
		t    time.Time
	}
	var backups []backup
	for _, m := range matches {
		ts := strings.TrimSuffix(strings.TrimPrefix(m, base+"-"), ext)
		t, err := time.ParseInLocation(backupTimeFormat, ts, time.Local)
		if err != nil {
			continue // not one of ours
		}
		backups = append(backups, backup{m, t})
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].t.After(backups[j].t) })

	cutoff := time.Time{}
	if rf.maxAge > 0 {
		cutoff = rf.now().Add(-rf.maxAge)
	}
	for i, b := range backups {
		tooMany := rf.maxBackups > 0 && i >= rf.maxBackups
		tooOld := !cutoff.IsZero() && b.t.Before(cutoff)
		if tooMany || tooOld {
			_ = os.Remove(b.path)
		}
	}
}
