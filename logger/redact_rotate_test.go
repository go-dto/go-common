package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRedactJSON(t *testing.T) {
	r := NewRedactor("nik")
	in := `{"user":"a","Password":"rahasia","nested":{"api-key":"k","list":[{"token":"t","ok":1}]},"NIK":"123","amount":12345678901234567890}`
	out := r.JSON([]byte(in))
	for _, leak := range []string{"rahasia", `"k"`, `"t"`, "123\""} {
		if strings.Contains(out, leak) {
			t.Errorf("leaked %s in %s", leak, out)
		}
	}
	if !strings.Contains(out, `"user":"a"`) || !strings.Contains(out, "12345678901234567890") {
		t.Errorf("non-sensitive data lost: %s", out)
	}
	if got := r.JSON([]byte(`not json password=x`)); strings.Contains(got, "password") {
		t.Errorf("invalid json leaked: %s", got)
	}
}

func TestRedactFormURLHeadersBody(t *testing.T) {
	r := DefaultRedactor()
	if got := r.Form("user=a&password=b"); strings.Contains(got, "=b") || !strings.Contains(got, "user=a") {
		t.Errorf("form: %s", got)
	}
	u := r.URL("https://user:pass@example.com/hook?token=abc&x=1#frag")
	if strings.Contains(u, "pass") || strings.Contains(u, "abc") || strings.Contains(u, "frag") || !strings.Contains(u, "example.com/hook") {
		t.Errorf("url: %s", u)
	}
	h := r.Headers(map[string][]string{
		"Authorization":       {"Bearer x"},
		"X-API-Key":           {"secret-key"},
		"Proxy-Authorization": {"Basic x"},
		"Content-Type":        {"application/json"},
	})
	if h["Authorization"] != redactedMarker || h["X-API-Key"] != redactedMarker ||
		h["Proxy-Authorization"] != redactedMarker || h["Content-Type"] != "application/json" {
		t.Errorf("headers: %v", h)
	}
	if got := r.Body("application/octet-stream", []byte("bin"), 0); !strings.HasPrefix(got, "[BODY OMITTED") {
		t.Errorf("body: %s", got)
	}
	if got := r.Body("application/json; charset=utf-8", []byte(`{"a":"`+strings.Repeat("x", 100)+`"}`), 20); !strings.HasSuffix(got, "[TRUNCATED]") {
		t.Errorf("truncate: %s", got)
	}
	if got := truncate("héllo", 2); got != "h...[TRUNCATED]" {
		t.Errorf("utf8 truncate: %q", got)
	}
	if got := r.JSON([]byte(`{"ok":true} {"password":"leak"}`)); got != "[UNPARSEABLE JSON OMITTED]" {
		t.Errorf("trailing JSON must be rejected: %s", got)
	}
	large := []byte(strings.Repeat("x", DefaultMaxRedactionInputBytes+1))
	if got := r.JSON(large); got != tooLargeJSONMarker {
		t.Errorf("large JSON = %q", got)
	}
}

func TestRotatingFileBySize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rf, err := NewRotatingFile(FileConfig{Path: path, MaxSizeMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("active log permissions = %v; want owner-only", info.Mode().Perm())
	}
	clock := time.Date(2026, 10, 6, 10, 0, 0, 0, time.Local)
	rf.now = func() time.Time { clock = clock.Add(time.Second); return clock }

	line := []byte(strings.Repeat("a", 1023) + "\n")
	for i := 0; i < 1024*5; i++ { // ~5MB -> several rotations
		if _, err := rf.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := rf.Close(); err != nil {
		t.Fatal(err)
	}
	backups, _ := filepath.Glob(filepath.Join(dir, "app-*.log"))
	if len(backups) != 2 {
		t.Fatalf("want 2 backups kept, got %d: %v", len(backups), backups)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 1024*1024 {
		t.Fatalf("active file too big: %d", info.Size())
	}
	if _, err := rf.Write(line); err == nil {
		t.Fatal("write after close should fail")
	}
}

func TestRotatingFileRotationFailureReturnsError(t *testing.T) {
	dir := t.TempDir()
	rf, err := NewRotatingFile(FileConfig{Path: filepath.Join(dir, "app.log"), MaxSizeMB: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	// Point the next rotation at a path whose parent does not exist. This makes
	// both creation and restoration fail after the old handle has been closed.
	// Write must return an error rather than panic on a nil file handle.
	rf.path = filepath.Join(dir, "missing", "app.log")
	rf.size = rf.maxSize
	if _, err := rf.Write([]byte("next line\n")); err == nil {
		t.Fatal("write should report an unrecoverable rotation failure")
	}
}

func TestRotatingFileDailyAndMaxAge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rf, err := NewRotatingFile(FileConfig{Path: path, MaxSizeMB: 100, RotateDaily: true, MaxAge: 48 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 10, 1, 12, 0, 0, 0, time.Local)
	rf.now = func() time.Time { return day }
	rf.openedOn = day.Format("2006-01-02")

	for i := 0; i < 5; i++ {
		_, _ = rf.Write([]byte("x\n"))
		day = day.Add(24 * time.Hour)
	}
	_ = rf.Close()
	backups, _ := filepath.Glob(filepath.Join(dir, "app-*.log"))
	// 4 rotations (10-02..10-05); at the last one cutoff is 10-03 12:00,
	// so 10-02 is deleted and 10-03, 10-04, 10-05 survive.
	if len(backups) != 3 {
		t.Fatalf("want 3 backups within max age, got %d: %v", len(backups), backups)
	}
}

func TestRotatingFileBackupNameCollision(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rf, err := NewRotatingFile(FileConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 123000000, time.Local)
	rf.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		if _, err := rf.Write([]byte("line\n")); err != nil {
			t.Fatal(err)
		}
		if err := rf.Rotate(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rf.Close(); err != nil {
		t.Fatal(err)
	}
	backups, _ := filepath.Glob(filepath.Join(dir, "app-*.log"))
	if len(backups) != 2 {
		t.Fatalf("want two unique backups, got %v", backups)
	}
	if err := rf.Rotate(); err == nil {
		t.Fatal("rotate after close should fail")
	}
}
