package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"net/url"
	"strings"
	"unicode/utf8"

	"go.uber.org/zap"
)

// DefaultSensitiveKeys are matched case-insensitively, ignoring "_" and "-",
// so "api_key", "apiKey" and "API-KEY" are all caught.
var DefaultSensitiveKeys = []string{
	"password", "passwd", "pwd", "secret", "client_secret",
	"new_password", "old_password", "password_confirmation",
	"token", "access_token", "refresh_token", "id_token", "authorization", "auth",
	"api_key", "apikey", "private_key", "privatekey", "sender_private_key",
	"x_api_key", "x_auth_token", "proxy_authorization", "x_amz_security_token", "x_goog_api_key",
	"cookie", "set_cookie", "session",
	"pin", "otp", "cvv", "cvc", "card_number", "pan",
}

const (
	// DefaultMaxRedactionInputBytes bounds the extra allocations made while
	// decoding and re-encoding payloads on a logging path.
	DefaultMaxRedactionInputBytes = 1 << 20 // 1 MiB
	tooLargeJSONMarker            = "[JSON OMITTED: TOO LARGE]"
	tooLargeFormMarker            = "[FORM OMITTED: TOO LARGE]"
)

// Redactor masks sensitive values in request/response bodies, URLs and headers.
// It is immutable and safe for concurrent use.
type Redactor struct {
	keys          map[string]struct{}
	maxInputBytes int
}

// NewRedactor builds a redactor for DefaultSensitiveKeys plus extra keys.
func NewRedactor(extra ...string) *Redactor {
	return NewRedactorWithLimit(DefaultMaxRedactionInputBytes, extra...)
}

// NewRedactorWithLimit is NewRedactor with a custom maximum input size.
// maxInputBytes <= 0 disables the limit and should only be used for trusted,
// already-bounded data.
func NewRedactorWithLimit(maxInputBytes int, extra ...string) *Redactor {
	r := &Redactor{
		keys:          make(map[string]struct{}, len(DefaultSensitiveKeys)+len(extra)),
		maxInputBytes: maxInputBytes,
	}
	for _, k := range append(append([]string{}, DefaultSensitiveKeys...), extra...) {
		if k = normalizeKey(k); k != "" {
			r.keys[k] = struct{}{}
		}
	}
	return r
}

var defaultRedactor = NewRedactor()

// DefaultRedactor returns the shared redactor using DefaultSensitiveKeys.
func DefaultRedactor() *Redactor { return defaultRedactor }

// IsSensitive reports whether key should be masked.
func (r *Redactor) IsSensitive(key string) bool {
	_, ok := r.keys[normalizeKey(key)]
	return ok
}

// JSON returns a copy of a JSON document with sensitive values (at any depth,
// including inside arrays) replaced by "[REDACTED]". If data is not valid JSON
// it returns a placeholder instead of the raw bytes, so nothing leaks by accident.
func (r *Redactor) JSON(data []byte) string {
	if r.inputTooLarge(len(data)) {
		return tooLargeJSONMarker
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // keep large numbers intact
	var v any
	if err := dec.Decode(&v); err != nil {
		return "[UNPARSEABLE JSON OMITTED]"
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return "[UNPARSEABLE JSON OMITTED]"
	}
	out, err := json.Marshal(r.walk(v))
	if err != nil {
		return "[UNSERIALIZABLE JSON OMITTED]"
	}
	return string(out)
}

func (r *Redactor) walk(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if r.IsSensitive(k) {
				t[k] = redactedMarker
				continue
			}
			t[k] = r.walk(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = r.walk(t[i])
		}
		return t
	default:
		return v
	}
}

// Form redacts an application/x-www-form-urlencoded body or URL query string.
func (r *Redactor) Form(raw string) string {
	if r.inputTooLarge(len(raw)) {
		return tooLargeFormMarker
	}
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return "[UNPARSEABLE FORM OMITTED]"
	}
	for k := range vals {
		if r.IsSensitive(k) {
			for i := range vals[k] {
				vals[k][i] = redactedMarker
			}
		}
	}
	return vals.Encode()
}

// Body redacts a body according to its content type and truncates it to
// maxBytes (0 = no limit). Unsupported content types are omitted entirely.
func (r *Redactor) Body(contentType string, body []byte, maxBytes int) string {
	if len(body) == 0 {
		return ""
	}
	ct := strings.ToLower(contentType)
	var s string
	switch {
	case strings.Contains(ct, "json"):
		s = r.JSON(body)
	case strings.Contains(ct, "application/x-www-form-urlencoded"):
		s = r.Form(string(body))
	default:
		return "[BODY OMITTED: " + strings.TrimSpace(strings.SplitN(ct, ";", 2)[0]) + "]"
	}
	return truncate(s, maxBytes)
}

func (r *Redactor) inputTooLarge(n int) bool {
	return r != nil && r.maxInputBytes > 0 && n > r.maxInputBytes
}

// URL strips credentials (user:pass@) and masks every query value, keeping the
// query keys so the log is still useful. Invalid URLs are omitted.
func (r *Redactor) URL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[INVALID URL OMITTED]"
	}
	if u.User != nil {
		u.User = url.User(redactedMarker)
	}
	if u.RawQuery != "" {
		q := u.Query()
		for k := range q {
			for i := range q[k] {
				q[k][i] = redactedMarker
			}
		}
		u.RawQuery = q.Encode()
	}
	u.Fragment = ""
	return u.String()
}

// Headers returns a flat copy of headers with sensitive ones masked.
func (r *Redactor) Headers(h map[string][]string) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if r.IsSensitive(k) {
			out[k] = redactedMarker
			continue
		}
		out[k] = strings.Join(v, ",")
	}
	return out
}

// ---- zap field helpers -----------------------------------------------------

// Redacted logs the key with a fixed "[REDACTED]" value, to show that a value
// existed without revealing it.
func Redacted(key string) zap.Field { return zap.String(key, redactedMarker) }

// SafeURL logs a URL with credentials and query values masked.
func SafeURL(key, raw string) zap.Field { return zap.String(key, defaultRedactor.URL(raw)) }

// SafeJSON logs a JSON payload with sensitive keys masked and truncated to maxBytes.
func SafeJSON(key string, body []byte, maxBytes int) zap.Field {
	return zap.String(key, truncate(defaultRedactor.JSON(body), maxBytes))
}

// Truncated logs s cut to maxBytes (on a UTF-8 boundary) with a suffix marker.
func Truncated(key, s string, maxBytes int) zap.Field { return zap.String(key, truncate(s, maxBytes)) }

func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "...[TRUNCATED]"
}

func normalizeKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	k = strings.ReplaceAll(k, "_", "")
	return strings.ReplaceAll(k, "-", "")
}
