package httpclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
)

// ErrorKind is a stable, low-cardinality error classification suitable for
// control flow, logs and metrics.
type ErrorKind string

const (
	KindInvalidConfig    ErrorKind = "invalid_config"
	KindInvalidRequest   ErrorKind = "invalid_request"
	KindAuth             ErrorKind = "auth"
	KindCanceled         ErrorKind = "canceled"
	KindTimeout          ErrorKind = "timeout"
	KindTLS              ErrorKind = "tls"
	KindTransport        ErrorKind = "transport"
	KindHTTPStatus       ErrorKind = "http_status"
	KindResponseTooLarge ErrorKind = "response_too_large"
	KindDecode           ErrorKind = "decode"
)

// Error is returned for request construction, transport, HTTP status and
// response decoding failures. Error messages deliberately omit URLs, headers
// and response bodies so they are safe to include in application logs.
type Error struct {
	Kind           ErrorKind
	Peer           string
	Operation      string
	Method         Method
	StatusCode     int
	Attempts       int
	RetryExhausted bool

	message string
	cause   error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	prefix := "httpclient"
	if e.Peer != "" {
		prefix += ": " + e.Peer
	}
	if e.Operation != "" {
		prefix += " " + e.Operation
	}
	if e.StatusCode > 0 {
		return fmt.Sprintf("%s returned HTTP %d", prefix, e.StatusCode)
	}
	if e.message != "" {
		return prefix + ": " + e.message
	}
	if e.Kind != "" {
		return prefix + ": " + string(e.Kind)
	}
	return prefix + ": request failed"
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// KindOf extracts a stable ErrorKind. Unknown errors are classified as transport errors.
func KindOf(err error) ErrorKind {
	if err == nil {
		return ""
	}
	var clientErr *Error
	if errors.As(err, &clientErr) {
		return clientErr.Kind
	}
	return classifyCause(err)
}

// IsTimeout reports whether err was classified as a timeout.
func IsTimeout(err error) bool { return KindOf(err) == KindTimeout }

// IsStatus reports whether err represents the given non-success HTTP status.
func IsStatus(err error, statusCode int) bool {
	var clientErr *Error
	return errors.As(err, &clientErr) && clientErr.Kind == KindHTTPStatus && clientErr.StatusCode == statusCode
}

// IsRetryExhausted reports whether all configured attempts were used.
func IsRetryExhausted(err error) bool {
	var clientErr *Error
	return errors.As(err, &clientErr) && clientErr.RetryExhausted
}

func classifyCause(err error) ErrorKind {
	switch {
	case errors.Is(err, context.Canceled):
		return KindCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout
	}

	var (
		netErr      net.Error
		certErr     *tls.CertificateVerificationError
		unknownAuth x509.UnknownAuthorityError
		hostErr     x509.HostnameError
		invalidCert x509.CertificateInvalidError
	)
	switch {
	case errors.As(err, &certErr), errors.As(err, &unknownAuth),
		errors.As(err, &hostErr), errors.As(err, &invalidCert):
		return KindTLS
	case errors.As(err, &netErr) && netErr.Timeout():
		return KindTimeout
	default:
		return KindTransport
	}
}

func wrapRequestError(kind ErrorKind, cfg Config, method Method, operation string, attempts int, err error) *Error {
	result := &Error{
		Kind:      kind,
		Peer:      cfg.Peer,
		Method:    method,
		Operation: operation,
		Attempts:  attempts,
		cause:     err,
	}
	if kind == KindInvalidRequest && err != nil {
		result.message = err.Error()
	}
	return result
}
