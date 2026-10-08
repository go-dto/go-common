package httpclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var errResponseTooLarge = errors.New("response body exceeds configured limit")

// Response is a fully buffered and already-closed HTTP response.
type Response struct {
	StatusCode int
	Attempts   int
	Duration   time.Duration

	headers   http.Header
	body      []byte
	success   bool
	peer      string
	method    Method
	operation string
}

// IsSuccess reports whether the status matched the request's accepted statuses.
func (r *Response) IsSuccess() bool { return r != nil && r.success }

// Header returns the first value for a response header.
func (r *Response) Header(name string) string {
	if r == nil {
		return ""
	}
	return r.headers.Get(name)
}

// HeaderValues returns a copied list of all values for a response header.
func (r *Response) HeaderValues(name string) []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.headers.Values(name)...)
}

// Headers returns a flattened copy of response headers.
func (r *Response) Headers() HeaderMap {
	if r == nil {
		return nil
	}
	result := make(HeaderMap, len(r.headers))
	for name, values := range r.headers {
		result[name] = strings.Join(values, ",")
	}
	return result
}

// Bytes returns a copy of the buffered response body.
func (r *Response) Bytes() []byte {
	if r == nil {
		return nil
	}
	return append([]byte(nil), r.body...)
}

// Text returns the buffered response body as UTF-8 text.
func (r *Response) Text() string {
	if r == nil {
		return ""
	}
	return string(r.body)
}

// JSON decodes exactly one JSON value from the response body.
func (r *Response) JSON(destination any) error {
	if r == nil {
		return &Error{Kind: KindDecode, message: "Response is nil"}
	}
	if destination == nil {
		return &Error{
			Kind: KindDecode, Peer: r.peer, Method: r.method, Operation: r.operation,
			StatusCode: r.StatusCode, Attempts: r.Attempts, message: "JSON destination is nil",
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(r.body))
	if err := decoder.Decode(destination); err != nil {
		return &Error{
			Kind: KindDecode, Peer: r.peer, Method: r.method, Operation: r.operation,
			StatusCode: r.StatusCode, Attempts: r.Attempts, cause: err,
		}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = errors.New("response contains more than one JSON value")
		}
		return &Error{
			Kind: KindDecode, Peer: r.peer, Method: r.method, Operation: r.operation,
			StatusCode: r.StatusCode, Attempts: r.Attempts, cause: err,
		}
	}
	return nil
}

func readLimitedBody(body io.Reader, limit int64) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	reader := io.LimitReader(body, limit+1)
	data, err := io.ReadAll(reader)
	if err != nil {
		return data, err
	}
	if int64(len(data)) > limit {
		return data[:limit], fmt.Errorf("%w: maximum %d bytes", errResponseTooLarge, limit)
	}
	return data, nil
}
