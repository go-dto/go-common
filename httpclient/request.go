package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// Method is an HTTP method understood by Client.Request. Applications normally
// use the method-specific Get/Post/Put/Patch/Delete helpers instead.
type Method string

const (
	MethodGet     Method = "GET"
	MethodHead    Method = "HEAD"
	MethodPost    Method = "POST"
	MethodPut     Method = "PUT"
	MethodPatch   Method = "PATCH"
	MethodDelete  Method = "DELETE"
	MethodOptions Method = "OPTIONS"
)

// HeaderMap represents request or response headers without requiring net/http.
type HeaderMap map[string]string

func (h HeaderMap) clone() HeaderMap {
	if h == nil {
		return nil
	}
	copy := make(HeaderMap, len(h))
	for name, value := range h {
		copy[name] = value
	}
	return copy
}

// RequestOption configures one high-level request.
type RequestOption interface {
	apply(*requestConfig) error
}

type requestOption func(*requestConfig) error

func (o requestOption) apply(cfg *requestConfig) error { return o(cfg) }

type requestConfig struct {
	pathParams     map[string]string
	query          url.Values
	headers        http.Header
	body           []byte
	bodySet        bool
	contentType    string
	accepted       map[int]struct{}
	operation      string
	requestTimeout time.Duration
}

// Path safely substitutes a {name} placeholder using URL path escaping.
func Path(name, value string) RequestOption {
	name = strings.TrimSpace(name)
	return requestOption(func(cfg *requestConfig) error {
		if name == "" || strings.ContainsAny(name, "{}") {
			return errors.New("path parameter name is invalid")
		}
		if _, exists := cfg.pathParams[name]; exists {
			return fmt.Errorf("path parameter %q was provided more than once", name)
		}
		cfg.pathParams[name] = value
		return nil
	})
}

// Query sets one query parameter. Values are URL encoded automatically.
func Query(name, value string) RequestOption {
	return requestOption(func(cfg *requestConfig) error {
		if strings.TrimSpace(name) == "" {
			return errors.New("query parameter name is required")
		}
		cfg.query.Set(name, value)
		return nil
	})
}

// QueryValues sets a repeated query parameter.
func QueryValues(name string, values ...string) RequestOption {
	values = append([]string(nil), values...)
	return requestOption(func(cfg *requestConfig) error {
		if strings.TrimSpace(name) == "" {
			return errors.New("query parameter name is required")
		}
		cfg.query.Del(name)
		for _, value := range values {
			cfg.query.Add(name, value)
		}
		return nil
	})
}

// Queries sets multiple single-valued query parameters.
func Queries(values map[string]string) RequestOption {
	values = cloneStringMap(values)
	return requestOption(func(cfg *requestConfig) error {
		for name, value := range values {
			if strings.TrimSpace(name) == "" {
				return errors.New("query parameter name is required")
			}
			cfg.query.Set(name, value)
		}
		return nil
	})
}

// Header sets one request header. Hop-by-hop and transport-owned headers are rejected.
func Header(name, value string) RequestOption {
	return requestOption(func(cfg *requestConfig) error {
		if err := validateHeader(name, value); err != nil {
			return err
		}
		cfg.headers.Set(name, value)
		return nil
	})
}

// Headers sets multiple request headers.
func Headers(values map[string]string) RequestOption {
	values = cloneStringMap(values)
	return requestOption(func(cfg *requestConfig) error {
		for name, value := range values {
			if err := validateHeader(name, value); err != nil {
				return err
			}
			cfg.headers.Set(name, value)
		}
		return nil
	})
}

// JSON serializes value and sets Content-Type to application/json.
func JSON(value any) RequestOption {
	return requestOption(func(cfg *requestConfig) error {
		body, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode JSON body: %w", err)
		}
		return cfg.setBody(body, "application/json")
	})
}

// Form encodes a single-valued application/x-www-form-urlencoded body.
func Form(values map[string]string) RequestOption {
	values = cloneStringMap(values)
	return requestOption(func(cfg *requestConfig) error {
		form := make(url.Values, len(values))
		for name, value := range values {
			form.Set(name, value)
		}
		return cfg.setBody([]byte(form.Encode()), "application/x-www-form-urlencoded")
	})
}

// Text sets a UTF-8 plain-text request body.
func Text(value string) RequestOption {
	return requestOption(func(cfg *requestConfig) error {
		return cfg.setBody([]byte(value), "text/plain; charset=utf-8")
	})
}

// Bytes sets a copied byte body with the supplied content type.
func Bytes(contentType string, value []byte) RequestOption {
	value = append([]byte(nil), value...)
	return requestOption(func(cfg *requestConfig) error {
		if strings.TrimSpace(contentType) == "" {
			return errors.New("content type is required for byte body")
		}
		return cfg.setBody(append([]byte(nil), value...), contentType)
	})
}

// IdempotencyKey marks a write request as safe for retry.
func IdempotencyKey(value string) RequestOption {
	return requestOption(func(cfg *requestConfig) error {
		if strings.TrimSpace(value) == "" {
			return errors.New("idempotency key is required")
		}
		return Header("Idempotency-Key", value).apply(cfg)
	})
}

// AcceptStatus replaces the default success range (200-299) for this request.
func AcceptStatus(statusCodes ...int) RequestOption {
	statusCodes = append([]int(nil), statusCodes...)
	return requestOption(func(cfg *requestConfig) error {
		if len(statusCodes) == 0 {
			return errors.New("at least one accepted status is required")
		}
		cfg.accepted = make(map[int]struct{}, len(statusCodes))
		for _, statusCode := range statusCodes {
			if statusCode < 100 || statusCode > 599 {
				return fmt.Errorf("invalid accepted HTTP status %d", statusCode)
			}
			cfg.accepted[statusCode] = struct{}{}
		}
		return nil
	})
}

// Operation supplies a safe, low-cardinality logical name for logs and metrics.
// Never put IDs, URLs, tokens or user-provided values in this name.
func Operation(name string) RequestOption {
	name = strings.TrimSpace(name)
	return requestOption(func(cfg *requestConfig) error {
		if name == "" || len(name) > 128 || strings.ContainsAny(name, "\r\n\t") {
			return errors.New("operation must be 1-128 characters without control characters")
		}
		cfg.operation = name
		return nil
	})
}

// RequestTimeout gives one call a stricter budget than the Client default.
func RequestTimeout(timeout time.Duration) RequestOption {
	return requestOption(func(cfg *requestConfig) error {
		if timeout <= 0 {
			return errors.New("request timeout must be positive")
		}
		cfg.requestTimeout = timeout
		return nil
	})
}

func (cfg *requestConfig) setBody(body []byte, contentType string) error {
	if cfg.bodySet {
		return errors.New("request body was configured more than once")
	}
	cfg.body, cfg.bodySet, cfg.contentType = body, true, contentType
	return nil
}

func (c *Client) Get(ctx context.Context, pathTemplate string, options ...RequestOption) (*Response, error) {
	return c.Request(ctx, MethodGet, pathTemplate, options...)
}

func (c *Client) Head(ctx context.Context, pathTemplate string, options ...RequestOption) (*Response, error) {
	return c.Request(ctx, MethodHead, pathTemplate, options...)
}

func (c *Client) Post(ctx context.Context, pathTemplate string, options ...RequestOption) (*Response, error) {
	return c.Request(ctx, MethodPost, pathTemplate, options...)
}

func (c *Client) Put(ctx context.Context, pathTemplate string, options ...RequestOption) (*Response, error) {
	return c.Request(ctx, MethodPut, pathTemplate, options...)
}

func (c *Client) Patch(ctx context.Context, pathTemplate string, options ...RequestOption) (*Response, error) {
	return c.Request(ctx, MethodPatch, pathTemplate, options...)
}

func (c *Client) Delete(ctx context.Context, pathTemplate string, options ...RequestOption) (*Response, error) {
	return c.Request(ctx, MethodDelete, pathTemplate, options...)
}

// Request executes a high-level request. The response body is buffered with a
// configured size limit and closed before this method returns.
func (c *Client) Request(ctx context.Context, method Method, pathTemplate string, options ...RequestOption) (*Response, error) {
	if c == nil || c.client == nil {
		return nil, &Error{Kind: KindInvalidConfig, message: "Client is nil"}
	}
	if ctx == nil {
		return nil, wrapRequestError(KindInvalidRequest, c.cfg, method, "", 0, errors.New("context is nil"))
	}
	if !validToken(string(method)) {
		return nil, wrapRequestError(KindInvalidRequest, c.cfg, method, "", 0, errors.New("method is invalid"))
	}

	requestCfg := requestConfig{
		pathParams: make(map[string]string),
		query:      make(url.Values),
		headers:    make(http.Header),
		operation:  string(method),
	}
	for name, value := range c.cfg.DefaultHeaders {
		requestCfg.headers.Set(name, value)
	}
	for _, option := range options {
		if option == nil {
			return nil, wrapRequestError(KindInvalidRequest, c.cfg, method, requestCfg.operation, 0, errors.New("request option is nil"))
		}
		if err := option.apply(&requestCfg); err != nil {
			return nil, wrapRequestError(KindInvalidRequest, c.cfg, method, requestCfg.operation, 0, err)
		}
	}
	if (method == MethodGet || method == MethodHead) && requestCfg.bodySet {
		return nil, wrapRequestError(KindInvalidRequest, c.cfg, method, requestCfg.operation, 0, errors.New("GET and HEAD requests cannot have a body"))
	}

	requestURL, err := c.buildURL(pathTemplate, requestCfg.pathParams, requestCfg.query)
	if err != nil {
		return nil, wrapRequestError(KindInvalidRequest, c.cfg, method, requestCfg.operation, 0, err)
	}
	if c.cfg.Auth != nil {
		if err := c.cfg.Auth.apply(ctx, requestCfg.headers); err != nil {
			clientErr := wrapRequestError(KindAuth, c.cfg, method, requestCfg.operation, 0, err)
			c.logResult(ctx, resultEvent{method: method, operation: requestCfg.operation, outcome: "auth_error", err: clientErr})
			return nil, clientErr
		}
	}
	if requestCfg.bodySet && requestCfg.headers.Get("Content-Type") == "" {
		requestCfg.headers.Set("Content-Type", requestCfg.contentType)
	}

	requestCtx := ctx
	var cancel context.CancelFunc
	if requestCfg.requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestCfg.requestTimeout)
		defer cancel()
	}
	state := &requestState{method: method, operation: requestCfg.operation}
	requestCtx = context.WithValue(requestCtx, requestStateKey{}, state)

	var body io.Reader
	if requestCfg.bodySet {
		body = bytes.NewReader(requestCfg.body)
	}
	request, err := http.NewRequestWithContext(requestCtx, string(method), requestURL.String(), body)
	if err != nil {
		return nil, wrapRequestError(KindInvalidRequest, c.cfg, method, requestCfg.operation, 0, err)
	}
	request.Header = requestCfg.headers.Clone()

	started := time.Now()
	standardResponse, requestErr := c.client.Do(request)
	duration := time.Since(started)
	attempts := max(int(state.attempts.Load()), 1)
	if requestErr != nil {
		kind := classifyCause(requestErr)
		clientErr := wrapRequestError(kind, c.cfg, method, requestCfg.operation, attempts, requestErr)
		clientErr.RetryExhausted = attempts > 1 && attempts >= c.cfg.MaxRetries+1
		c.logResult(ctx, resultEvent{
			method: method, operation: requestCfg.operation, attempts: attempts,
			duration: duration, outcome: string(kind), err: clientErr,
		})
		return nil, clientErr
	}

	bodyBytes, readErr := readLimitedBody(standardResponse.Body, c.cfg.MaxResponseBodyBytes)
	closeErr := standardResponse.Body.Close()
	duration = time.Since(started)
	response := &Response{
		StatusCode: standardResponse.StatusCode,
		Attempts:   attempts,
		Duration:   duration,
		headers:    standardResponse.Header.Clone(),
		body:       bodyBytes,
		peer:       c.cfg.Peer,
		method:     method,
		operation:  requestCfg.operation,
	}
	if readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		kind := classifyCause(readErr)
		if errors.Is(readErr, errResponseTooLarge) {
			kind = KindResponseTooLarge
		}
		clientErr := wrapRequestError(kind, c.cfg, method, requestCfg.operation, attempts, readErr)
		c.logResult(ctx, resultEvent{
			method: method, operation: requestCfg.operation, statusCode: response.StatusCode,
			attempts: attempts, duration: duration, outcome: string(kind), err: clientErr,
		})
		return response, clientErr
	}

	if !requestCfg.accepts(response.StatusCode) {
		clientErr := &Error{
			Kind: KindHTTPStatus, Peer: c.cfg.Peer, Method: method,
			Operation: requestCfg.operation, StatusCode: response.StatusCode, Attempts: attempts,
			RetryExhausted: attempts > 1 && attempts >= c.cfg.MaxRetries+1 && slices.Contains(c.cfg.RetryStatuses, response.StatusCode),
		}
		c.logResult(ctx, resultEvent{
			method: method, operation: requestCfg.operation, statusCode: response.StatusCode,
			attempts: attempts, duration: duration, outcome: "http_error", err: clientErr,
		})
		return response, clientErr
	}

	response.success = true
	outcome := "success"
	if attempts > 1 {
		outcome = "success_after_retry"
	}
	c.logResult(ctx, resultEvent{
		method: method, operation: requestCfg.operation, statusCode: response.StatusCode,
		attempts: attempts, duration: duration, outcome: outcome,
	})
	return response, nil
}

func (cfg *requestConfig) accepts(statusCode int) bool {
	if cfg.accepted == nil {
		return statusCode >= 200 && statusCode <= 299
	}
	_, ok := cfg.accepted[statusCode]
	return ok
}

func (c *Client) buildURL(pathTemplate string, params map[string]string, query url.Values) (*url.URL, error) {
	if pathTemplate == "" || !strings.HasPrefix(pathTemplate, "/") {
		return nil, errors.New("path must start with /")
	}
	escapedPath := pathTemplate
	for name, value := range params {
		placeholder := "{" + name + "}"
		if !strings.Contains(escapedPath, placeholder) {
			return nil, fmt.Errorf("path placeholder %s was not found", placeholder)
		}
		escapedPath = strings.ReplaceAll(escapedPath, placeholder, url.PathEscape(value))
	}
	if strings.ContainsAny(escapedPath, "{}") {
		return nil, errors.New("one or more path placeholders have no value")
	}

	relative, err := url.Parse(escapedPath)
	if err != nil || relative.IsAbs() || relative.Host != "" || relative.RawQuery != "" || relative.Fragment != "" {
		return nil, errors.New("path must be relative and must not contain query or fragment")
	}
	for _, segment := range strings.Split(relative.Path, "/") {
		if segment == ".." {
			return nil, errors.New("path traversal is not allowed")
		}
	}

	base := cloneURL(c.baseURL)
	baseEscaped := strings.TrimSuffix(base.EscapedPath(), "/")
	relativeEscaped := "/" + strings.TrimPrefix(relative.EscapedPath(), "/")
	joinedEscaped := baseEscaped + relativeEscaped
	joinedPath, err := url.PathUnescape(joinedEscaped)
	if err != nil {
		return nil, errors.New("path contains invalid escaping")
	}
	base.Path = joinedPath
	base.RawPath = ""
	if joinedEscaped != joinedPath {
		base.RawPath = joinedEscaped
	}
	base.RawQuery = query.Encode()
	return base, nil
}

var forbiddenHeaders = map[string]struct{}{
	"connection":          {},
	"content-length":      {},
	"host":                {},
	"keep-alive":          {},
	"proxy-authenticate":  {},
	"proxy-authorization": {},
	"proxy-connection":    {},
	"te":                  {},
	"trailer":             {},
	"transfer-encoding":   {},
	"upgrade":             {},
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}

func validateHeader(name, value string) error {
	if !validToken(name) {
		return fmt.Errorf("header name %q is invalid", name)
	}
	if _, forbidden := forbiddenHeaders[strings.ToLower(name)]; forbidden {
		return fmt.Errorf("header %q is controlled by the transport", name)
	}
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("header %q contains a newline", name)
	}
	return nil
}

func validToken(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
			continue
		default:
			return false
		}
	}
	return true
}
