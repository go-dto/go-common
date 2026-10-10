package httpclient

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
)

// Authenticator adds authentication to every request without exposing secret
// values to logs. Use the constructors in this package.
type Authenticator interface {
	apply(context.Context, http.Header) error
	validate() error
}

type authFunc struct {
	validateFunc func() error
	applyFunc    func(context.Context, http.Header) error
}

func (a authFunc) apply(ctx context.Context, header http.Header) error {
	return a.applyFunc(ctx, header)
}

func (a authFunc) validate() error { return a.validateFunc() }

// TokenProvider returns a bearer token for one request. It can refresh tokens
// without rebuilding the Client.
type TokenProvider func(context.Context) (string, error)

// BearerToken configures a static Authorization: Bearer header.
func BearerToken(token string) Authenticator {
	return authFunc{
		validateFunc: func() error {
			if strings.TrimSpace(token) == "" {
				return errors.New("bearer token is required")
			}
			if strings.ContainsAny(token, "\r\n") {
				return errors.New("bearer token contains a newline")
			}
			return nil
		},
		applyFunc: func(_ context.Context, header http.Header) error {
			header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
			return nil
		},
	}
}

// BearerTokenProvider configures a dynamic bearer token provider.
func BearerTokenProvider(provider TokenProvider) Authenticator {
	return authFunc{
		validateFunc: func() error {
			if provider == nil {
				return errors.New("token provider is required")
			}
			return nil
		},
		applyFunc: func(ctx context.Context, header http.Header) error {
			token, err := provider(ctx)
			if err != nil {
				return err
			}
			token = strings.TrimSpace(token)
			if token == "" {
				return errors.New("token provider returned an empty token")
			}
			if strings.ContainsAny(token, "\r\n") {
				return errors.New("token provider returned a token containing a newline")
			}
			header.Set("Authorization", "Bearer "+token)
			return nil
		},
	}
}

// BasicAuth configures HTTP Basic authentication.
func BasicAuth(username, password string) Authenticator {
	return authFunc{
		validateFunc: func() error {
			if strings.ContainsAny(username, "\r\n") || strings.ContainsAny(password, "\r\n") {
				return errors.New("basic auth credentials contain a newline")
			}
			return nil
		},
		applyFunc: func(_ context.Context, header http.Header) error {
			encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
			header.Set("Authorization", "Basic "+encoded)
			return nil
		},
	}
}

// APIKey configures a static API key in a named header.
func APIKey(headerName, value string) Authenticator {
	return authFunc{
		validateFunc: func() error {
			if value == "" {
				return errors.New("API key value is required")
			}
			return validateHeader(headerName, value)
		},
		applyFunc: func(_ context.Context, header http.Header) error {
			header.Set(headerName, value)
			return nil
		},
	}
}
