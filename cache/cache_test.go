package cache

import (
	"context"
	"errors"
	"testing"
)

func TestErrorClassificationAndSanitizedMessage(t *testing.T) {
	secretCause := errors.New("dial cache.internal with password=hunter2")
	err := NewError("get", KindUnavailable, secretCause)
	if got := err.Error(); got != "cache: get: unavailable" {
		t.Fatalf("Error() = %q", got)
	}
	if KindOf(err) != KindUnavailable || !errors.Is(err, secretCause) {
		t.Fatalf("classification/unwrapping failed: %v", err)
	}
	if KindOf(ErrMiss) != KindMiss || !IsMiss(ErrMiss) {
		t.Fatal("miss classification failed")
	}
	if KindOf(ErrClosed) != KindClosed {
		t.Fatal("closed classification failed")
	}
	if KindOf(context.Canceled) != KindCanceled || KindOf(context.DeadlineExceeded) != KindTimeout {
		t.Fatal("context classification failed")
	}
	if KindOf(errors.New("unknown")) != KindUnavailable || KindOf(nil) != "" {
		t.Fatal("fallback classification failed")
	}
	var nilErr *Error
	if nilErr.Error() != "cache: error" || nilErr.Unwrap() != nil {
		t.Fatal("nil error methods are not safe")
	}
	if NewError("", KindInvalid, nil).Error() != "cache: invalid" {
		t.Fatal("empty operation message mismatch")
	}
	if !IsTimeout(NewError("get", KindTimeout, nil)) {
		t.Fatal("typed timeout not detected")
	}
}
