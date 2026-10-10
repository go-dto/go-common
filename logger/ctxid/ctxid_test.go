package ctxid

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestContextLifecycle(t *testing.T) {
	//lint:ignore SA1012 deliberately testing the public nil-context boundary
	if _, ok := FromContext(nil); ok {
		t.Fatal("nil context must not contain an id")
	}
	//lint:ignore SA1012 deliberately testing the public nil-context boundary
	ctx := WithContext(nil, "request-1")
	if id, ok := FromContext(ctx); !ok || id != "request-1" {
		t.Fatalf("id=%q ok=%v", id, ok)
	}
	ensured, id := Ensure(ctx)
	if ensured != ctx || id != "request-1" {
		t.Fatal("Ensure must preserve an existing id and context")
	}
	ensured, id = Ensure(context.Background())
	if got, ok := FromContext(ensured); !ok || got != id || len(id) != 32 {
		t.Fatalf("generated id=%q context id=%q ok=%v", id, got, ok)
	}
}

func TestValid(t *testing.T) {
	for _, id := range []string{"abc", "A-1_b.c:2", strings.Repeat("x", MaxLength)} {
		if !Valid(id) {
			t.Errorf("expected valid id %q", id)
		}
	}
	for _, id := range []string{"", "with space", "line\nbreak", strings.Repeat("x", MaxLength+1)} {
		if Valid(id) {
			t.Errorf("expected invalid id %q", id)
		}
	}
}

func TestNewFallbackRemainsUnique(t *testing.T) {
	old := readRandom
	t.Cleanup(func() { readRandom = old })

	readRandom = func([]byte) (int, error) { return 0, errors.New("entropy unavailable") }
	first, second := New(), New()
	if len(first) != 32 || len(second) != 32 || first == second {
		t.Fatalf("fallback ids must be unique 32-character hex strings: %q %q", first, second)
	}

	readRandom = func(dst []byte) (int, error) {
		dst[0] = 1
		return 1, nil
	}
	partial := New()
	if len(partial) != 32 || partial == "01000000000000000000000000000000" {
		t.Fatalf("partial entropy must use the fallback: %q", partial)
	}
}
