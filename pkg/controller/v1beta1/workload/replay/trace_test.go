package replay

import (
	"testing"
	"time"
)

// TestNormalizerTokensAreFirstAppearanceOrdered pins the property every
// golden rests on: the same identifier always renders as the same token,
// and the token is derived from the order the run met it.
func TestNormalizerTokensAreFirstAppearanceOrdered(t *testing.T) {
	n := newNormalizer(traceStart)
	if got := n.op("b4f2"); got != "op#1" {
		t.Fatalf("first operation: got %q want op#1", got)
	}
	if got := n.op("9ac1"); got != "op#2" {
		t.Fatalf("second operation: got %q want op#2", got)
	}
	if got := n.op("b4f2"); got != "op#1" {
		t.Fatalf("repeat operation: got %q want op#1", got)
	}
	if got := n.uid("pod-uid-1"); got != "uid#1" {
		t.Fatalf("first uid: got %q want uid#1", got)
	}
	if got := n.op(""); got != "" {
		t.Fatalf("empty id must not consume a token, got %q", got)
	}
}

// TestNormalizerTextReplacesLongestKeyFirst is the guard against a short
// identifier shadowing the longer one it is a prefix of: replacing
// "pod-uid-1" before "pod-uid-11" would leave "uid#11".
func TestNormalizerTextReplacesLongestKeyFirst(t *testing.T) {
	n := newNormalizer(traceStart)
	n.uid("pod-uid-1")
	n.uid("pod-uid-11")
	got := n.text("pod-uid-11 replaces pod-uid-1")
	if want := "uid#2 replaces uid#1"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

// TestNormalizerTextRewritesTimestamps keeps a wall-clock instant the
// engine embedded in a message from making every run differ.
func TestNormalizerTextRewritesTimestamps(t *testing.T) {
	n := newNormalizer(traceStart)
	got := n.text("deadline 2024-01-01T00:02:00Z elapsed")
	if want := "deadline t+2m0s elapsed"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := n.text("not a time: 12:00:00"); got != "not a time: 12:00:00" {
		t.Fatalf("a non-timestamp must survive verbatim, got %q", got)
	}
}

func TestNormalizerAtRendersBothDirections(t *testing.T) {
	n := newNormalizer(traceStart)
	if got := n.at(traceStart.Add(90 * time.Second)); got != "t+1m30s" {
		t.Fatalf("forward: got %q", got)
	}
	if got := n.at(traceStart.Add(-time.Minute)); got != "t-1m0s" {
		t.Fatalf("backward: got %q", got)
	}
	if got := n.at(time.Time{}); got != "nil" {
		t.Fatalf("zero: got %q", got)
	}
}
