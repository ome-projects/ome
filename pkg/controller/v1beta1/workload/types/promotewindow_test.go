package types

import (
	"testing"
	"time"
)

// The first Instance to cross its window is the first thing the next pass
// can commit, so the sink keeps the earliest remainder it saw.
func TestPromoteWindowKeepsTheEarliestRemainder(t *testing.T) {
	var w PromoteWindow
	if got := w.Pending(); got != 0 {
		t.Fatalf("nothing waiting: got %v want 0", got)
	}
	w.Observe(30 * time.Second)
	w.Observe(5 * time.Second)
	w.Observe(10 * time.Second)
	if got := w.Pending(); got != 5*time.Second {
		t.Fatalf("earliest window wins: got %v want 5s", got)
	}
}

func TestPromoteWindowIgnoresAnElapsedWindow(t *testing.T) {
	var w PromoteWindow
	w.Observe(0)
	w.Observe(-time.Second)
	if got := w.Pending(); got != 0 {
		t.Fatalf("an elapsed window owes no wake-up: got %v", got)
	}
}

func TestPromoteWindowIsNilSafe(t *testing.T) {
	var w *PromoteWindow
	w.Observe(time.Second)
	if got := w.Pending(); got != 0 {
		t.Fatalf("a nil sink pends nothing: got %v", got)
	}
}
