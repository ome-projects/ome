package types

import (
	"testing"
	"time"
)

func TestPassWake_KeepsTheEarliestInterval(t *testing.T) {
	var w PassWake
	w.Observe(30 * time.Second)
	w.Observe(5 * time.Second)
	w.Observe(10 * time.Second)
	if got := w.Pending(); got != 5*time.Second {
		t.Fatalf("earliest wait wins: got %v want 5s", got)
	}
	if w.Bare() {
		t.Fatalf("a configured interval is not a bare wake-up")
	}
}

// A bare wake-up asks for the controller's rate-limited backoff, which an
// explicit wait already beats: the dispatcher applies it only when the
// result carries no explicit wait.
func TestPassWake_BareIsReportedBesideTheInterval(t *testing.T) {
	var bare PassWake
	bare.Observe(0)
	if !bare.Bare() || bare.Pending() != 0 {
		t.Fatalf("a bare wake-up asks for backoff and no interval: got bare=%v pending=%v", bare.Bare(), bare.Pending())
	}
	bare.Observe(5 * time.Second)
	if !bare.Bare() || bare.Pending() != 5*time.Second {
		t.Fatalf("both deposits are kept: got bare=%v pending=%v", bare.Bare(), bare.Pending())
	}
}

func TestPassWake_IsNilSafe(t *testing.T) {
	var w *PassWake
	w.Observe(time.Second)
	if w.Pending() != 0 || w.Bare() {
		t.Fatalf("a nil sink reports nothing: got pending=%v bare=%v", w.Pending(), w.Bare())
	}
}
