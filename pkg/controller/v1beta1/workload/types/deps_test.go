package types

import (
	"testing"
	"time"
)

func TestDepsNowPrefersTheInjectedClock(t *testing.T) {
	fixed := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	d := &Deps{Clock: fixedClock(fixed)}
	if got := d.Now(); !got.Equal(fixed) {
		t.Fatalf("injected clock: got %v want %v", got, fixed)
	}
	before := time.Now()
	if got := (&Deps{}).Now(); got.Before(before) {
		t.Fatalf("no clock falls back to the wall clock: got %v, before the call at %v", got, before)
	}
}

// The uncached reader is what a read-after-write must go to; Reader falls
// back to the cached client only when no APIReader is wired.
func TestDepsReaderPrefersTheUncachedAPIReader(t *testing.T) {
	cached, uncached := newNamedClient("cached"), newNamedClient("uncached")
	d := &Deps{Client: cached, APIReader: uncached}
	if got := d.Reader().(*namedClient); got.name != "uncached" {
		t.Fatalf("APIReader wins: got %q", got.name)
	}
	d = &Deps{Client: cached}
	if got := d.Reader().(*namedClient); got.name != "cached" {
		t.Fatalf("no APIReader falls back to the client: got %q", got.name)
	}
}

func TestDepsExpectationsCacheFallsBackToTheSingleton(t *testing.T) {
	own := NewExpectations()
	if got := (&Deps{Expectations: own}).ExpectationsCache(); got != own {
		t.Error("a wired cache is the one used")
	}
	if got := (&Deps{}).ExpectationsCache(); got != DefaultExpectations {
		t.Error("an unwired Deps shares the process singleton")
	}
}
