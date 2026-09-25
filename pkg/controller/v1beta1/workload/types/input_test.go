package types

import (
	"context"
	"testing"
	"time"
)

// Lifecycle code reads time through the input's seam, never time.Now(),
// or a replay cannot pin a deadline.
func TestReconcileInputNowPrefersTheInjectedClock(t *testing.T) {
	fixed := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	in := ReconcileInput{Clock: fixedClock(fixed)}
	if got := in.Now(); !got.Equal(fixed) {
		t.Fatalf("injected clock: got %v want %v", got, fixed)
	}
	before := time.Now()
	unwired := ReconcileInput{}
	if got := unwired.Now(); got.Before(before) {
		t.Fatalf("no clock falls back to the wall clock: got %v, before the call at %v", got, before)
	}
}

// TestReconcileInput_Compiles is a compile-time pin on the
// ReconcileInput struct shape and the MutateInstance callback
// signature. The struct must be value-constructible with no adapter
// scaffolding, and the MutateInstance signature must accept a
// workload-owned InstanceStatus mutator.
func TestReconcileInput_Compiles(t *testing.T) {
	_ = ReconcileInput{
		MutateInstance: func(ctx context.Context, idx int32, mutate func(*InstanceStatus) bool) error {
			// Exercise the mutate callback shape with a no-op caller.
			var s InstanceStatus
			_ = mutate(&s)
			_ = ctx
			_ = idx
			return nil
		},
	}
}
