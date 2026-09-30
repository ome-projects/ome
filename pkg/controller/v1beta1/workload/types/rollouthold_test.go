package types

import "testing"

// DrainHolds keeps the first hold of a pass and tolerates a nil receiver, so an
// op can report through it whether or not the dispatcher allocated one.
func TestDrainHolds_KeepsTheFirstHold(t *testing.T) {
	var none *DrainHolds
	none.Observe(RolloutHold{Gate: RolloutHoldGatePairing})
	if none.First() != nil {
		t.Fatalf("a nil observer must report no hold")
	}

	holds := &DrainHolds{}
	if holds.First() != nil {
		t.Fatalf("a fresh observer must report no hold")
	}
	holds.Observe(RolloutHold{Gate: RolloutHoldGatePairing, Reason: "peer cohort not serving", Target: "rev-a"})
	holds.Observe(RolloutHold{Gate: RolloutHoldGateBudget, Reason: "later", Target: "rev-b"})
	got := holds.First()
	if got == nil || got.Gate != RolloutHoldGatePairing || got.Target != "rev-a" {
		t.Fatalf("the first hold must be kept, got %+v", got)
	}
}
