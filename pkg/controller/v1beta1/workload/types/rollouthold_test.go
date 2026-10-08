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

// A parked attempt is an Update operation on the parked step; the wait it
// names is a rollout hold gate, never a hold authority's token.
func TestOperationParked_UpdateAttemptOnTheParkedStep(t *testing.T) {
	for _, gate := range []RolloutHoldGate{RolloutHoldGateRatio, RolloutHoldGateSequential, RolloutHoldGateBudget, RolloutHoldGateRetryBlock, RolloutHoldGateHeld, RolloutHoldGatePairing} {
		if !ParkedWaitingReason(string(gate)) {
			t.Errorf("%s names a wait a parked attempt stands behind", gate)
		}
		if !OperationParked(&InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepParked, Waiting: string(gate)}) {
			t.Errorf("an Update attempt on the parked step waiting on %s is parked", gate)
		}
		if OperationParked(&InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepDrain, Waiting: string(gate)}) {
			t.Errorf("an attempt on its Drain step is in flight whatever wait it names")
		}
		if OperationParked(&InstanceOperation{Type: InstanceOperationRestart, Step: UpdateStepParked, Waiting: string(gate)}) {
			t.Errorf("only an Update attempt parks")
		}
	}
	for _, token := range []string{"", WaitingReasonUnschedulable, WaitingReasonPaused, WaitingReasonPodGroupTerminating} {
		if ParkedWaitingReason(token) {
			t.Errorf("%q is a hold authority's token or none, not a wait a parked attempt names", token)
		}
	}
	if OperationParked(nil) {
		t.Error("no operation is not parked")
	}
}
