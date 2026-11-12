package query

import (
	"testing"

	"k8s.io/utils/ptr"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	stableRevisionName = "svc-engine-00000old"
	targetRevisionName = "svc-engine-00000new"
)

// A canary step's stable side is the revision the status reports as
// current while the step's projected partition holds Instances and the
// roll target is another revision. Everything else names no stable side:
// the user's own rollingUpdate partition, a step that released the
// Component, a Component that has promoted no revision yet, a roll back to
// the current revision, and a pass with no roll target.
func TestCanaryStepStableRevision_NamesTheCurrentRevisionWhileAStepHolds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		step    *int32
		current string
		target  string
		want    string
	}{
		{name: "a step holds the Component", step: ptr.To[int32](2), current: stableRevisionName, target: targetRevisionName, want: stableRevisionName},
		{name: "no step is projected", current: stableRevisionName, target: targetRevisionName},
		{name: "the step released the Component", step: ptr.To[int32](0), current: stableRevisionName, target: targetRevisionName},
		{name: "no revision promoted yet", step: ptr.To[int32](1), target: targetRevisionName},
		{name: "the roll returns to the current revision", step: ptr.To[int32](1), current: stableRevisionName, target: stableRevisionName},
		{name: "no roll target", step: ptr.To[int32](1), current: stableRevisionName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desired := workload.WorkloadDesiredSpec{}
			if tc.step != nil {
				desired.Pacing = &workload.WorkloadPacing{Partition: tc.step}
			}
			observed := &workload.WorkloadObservedState{CurrentRevision: tc.current}
			if got := CanaryStepStableRevision(desired, observed, tc.target); got != tc.want {
				t.Errorf("CanaryStepStableRevision() = %q, want %q", got, tc.want)
			}
		})
	}
}

// The revision a new Instance is built on is the step's stable side while
// a canary step holds the Component. Under a plain roll, with no roll open
// and under a roll back the answer is empty: the roll target decides.
func TestNewInstanceRevision_JoinsTheStepsStableSideElseLeavesTheTargetToDecide(t *testing.T) {
	step := workload.WorkloadDesiredSpec{Pacing: &workload.WorkloadPacing{Partition: ptr.To[int32](1)}}
	plain := workload.WorkloadDesiredSpec{}
	onStable := &workload.WorkloadObservedState{CurrentRevision: stableRevisionName}
	onTarget := &workload.WorkloadObservedState{CurrentRevision: targetRevisionName}

	if got := NewInstanceRevision(step, onStable, targetRevisionName); got != stableRevisionName {
		t.Errorf("under a canary step a new Instance joins the stable side %s, got %q", stableRevisionName, got)
	}
	if got := NewInstanceRevision(plain, onStable, targetRevisionName); got != "" {
		t.Errorf("under a plain roll the target decides, got %q", got)
	}
	if got := NewInstanceRevision(step, onTarget, targetRevisionName); got != "" {
		t.Errorf("with no roll open the target decides, got %q", got)
	}
	if got := NewInstanceRevision(step, onStable, stableRevisionName); got != "" {
		t.Errorf("under a roll back to the current revision the target decides, got %q", got)
	}
	if got := NewInstanceRevision(step, nil, targetRevisionName); got != "" {
		t.Errorf("with nothing observed the target decides, got %q", got)
	}
}
