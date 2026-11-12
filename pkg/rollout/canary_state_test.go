package rollout

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestStateOf(t *testing.T) {
	failed := &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureCapacityTimeout}
	for _, tc := range []struct {
		name  string
		cs    *v1beta1.CanaryStatus
		phase v1beta1.RolloutPhase
		steps int
		want  CanaryState
	}{
		{"nil status is Idle", nil, v1beta1.RolloutPhaseStable, 2, CanaryStateIdle},
		{"no phase yet is Staging", &v1beta1.CanaryStatus{}, "", 2, CanaryStateStaging},
		{"Pending is Staging", &v1beta1.CanaryStatus{}, v1beta1.RolloutPhasePending, 2, CanaryStateStaging},
		{"Canarying is Serving", &v1beta1.CanaryStatus{}, v1beta1.RolloutPhaseCanarying, 2, CanaryStateServing},
		{"Paused is Serving", &v1beta1.CanaryStatus{}, v1beta1.RolloutPhasePaused, 2, CanaryStateServing},
		{"pre-step hold wins over Paused", &v1beta1.CanaryStatus{PreStepHold: true}, v1beta1.RolloutPhasePaused, 2, CanaryStatePreHold},
		{"Promoting is Draining", &v1beta1.CanaryStatus{CurrentStep: 1}, v1beta1.RolloutPhasePromoting, 2, CanaryStateDraining},
		{"a held final gate at full traffic is Draining", &v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 100}, v1beta1.RolloutPhasePaused, 2, CanaryStateDraining},
		{"a held intermediate gate at full traffic is Serving", &v1beta1.CanaryStatus{CurrentStep: 0, ObservedTrafficWeight: 100}, v1beta1.RolloutPhasePaused, 2, CanaryStateServing},
		{"done sentinel is Done whatever the phase", &v1beta1.CanaryStatus{CurrentStep: 2}, v1beta1.RolloutPhasePromoting, 2, CanaryStateDone},
		{"rejected hash with pods draining is RollingBack", &v1beta1.CanaryStatus{RolledBackRevisionHash: "b"}, v1beta1.RolloutPhaseRollingBack, 2, CanaryStateRollingBack},
		{"rejected hash settled is RolledBack", &v1beta1.CanaryStatus{RolledBackRevisionHash: "b"}, v1beta1.RolloutPhaseRolledBack, 2, CanaryStateRolledBack},
		{"a recorded failure is Failed", &v1beta1.CanaryStatus{Failed: failed}, v1beta1.RolloutPhasePending, 2, CanaryStateFailed},
		{"a Failed phase written on a unit short of its last step is Failed", &v1beta1.CanaryStatus{}, v1beta1.RolloutPhaseFailed, 2, CanaryStateFailed},
		{"a Failed phase written on a finished unit is Done", &v1beta1.CanaryStatus{CurrentStep: 2}, v1beta1.RolloutPhaseFailed, 2, CanaryStateDone},
		{"a recorded failure on a finished unit is Failed", &v1beta1.CanaryStatus{CurrentStep: 2, Failed: failed}, v1beta1.RolloutPhaseFailed, 2, CanaryStateFailed},
		{"a rollback request ends a Failed park", &v1beta1.CanaryStatus{Failed: failed, RolledBackRevisionHash: "b"}, v1beta1.RolloutPhaseRollingBack, 2, CanaryStateRollingBack},
		{"a rollback with no stable revision is a park", &v1beta1.CanaryStatus{Failed: &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureStableRevisionMissing}, RolledBackRevisionHash: "b"}, v1beta1.RolloutPhaseFailed, 2, CanaryStateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := StateOf(tc.cs, tc.phase, tc.steps); got != tc.want {
				t.Fatalf("StateOf = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStepOf(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{{Traffic: 25}, {Traffic: 50}, {Traffic: 100}}
	for _, tc := range []struct {
		name   string
		step   int32
		phase  v1beta1.RolloutPhase
		weight int32
		want   int32
	}{
		{"a recorded step is kept", 1, v1beta1.RolloutPhasePaused, 50, 1},
		{"the done sentinel is kept", 3, v1beta1.RolloutPhaseStable, 100, 3},
		{"no traffic shifted yet", -1, v1beta1.RolloutPhasePending, 0, 0},
		{"first split", -1, v1beta1.RolloutPhasePaused, 25, 0},
		{"second split", -1, v1beta1.RolloutPhasePaused, 50, 1},
		{"weight between two steps", -1, v1beta1.RolloutPhasePaused, 40, 0},
		{"drain at 100%", -1, v1beta1.RolloutPhasePromoting, 100, 2},
		{"completed cutover", -1, v1beta1.RolloutPhaseStable, 100, 3},
		{"completed cutover with no weight recorded", -1, v1beta1.RolloutPhaseStable, 0, 3},
		{"parked before any split", -1, v1beta1.RolloutPhaseFailed, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := &v1beta1.CanaryStatus{CurrentStep: tc.step, ObservedTrafficWeight: tc.weight}
			if got := StepOf(cs, tc.phase, steps); got != tc.want {
				t.Fatalf("StepOf = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCanaryStateTerminal(t *testing.T) {
	for s, want := range map[CanaryState]bool{
		CanaryStateFailed: true, CanaryStateRolledBack: true,
		CanaryStateIdle: false, CanaryStateServing: false, CanaryStateDone: false, CanaryStateRollingBack: false,
	} {
		if s.Terminal() != want {
			t.Errorf("%s.Terminal() = %v, want %v", s, !want, want)
		}
	}
}

func TestClampStep(t *testing.T) {
	step := func(traffic int32) v1beta1.RolloutGroupStep { return v1beta1.RolloutGroupStep{Traffic: traffic} }
	four := []v1beta1.RolloutGroupStep{step(10), step(30), step(60), step(100)}
	two := []v1beta1.RolloutGroupStep{step(10), step(100)}
	six := []v1beta1.RolloutGroupStep{step(10), step(20), step(30), step(60), step(80), step(100)}
	for _, tc := range []struct {
		name     string
		cs       v1beta1.CanaryStatus
		phase    v1beta1.RolloutPhase
		ranUnder []v1beta1.RolloutGroupStep
		steps    []v1beta1.RolloutGroupStep
		wantStep int32
		wantHold bool
	}{
		{"a shorter list lands the kept index on a step above the recorded traffic, held", v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 30}, v1beta1.RolloutPhasePaused, four, two, 1, true},
		{"an index past the shorter list's end lands on its last step", v1beta1.CanaryStatus{CurrentStep: 2, ObservedTrafficWeight: 60}, v1beta1.RolloutPhasePaused, four, two, 1, true},
		{"a list whose kept step serves the recorded traffic sets no hold", v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 30}, v1beta1.RolloutPhasePaused, four, []v1beta1.RolloutGroupStep{step(10), step(30), step(100)}, 1, false},
		{"a longer list keeps the index where a lower step serves less", v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 30}, v1beta1.RolloutPhasePaused, four, six, 1, false},
		{"a unit past its old list stays past the new one", v1beta1.CanaryStatus{CurrentStep: 4, ObservedTrafficWeight: 100}, v1beta1.RolloutPhasePromoting, four, six, 6, false},
		{"a unit past its old list stays past a shorter one", v1beta1.CanaryStatus{CurrentStep: 4, ObservedTrafficWeight: 100}, v1beta1.RolloutPhaseStable, four, two, 2, false},
		{"a negative index is read against the list it ran under", v1beta1.CanaryStatus{CurrentStep: -1, ObservedTrafficWeight: 30}, v1beta1.RolloutPhasePaused, four, two, 1, true},
		{"a staging step whose traffic is still to come is held", v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 10}, v1beta1.RolloutPhasePending, four, four, 1, true},
		{"with no list ran under the index is kept and bounded", v1beta1.CanaryStatus{CurrentStep: 3, ObservedTrafficWeight: 100}, v1beta1.RolloutPhasePromoting, nil, two, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := tc.cs
			ClampStep(&cs, tc.phase, tc.ranUnder, tc.steps)
			if cs.CurrentStep != tc.wantStep || cs.PreStepHold != tc.wantHold {
				t.Fatalf("step %d hold %v, want step %d hold %v", cs.CurrentStep, cs.PreStepHold, tc.wantStep, tc.wantHold)
			}
		})
	}
	t.Run("no steps to land on leaves the record alone", func(t *testing.T) {
		cs := v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 30}
		ClampStep(&cs, v1beta1.RolloutPhasePaused, four, nil)
		if cs.CurrentStep != 1 || cs.PreStepHold {
			t.Fatalf("record moved: %+v", cs)
		}
	})
}
