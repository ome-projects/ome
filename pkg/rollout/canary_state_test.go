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
		{"failure marker is Failed", &v1beta1.CanaryStatus{Failed: failed}, v1beta1.RolloutPhasePending, 2, CanaryStateFailed},
		{"legacy Failed phase without a marker is Failed", &v1beta1.CanaryStatus{}, v1beta1.RolloutPhaseFailed, 2, CanaryStateFailed},
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
