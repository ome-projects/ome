package canary

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// finalGateUnit is two engines and two decoders in one canary group: a
// manual first step at 10 percent, then a final step gated on a metric
// with a five-minute pause and a one-minute sampling interval, inside a
// ten-minute drain window. It walks the unit to its open final gate: 100
// percent shifts at t+1m, a passing sample at t+2m is recorded under the
// pause, and a passing sample at t+6m opens the gate. It returns the unit,
// its inputs and the clock the walk started from.
func finalGateUnit(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, time.Time) {
	t.Helper()
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 5 * time.Minute}}, Analysis: analysisGate(nil)},
	}
	isvc := canaryISVC(steps, i32(600))
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "decnew", StableRevision: "decold"},
	)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 1})
	in.TargetID = "t1"
	in.DesiredReplicas = 2
	in.CanaryPairingProtocol, in.StablePairingProtocol = pdCanaryProtocol, pdStableProtocol
	in.Secondaries = map[v1beta1.ComponentType]MemberRevisions{v1beta1.DecoderComponent: {
		CanaryRevisionHash: "decnew", StableRevisionHash: "decold",
		CanaryPairingProtocol: pdCanaryProtocol, StablePairingProtocol: pdStableProtocol,
		ReadyCanaryCapacity: 1, DesiredReplicas: 2,
	}}
	in.GroupStableRevisionHashes = map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "old", v1beta1.DecoderComponent: "decold"}
	decoderPods := map[string]int32{"decnew": 1, "decold": 1}
	in.GroupReadyPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{v1beta1.DecoderComponent: decoderPods}
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: in.PerRevisionPods, v1beta1.DecoderComponent: decoderPods}
	in.SecondaryCapacityReady = true
	in.Sampler = missingSample{}
	in.BundledPrometheusAddress = "http://prometheus.example.com:9090"
	start := in.Now

	mustReconcile(t, isvc, in) // the first step serves 10 percent under its manual pause
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the promote advances to the final step
	in.Now = start.Add(time.Minute)
	mustReconcile(t, isvc, in) // 100 percent shifts onto version 2; the gate holds with no sample
	in.Now = start.Add(2 * time.Minute)
	withSampler(analysis.Pass)(&in)
	mustReconcile(t, isvc, in) // a passing sample inside the pause is recorded; the gate holds
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePaused {
		t.Fatalf("a passing sample inside the pause must hold the gate, phase %q", got)
	}
	in.Now = start.Add(6 * time.Minute)
	withSampler(analysis.Pass)(&in)
	mustReconcile(t, isvc, in) // the pause elapsed and the sample passed: the gate opens
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePromoting {
		t.Fatalf("a passing sample once the pause elapsed must open the gate, phase %q", got)
	}
	return isvc, in, start
}

func engineRecordOf(isvc *v1beta1.InferenceService) *v1beta1.CanaryStatus {
	return rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
}

func TestReconcile_APassedFinalGateStaysOpenAndReleasesWhenTheWindowEnds(t *testing.T) {
	isvc, in, start := finalGateUnit(t)

	// Ten seconds after the gate opened the pass finds no fresh sample.
	in.Now = start.Add(6*time.Minute + 10*time.Second)
	in.Sampler = missingSample{}
	mustReconcile(t, isvc, in)
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePromoting {
		t.Errorf("ten seconds after the gate passed, a pass with no fresh sample reads %q, want Promoting: a passed gate stays open", got)
	}
	if got := rollout.StateOf(engineRecordOf(isvc), phaseOf(isvc), 2); got != rollout.CanaryStateDraining {
		t.Errorf("state %q, want Draining", got)
	}

	// The window ends ten minutes after the shift; the pass still has no
	// fresh sample.
	in.Now = start.Add(11 * time.Minute)
	mustReconcile(t, isvc, in)
	cs := engineRecordOf(isvc)
	if cs.CurrentStep != 2 {
		t.Errorf("at the window's end with no fresh sample the record is at step %d, want 2: the release lands when the window ends", cs.CurrentStep)
	}
	if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != rollout.CanaryStateDone {
		t.Errorf("state %q, want Done", got)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePromoting {
		t.Errorf("phase %q, want Promoting through the cutover", got)
	}
}

func TestReconcile_AnOpenFinalGateStillRollsBackOnABreachAtTheLimit(t *testing.T) {
	isvc, in, start := finalGateUnit(t)

	// Three breaches, one per sampling interval, reach the failure limit.
	for i, at := range []time.Duration{7 * time.Minute, 8 * time.Minute, 9 * time.Minute} {
		in.Now = start.Add(at)
		withSampler(analysis.Fail)(&in)
		res := mustReconcile(t, isvc, in)
		cs := engineRecordOf(isvc)
		if cs.AnalysisFailedChecks != int32(i+1) {
			t.Fatalf("after breach %d the record counts %d failing checks", i+1, cs.AnalysisFailedChecks)
		}
		if i < 2 {
			if cs.RolledBackRevisionHash != "" || res.RolledBack {
				t.Fatalf("breach %d is below the limit and must not roll back", i+1)
			}
			if cs.CurrentStep != 1 {
				t.Fatalf("breach %d must not release the held Instances, step %d", i+1, cs.CurrentStep)
			}
			continue
		}
		if cs.RolledBackRevisionHash != "new" || !res.RolledBack {
			t.Fatalf("the breach at the limit must roll back: rejected %q, rolledBack=%v", cs.RolledBackRevisionHash, res.RolledBack)
		}
		if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != rollout.CanaryStateRollingBack {
			t.Errorf("state %q, want RollingBack", got)
		}
	}
}
