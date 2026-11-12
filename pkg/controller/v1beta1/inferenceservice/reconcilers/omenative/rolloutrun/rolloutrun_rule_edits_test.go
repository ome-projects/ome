package rolloutrun

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// openPairRun opens a fresh run over a rolling group of the engine and the
// decoder under a ratio rule at tolerance 5, both members diverged.
func openPairRun(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{},
		MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
	})
	if out := pass(t, isvc, namedIR("llm-a-engine", oldRev, newRev), namedIR("llm-a-decoder", decoderRev, decoderNewRev)); !out.Opened || out.Adopted {
		t.Fatalf("fixture: %+v, want a fresh open", out)
	}
	return isvc
}

// A ratio rule edited while the run is open leaves the run as it is: the
// same run, the plan it recorded, and no drift, since the rule is read from
// the spec by the coordination gates and is not plan content.
func TestARatioRuleEditKeepsTheRunAndReadsInSync(t *testing.T) {
	isvc := openPairRun(t)
	id := isvc.Status.Rollout.ActiveRun.RunID
	engine, decoder := namedIR("llm-a-engine", oldRev, newRev), namedIR("llm-a-decoder", decoderRev, decoderNewRev)

	isvc.Spec.Rollout.Groups[0].MaintainRatio.Tolerance = ptr.To(int32(20))
	if out := pass(t, isvc, engine, decoder); !emptyOutcome(out) {
		t.Fatalf("the tolerance raised: outcome = %+v, want nothing", out)
	}
	run := requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
	if got := run.Plan.Groups[0].Group.MaintainRatio.Tolerance; got == nil || *got != 5 {
		t.Fatalf("the recorded plan reads tolerance %v, want the 5 it was opened with", got)
	}
	if got := rollout.EffectiveCoordination(isvc, rollout.Policies{}).Groups[0].MaintainRatio.Tolerance; got == nil || *got != 20 {
		t.Fatalf("the coordination view reads tolerance %v, want the spec's 20", got)
	}

	isvc.Spec.Rollout.Groups[0].MaintainRatio = nil
	if out := pass(t, isvc, engine, decoder); !emptyOutcome(out) {
		t.Fatalf("the rule removed: outcome = %+v, want nothing", out)
	}
	run = requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
	if run.Plan.Groups[0].Group.MaintainRatio == nil {
		t.Fatal("the recorded plan lost the rule it was opened with")
	}
	if rollout.EffectiveCoordination(isvc, rollout.Policies{}).Groups[0].MaintainRatio != nil {
		t.Fatal("the coordination view keeps a rule the spec no longer names")
	}
}

// A rolling group removed while its run is open leaves the run on its
// recorded revisions, reported as drift with the removed-group warning;
// the run closes Completed once, when both members rest on them, and the
// pass after opens nothing.
func TestARunWhoseRollingGroupLeftTheSpecClosesOnceBothSidesConverge(t *testing.T) {
	isvc := openPairRun(t)
	id := isvc.Status.Rollout.ActiveRun.RunID

	isvc.Spec.Rollout = nil
	out, events := passWith(t, isvc, nil, namedIR("llm-a-engine", oldRev, newRev), namedIR("llm-a-decoder", decoderRev, decoderNewRev))
	if out.StateChanged || out.RequeueAfter != shortRequeue || !hasEvent(events, EventGroupRemoved) {
		t.Fatalf("outcome = %+v events = %v, want the run kept with the removed-group warning", out, events)
	}
	requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
	if len(rollout.EffectiveCoordination(isvc, rollout.Policies{}).Groups[0].Components) != 0 {
		t.Fatal("the coordination view still lists the removed group's members")
	}

	// The engine finishes first; the run waits for the decoder.
	out = pass(t, isvc, namedIR("llm-a-engine", newRev, newRev), namedIR("llm-a-decoder", decoderRev, decoderNewRev))
	if out.StateChanged {
		t.Fatalf("outcome = %+v, want the run kept while the decoder still rolls", out)
	}
	requireSameRun(t, isvc, id)

	out = pass(t, isvc, namedIR("llm-a-engine", newRev, newRev), namedIR("llm-a-decoder", decoderNewRev, decoderNewRev))
	if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want the Completed close", out, v1beta1.RolloutRunActive(isvc))
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("lastRun = %+v, want Completed", last)
	}
	requireNoRun(t, isvc, v1beta1.RolloutPlanReasonNoRun)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)

	if out := pass(t, isvc, namedIR("llm-a-engine", newRev, newRev), namedIR("llm-a-decoder", decoderNewRev, decoderNewRev)); !emptyOutcome(out) || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want nothing after the close", out, v1beta1.RolloutRunActive(isvc))
	}
}

// A rolling group's budget edited while the run is open leaves the run as
// it is and reads no drift: the budgets are read from the spec by the
// coordination gates. A change of the group's progression kind is still
// drift: the recorded kind runs to the end of the run.
func TestAGroupBudgetEditKeepsTheRunAndAProgressionChangeIsDrift(t *testing.T) {
	isvc := openPairRun(t)
	id := isvc.Status.Rollout.ActiveRun.RunID
	engine, decoder := namedIR("llm-a-engine", oldRev, newRev), namedIR("llm-a-decoder", decoderRev, decoderNewRev)

	isvc.Spec.Rollout.Groups[0].RollingUpdate.MaxUnavailable = ptr.To(intstr.FromString("5%"))
	if out := pass(t, isvc, engine, decoder); !emptyOutcome(out) {
		t.Fatalf("the budget lowered: outcome = %+v, want nothing", out)
	}
	run := requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionFalse, v1beta1.RolloutPlanDriftReasonInSync)
	if got := run.Plan.Groups[0].Group.RollingUpdate.MaxUnavailable; got != nil {
		t.Fatalf("the recorded plan reads maxUnavailable %v, want the unset budget it was opened with", got)
	}
	if got := rollout.EffectiveCoordination(isvc, rollout.Policies{}).Groups[0].RollingUpdate.MaxUnavailable; got == nil || got.String() != "5%" {
		t.Fatalf("the coordination view reads maxUnavailable %v, want the spec's 5%%", got)
	}

	isvc.Spec.Rollout.Groups[0].RollingUpdate = nil
	isvc.Spec.Rollout.Groups[0].BlueGreen = &v1beta1.GroupBlueGreen{}
	if out := pass(t, isvc, engine, decoder); !emptyOutcome(out) {
		t.Fatalf("the progression changed: outcome = %+v, want nothing", out)
	}
	run = requireSameRun(t, isvc, id)
	requireDrift(t, isvc, corev1.ConditionTrue, v1beta1.RolloutPlanDriftReasonSpecNewerThanRun)
	if run.Plan.Groups[0].Group.RollingUpdate == nil {
		t.Fatal("the recorded plan lost the rolling progression it was opened with")
	}
	if view := rollout.EffectiveCoordination(isvc, rollout.Policies{}); view.Groups[0].RollingUpdate == nil || view.Groups[0].BlueGreen != nil {
		t.Fatal("the coordination view must keep the recorded progression until the next run")
	}
}
