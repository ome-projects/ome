package rolloutrun

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// A rollingUpdate group whose members change one at a time. The run records
// every member: a changed one at its new target, an unchanged one at the
// revision it already runs. The run closes once every member rests on its
// record, which an unchanged member does from the start.

const (
	decoderRev    = "llm-a-decoder-dddddddd"
	decoderNewRev = "llm-a-decoder-eeeeeeee"
)

func pdRollingGroup() v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{},
	}
}

// decoderIR is the decoder's replica at the given revision pair and counters.
func decoderIR(current, target string, replicas, updated int32) *v1beta1.InferenceReplica {
	ir := namedIR("llm-a-decoder", current, target)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = replicas, updated
	return ir
}

// rollingIR is the engine's replica mid-roll: one of two Instances on the target.
func rollingIR(current, target string) *v1beta1.InferenceReplica {
	ir := irFixture(current, target)
	ir.Status.Replicas, ir.Status.UpdatedReplicas = 2, 1
	return ir
}

func requireTarget(t *testing.T, targets []v1beta1.RolloutRunTarget, comp v1beta1.ComponentType, revision, stable string) {
	t.Helper()
	for _, target := range targets {
		if target.Component != comp {
			continue
		}
		if target.Revision != revision || target.StableRevision != stable {
			t.Fatalf("%s recorded at %+v, want revision %q with stable %q", comp, target, revision, stable)
		}
		return
	}
	t.Fatalf("no %s target among %+v", comp, targets)
}

// Only the engine's template changed: the run records the decoder at the
// revision it runs, stays open while the engine rolls, and closes Completed
// once the engine's pair agrees with its record and every Instance is updated,
// the decoder never having moved.
func TestOneMemberChangeOpensARunThatPinsTheOtherAtItsOwnRevision(t *testing.T) {
	isvc := isvcFixture(pdRollingGroup())
	settledDecoder := decoderIR(decoderRev, decoderRev, 2, 2)

	if out := pass(t, isvc, divergedIR(oldRev, newRev), settledDecoder); !out.Opened {
		t.Fatalf("outcome = %+v, want the engine's new target to open a run", out)
	}
	run := requirePinned(t, isvc)
	requireTarget(t, run.TargetRevisions, v1beta1.EngineComponent, "bbbbbbbb", "aaaaaaaa")
	requireTarget(t, run.TargetRevisions, v1beta1.DecoderComponent, "dddddddd", "dddddddd")

	if out := pass(t, isvc, rollingIR(oldRev, newRev), settledDecoder); out.StateChanged || !v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want the run kept open while the engine rolls", out, v1beta1.RolloutRunActive(isvc))
	}

	out := pass(t, isvc, settledIR(newRev), settledDecoder)
	if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want the converged engine to close the run", out, v1beta1.RolloutRunActive(isvc))
	}
	last := isvc.Status.Rollout.LastRun
	if last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("lastRun = %+v, want Completed", last)
	}
	requireTarget(t, last.TargetRevisions, v1beta1.EngineComponent, "bbbbbbbb", "aaaaaaaa")
	requireTarget(t, last.TargetRevisions, v1beta1.DecoderComponent, "dddddddd", "dddddddd")
}

// The decoder's template changes while the engine is still rolling toward
// its own target. The recorded run closes Superseded and a fresh one opens in
// the same pass: the engine keeps its in-flight target and its stable, the
// decoder is recorded at its new target with the revision it runs as stable,
// and the fresh run closes only when both rest on their records.
func TestASecondMemberChangeMidRollKeepsTheFirstMembersTarget(t *testing.T) {
	isvc := isvcFixture(pdRollingGroup())
	if out := pass(t, isvc, divergedIR(oldRev, newRev), decoderIR(decoderRev, decoderRev, 2, 2)); !out.Opened {
		t.Fatalf("outcome = %+v, want the engine's new target to open a run", out)
	}
	first := requirePinned(t, isvc).RunID

	out := pass(t, isvc, rollingIR(oldRev, newRev), decoderIR(decoderRev, decoderNewRev, 2, 0))
	if !out.Opened || !out.StateChanged {
		t.Fatalf("outcome = %+v, want the decoder's new target to open a fresh run", out)
	}
	run := requirePinned(t, isvc)
	if run.RunID == first {
		t.Fatal("the decoder's new target must replace the engine's run, not continue it")
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunSuperseded {
		t.Fatalf("lastRun = %+v, want the first run closed Superseded", last)
	}
	requireTarget(t, run.TargetRevisions, v1beta1.EngineComponent, "bbbbbbbb", "aaaaaaaa")
	requireTarget(t, run.TargetRevisions, v1beta1.DecoderComponent, "eeeeeeee", "dddddddd")

	if out := pass(t, isvc, settledIR(newRev), decoderIR(decoderRev, decoderNewRev, 2, 1)); out.StateChanged || !v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want the run kept open while the decoder rolls", out, v1beta1.RolloutRunActive(isvc))
	}
	out = pass(t, isvc, settledIR(newRev), decoderIR(decoderNewRev, decoderNewRev, 2, 2))
	if !out.StateChanged || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("outcome = %+v active=%v, want both converged members to close the run", out, v1beta1.RolloutRunActive(isvc))
	}
	last := isvc.Status.Rollout.LastRun
	if last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("lastRun = %+v, want Completed", last)
	}
	requireTarget(t, last.TargetRevisions, v1beta1.EngineComponent, "bbbbbbbb", "aaaaaaaa")
	requireTarget(t, last.TargetRevisions, v1beta1.DecoderComponent, "eeeeeeee", "dddddddd")
}
