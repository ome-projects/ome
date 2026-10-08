package canary

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A canary-owned Component carries the same rolled-out revision fields a
// coordination-owned one does, published by the canary executor: the
// revision whose pods reached Ready leads while the ladder is in flight, and
// a completed promotion or rollback records the revision that owns the
// member's traffic and the one it superseded.

// revisionWant is one member's expected rolled-out revision fields, by hash.
type revisionWant struct {
	ready, latest, previous string
}

func expectRevisions(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, want revisionWant) {
	t.Helper()
	name := func(hash string) string {
		if hash == "" {
			return ""
		}
		return coordination.PerRevisionServiceName(isvc.Name, comp, hash)
	}
	cs := isvc.Status.Components[comp]
	if cs.LatestReadyRevision != name(want.ready) || cs.LatestRolledoutRevision != name(want.latest) || cs.PreviousRolledoutRevision != name(want.previous) {
		t.Fatalf("%s revisions: ready %q latest %q previous %q, want ready %q latest %q previous %q",
			comp, cs.LatestReadyRevision, cs.LatestRolledoutRevision, cs.PreviousRolledoutRevision,
			name(want.ready), name(want.latest), name(want.previous))
	}
}

func TestDispatch_MembersRecordTheRolledOutRevisions(t *testing.T) {
	f := newPDCanary(t, "pdrev", true)
	f.dispatch("serve")
	// The split serves: each member's target has reached Ready, nothing has
	// completed.
	expectRevisions(t, f.isvc, v1beta1.EngineComponent, revisionWant{ready: "engnew"})
	expectRevisions(t, f.isvc, v1beta1.DecoderComponent, revisionWant{ready: "decnew"})

	annotate(f.isvc, constants.RolloutPromoteAnnotation, "engnew")
	f.dispatch("advance")
	f.movePod("engine", "engold", "engnew", "0")
	f.movePod("decoder", "decold", "decnew", "0")
	f.dispatch("shift") // 100% traffic moves; the release reads the next pass's count
	f.dispatch("cut over")
	if f.phase() != v1beta1.RolloutPhaseStable {
		t.Fatalf("the ladder must complete, got %q", f.phase())
	}
	done := func() {
		t.Helper()
		expectRevisions(t, f.isvc, v1beta1.EngineComponent, revisionWant{ready: "engnew", latest: "engnew", previous: "engold"})
		expectRevisions(t, f.isvc, v1beta1.DecoderComponent, revisionWant{ready: "decnew", latest: "decnew", previous: "decold"})
	}
	done()
	// The done sentinel keeps the record.
	f.dispatch("done")
	done()
}

// After a completed promotion the run closes Completed, and the next run
// may pin a member with no stable revision. A rollback then resolves each
// member's stable revision from its LatestRolledoutRevision: the secondary
// has no canary record of its own, and a completed run's record is not a
// rollback's to use.
func TestDispatch_RollbackStableResolvesFromTheRolledOutRevision(t *testing.T) {
	f := newPDCanary(t, "pdfb", true)
	f.dispatch("serve")
	annotate(f.isvc, constants.RolloutPromoteAnnotation, "engnew")
	f.dispatch("advance")
	f.movePod("engine", "engold", "engnew", "0")
	f.movePod("decoder", "decold", "decnew", "0")
	f.dispatch("shift") // 100% traffic moves; the release reads the next pass's count
	f.dispatch("cut over")
	if f.phase() != v1beta1.RolloutPhaseStable {
		t.Fatalf("the ladder must complete, got %q", f.phase())
	}

	// The run closes, each IR promotes its current revision and names the
	// next target, and the fresh run pins the targets with no stable revision.
	closed := f.isvc.Status.Rollout.ActiveRun
	f.isvc.Status.Rollout.LastRun = &v1beta1.RolloutRunRecord{
		Outcome:         v1beta1.RolloutRunCompleted,
		TargetRevisions: append([]v1beta1.RolloutRunTarget(nil), closed.TargetRevisions...),
	}
	reopenRun(f.isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "engnext"},
		v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "decnext"})
	next := map[v1beta1.ComponentType][2]string{
		v1beta1.EngineComponent:  {"engnew", "engnext"},
		v1beta1.DecoderComponent: {"decnew", "decnext"},
	}
	for comp, pair := range next {
		key := types.NamespacedName{Namespace: f.ns, Name: f.name + "-" + string(comp)}
		updateIR(t, f.c, key, func(ir *v1beta1.InferenceReplica) {
			ir.Status.CurrentRevision = f.name + "-" + string(comp) + "-" + pair[0]
			ir.Status.UpdateRevision = f.name + "-" + string(comp) + "-" + pair[1]
		})
	}
	f.dispatch("arm")
	if cs := rollout.CanaryStatusFor(&f.isvc.Status, v1beta1.EngineComponent); cs == nil || cs.CanaryRevisionHash != "engnext" || cs.CurrentStep != 0 {
		t.Fatalf("the new target must arm a fresh ladder, got %+v", cs)
	}
	if got := componentStableRevisionHash(f.isvc, v1beta1.DecoderComponent, v1beta1.EngineComponent); got != "decnew" {
		t.Fatalf("the decoder's stable revision = %q, want dNew from its LatestRolledoutRevision", got)
	}

	annotateStored(t, f.c, f.isvc, constants.RolloutRollbackAnnotation, "true")
	f.dispatch("roll back")
	if f.phase() != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("no member serves the rejected revision, got %q", f.phase())
	}
	for comp, pair := range next {
		got := &v1beta1.InferenceReplica{}
		if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.ns, Name: f.name + "-" + string(comp)}, got); err != nil {
			t.Fatal(err)
		}
		want := f.name + "-" + string(comp) + "-" + pair[0]
		if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != want {
			t.Fatalf("%s must roll back to its last promoted revision %q, got pacing=%+v", comp, want, got.Spec.Pacing)
		}
	}
	f.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 100, protocol: pdCanaryProtocol},
	})
	expectRevisions(t, f.isvc, v1beta1.EngineComponent, revisionWant{ready: "engnew", latest: "engnew", previous: "engold"})
	expectRevisions(t, f.isvc, v1beta1.DecoderComponent, revisionWant{ready: "decnew", latest: "decnew", previous: "decold"})
}

// pdReadyDecoder is a bumped decoder whose target has Ready capacity.
func pdReadyDecoder() MemberRevisions {
	m := bumpedDecoder()
	m.ReadyCanaryCapacity = 1
	return m
}

func TestReconcile_CompletionRecordsEveryMembersRevisions(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2}, pdReadyDecoder())
	mustReconcile(t, isvc, in)
	// In flight: each target has reached Ready; nothing owns the traffic yet.
	expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{ready: "new"})
	expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{ready: "decnew"})

	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 4}
	mustReconcile(t, isvc, in) // 100% traffic moves; the release reads the next pass's count
	if res := mustReconcile(t, isvc, in); !res.Complete {
		t.Fatalf("the ungated final step completes, got %+v", res)
	}
	expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{ready: "new", latest: "new", previous: "old"})
	expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{ready: "decnew", latest: "decnew", previous: "decold"})
}

// A target with no Ready capacity is not a ready revision yet.
func TestReconcile_UnreadyTargetPublishesNoReadyRevision(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"old": 4}, bumpedDecoder())
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("the step waits for its capacity, got %q", phaseOf(isvc))
	}
	expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{})
	expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{})
}

// A completed revert records the stable revision as the one that owns the
// traffic; the rejected revision, which never completed, is never demoted
// into PreviousRolledoutRevision, and the hold re-records nothing.
func TestReconcile_RollbackCompletionRecordsTheStableRevision(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2}, pdReadyDecoder())
	// The engine's record still names a revision the stable one superseded;
	// the decoder carries none.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {LatestRolledoutRevision: coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "older")},
	}
	mustReconcile(t, isvc, in)
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	if res := mustReconcile(t, isvc, in); !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rejected pods still serve, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{ready: "new", latest: "older"})
	expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{ready: "decnew"})

	in.PerRevisionPods = map[string]int32{"old": 4}
	if res := mustReconcile(t, isvc, in); !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("every member is back on stable, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	reverted := func() {
		t.Helper()
		expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{ready: "old", latest: "old", previous: "older"})
		expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{ready: "decold", latest: "decold"})
	}
	reverted()
	mustReconcile(t, isvc, in)
	reverted()
}

// A member that was not bumped records its one revision without demoting it.
func TestReconcile_UnbumpedMemberRecordsItsRevisionWithoutDemotion(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2}, MemberRevisions{
		CanaryRevisionHash: "decold", StableRevisionHash: "decold", ReadyCanaryCapacity: 2,
	})
	mustReconcile(t, isvc, in)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 4}
	mustReconcile(t, isvc, in) // 100% traffic moves; the release reads the next pass's count
	if res := mustReconcile(t, isvc, in); !res.Complete {
		t.Fatalf("the ungated final step completes, got %+v", res)
	}
	expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{ready: "decold", latest: "decold"})
}

// With no ladder active, each member is recorded on the one revision its
// replica has settled on: the pair agrees at the replica's generation, a pod
// of it is Ready and no pod of another revision exists. A member recorded
// there already changes nothing; one recorded elsewhere moves to it and the
// prior latest is demoted to previous. Anything less than settled records
// nothing.
func TestRecordSettledRevisionsFollowsTheReplica(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"}}
	g := &v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}}
	settled := func(hash string) observedCanaryRevisions {
		return observedCanaryRevisions{currentHash: hash, targetHash: hash, fromIR: true, statusFresh: true}
	}
	pods := func(engine, decoder string) map[v1beta1.ComponentType]map[string]int32 {
		return map[v1beta1.ComponentType]map[string]int32{
			v1beta1.EngineComponent:  {engine: 2},
			v1beta1.DecoderComponent: {decoder: 1},
		}
	}
	secondaries := map[v1beta1.ComponentType]observedCanaryRevisions{v1beta1.DecoderComponent: settled("dnew")}

	recordSettledRevisions(isvc, g, v1beta1.EngineComponent, settled("engnew"), secondaries, pods("engnew", "dnew"), pods("engnew", "dnew"))
	expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{ready: "engnew", latest: "engnew"})
	expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{ready: "dnew", latest: "dnew"})

	recordSettledRevisions(isvc, g, v1beta1.EngineComponent, settled("engnew"), secondaries, pods("engnew", "dnew"), pods("engnew", "dnew"))
	expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{ready: "engnew", latest: "engnew"})

	recordSettledRevisions(isvc, g, v1beta1.EngineComponent, settled("engnext"), secondaries, pods("engnext", "dnew"), pods("engnext", "dnew"))
	expectRevisions(t, isvc, v1beta1.EngineComponent, revisionWant{ready: "engnext", latest: "engnext", previous: "engnew"})
	expectRevisions(t, isvc, v1beta1.DecoderComponent, revisionWant{ready: "dnew", latest: "dnew"})

	for name, tc := range map[string]struct {
		observed     observedCanaryRevisions
		total, ready map[string]int32
	}{
		"a pod of another revision":  {settled("x"), map[string]int32{"x": 1, "y": 1}, map[string]int32{"x": 1}},
		"no Ready pod":               {settled("x"), map[string]int32{"x": 1}, map[string]int32{}},
		"a pair that disagrees":      {observedCanaryRevisions{currentHash: "x", targetHash: "y", fromIR: true, statusFresh: true}, map[string]int32{"x": 1}, map[string]int32{"x": 1}},
		"a status behind generation": {observedCanaryRevisions{currentHash: "x", targetHash: "x", fromIR: true}, map[string]int32{"x": 1}, map[string]int32{"x": 1}},
		"no replica observation":     {observedCanaryRevisions{currentHash: "x", targetHash: "x", statusFresh: true}, map[string]int32{"x": 1}, map[string]int32{"x": 1}},
	} {
		if got := settledRevision(tc.observed, tc.total, tc.ready); got != "" {
			t.Errorf("%s: settledRevision = %q, want nothing", name, got)
		}
	}
}
