package canary

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A traffic target names a revision only while that revision has a serving
// pod. Two transitions can leave a split standing over pods that are gone: a
// re-target of a live ladder, whose superseded canary is replaced under its
// weight while the new revision stages, and a re-target from the done
// sentinel, whose pre-canary stable has rolled onto the finished canary by
// the time the new step's remainder is written. The stories below walk the
// passes of each transition and check every write against the pass's own
// pod view.

// expectTrafficServes asserts every weighted target of comp names a revision
// with a serving pod in readyPods. A Component with no targets written passes:
// nothing is routed to nothing.
func expectTrafficServes(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, readyPods map[string]int32) {
	t.Helper()
	traffic := isvc.Status.Components[comp].Traffic
	for _, target := range traffic {
		if target.Percent <= 0 {
			continue
		}
		hash := query.RevisionFromName(target.RevisionName).Hash()
		if readyPods[hash] <= 0 {
			t.Errorf("%s routes %d%% to %q, which has no serving pod (serving %v); traffic %+v",
				comp, target.Percent, hash, readyPods, traffic)
		}
	}
}

// expectStaging asserts the pass left the unit staging its step at Pending.
func expectStaging(t *testing.T, isvc *v1beta1.InferenceService, steps int) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if got := rollout.StateOf(cs, phaseOf(isvc), steps); got != rollout.CanaryStateStaging {
		t.Fatalf("state %q, want %q (phase %q, status %+v)", got, rollout.CanaryStateStaging, phaseOf(isvc), cs)
	}
}

// twoReplicaSplit is the manual two-step ladder on a two-replica Component
// at its first step: one instance held on the stable revision, one serving
// the canary, the split programmed under the manual hold.
func twoReplicaSplit(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	t.Helper()
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 1})
	in.DesiredReplicas = 2
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 50, latest: true},
		"old": {percent: 50},
	})
	return isvc, in
}

// A re-target while the split serves: the superseded canary's weight returns
// to the stable revision on the pass that re-arms, stays there while that
// canary's only pod is replaced and the new revision is not yet Ready, and
// the new step's gate restores the split on the revision that now serves.
func TestReconcile_RetargetMidSplitReturnsTrafficToStable(t *testing.T) {
	isvc, in := twoReplicaSplit(t)

	in.CanaryRevisionHash, in.TargetID = "newer", "t2"
	mustReconcile(t, isvc, in)
	expectStaging(t, isvc, 2)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)

	// The superseded canary's pod is replaced; its replacement is not Ready.
	in.PerRevisionPods = map[string]int32{"old": 1}
	mustReconcile(t, isvc, in)
	expectStaging(t, isvc, 2)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)

	// The new revision reaches the step's capacity.
	in.PerRevisionPods = map[string]int32{"old": 1, "newer": 1}
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the restored split holds at the manual gate, got %q", phaseOf(isvc))
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"newer": {percent: 50, latest: true},
		"old":   {percent: 50},
	})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
}

// pdMember is the decoder's revision pair as the dispatcher resolves it for
// the traffic writer.
func pdMember(target string, readyCapacity int32) MemberRevisions {
	return MemberRevisions{CanaryRevisionHash: target, StableRevisionHash: "decold", ReadyCanaryCapacity: readyCapacity, DesiredReplicas: 2}
}

// setPDReadyPods writes the primary's and the group's ready pod views: the
// engine's ready pods are the primary view and the engine entry of the group
// view alike.
func setPDReadyPods(in *ReconcileInputs, engine, decoder map[string]int32) {
	in.PerRevisionPods = engine
	in.GroupReadyPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent:  engine,
		v1beta1.DecoderComponent: decoder,
	}
}

// The same re-target in a two-member unit: every member's superseded weight
// returns to its own stable revision, the whole unit waits for every
// member's new capacity, and the gate restores every member's split.
func TestReconcile_RetargetMidSplitReturnsEveryMemberToStable(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "decnew", StableRevision: "decold"},
	)
	in := baseInputs(isvc, nil)
	in.DesiredReplicas = 2
	in.TargetID = "t1"
	in.GroupStableRevisionHashes = map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "old", v1beta1.DecoderComponent: "decold"}
	in.Secondaries = map[v1beta1.ComponentType]MemberRevisions{v1beta1.DecoderComponent: pdMember("decnew", 1)}
	setPDReadyPods(&in, map[string]int32{"new": 1, "old": 1}, map[string]int32{"decnew": 1, "decold": 1})
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 50, latest: true}, "old": {percent: 50}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnew": {percent: 50, latest: true}, "decold": {percent: 50}})

	// Both members re-target; neither new revision has capacity yet.
	in.CanaryRevisionHash, in.TargetID = "newer", "t2"
	in.Secondaries[v1beta1.DecoderComponent] = pdMember("decnewer", 0)
	in.SecondaryCapacityReady = false
	mustReconcile(t, isvc, in)
	expectStaging(t, isvc, 2)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100}})

	// The superseded pods of both members are replaced before anything new is Ready.
	setPDReadyPods(&in, map[string]int32{"old": 1}, map[string]int32{"decold": 1})
	mustReconcile(t, isvc, in)
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
	expectTrafficServes(t, isvc, v1beta1.DecoderComponent, in.GroupReadyPerRevisionPods[v1beta1.DecoderComponent])

	// The engine's new pod is Ready first; the split waits for the decoder.
	setPDReadyPods(&in, map[string]int32{"old": 1, "newer": 1}, map[string]int32{"decold": 1})
	mustReconcile(t, isvc, in)
	expectStaging(t, isvc, 2)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100}})

	// The decoder's new capacity is up: the split is restored on every member.
	setPDReadyPods(&in, map[string]int32{"old": 1, "newer": 1}, map[string]int32{"decold": 1, "decnewer": 1})
	in.Secondaries[v1beta1.DecoderComponent] = pdMember("decnewer", 1)
	in.SecondaryCapacityReady = true
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"newer": {percent: 50, latest: true}, "old": {percent: 50}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnewer": {percent: 50, latest: true}, "decold": {percent: 50}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
	expectTrafficServes(t, isvc, v1beta1.DecoderComponent, in.GroupReadyPerRevisionPods[v1beta1.DecoderComponent])
}

// doneRollingTwoReplicas drives the two-replica ladder to its done sentinel:
// the final step shifted 100% onto the canary with no drain window, the
// floor is released and its instance, still serving the pre-canary stable,
// has begun its roll.
func doneRollingTwoReplicas(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	t.Helper()
	isvc, in := twoReplicaSplit(t)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	mustReconcile(t, isvc, in) // 100% traffic moves; the release reads the next pass's count
	res := mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 2 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("the final step must set the done sentinel with the floor still rolling, got step %d phase %q res %+v", cs.CurrentStep, phaseOf(isvc), res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
	return isvc, in
}

// A re-target from the done sentinel: the released floor rolls onto the
// finished canary, so nothing serves the pre-canary stable the new ladder
// still records as its rollback target. The new step's remainder is written
// on the revision that serves in that stable's place, and the identity the
// rollback returns to is left as it is.
func TestReconcile_RetargetAfterCutoverSplitsOnTheRevisionThatServes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		atRetarget    map[string]int32
		afterRetarget map[string]trafficWant
	}{
		{
			name:          "the floor has finished rolling when the target lands",
			atRetarget:    map[string]int32{"new": 2},
			afterRetarget: map[string]trafficWant{"new": {percent: 100}},
		},
		{
			name:          "the floor is still rolling when the target lands",
			atRetarget:    map[string]int32{"new": 1, "old": 1},
			afterRetarget: map[string]trafficWant{"old": {percent: 100}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in := doneRollingTwoReplicas(t)

			in.CanaryRevisionHash, in.TargetID = "newer", "t2"
			in.PerRevisionPods = tc.atRetarget
			mustReconcile(t, isvc, in)
			expectStaging(t, isvc, 2)
			expectTargets(t, isvc, v1beta1.EngineComponent, tc.afterRetarget)
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)

			// The floor's roll lands: nothing serves the pre-canary stable, so
			// the finished canary carries the whole remainder in its place.
			in.PerRevisionPods = map[string]int32{"new": 2}
			mustReconcile(t, isvc, in)
			expectStaging(t, isvc, 2)
			expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100}})
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)

			// The other instance rolls onto the new target and meets the step's capacity.
			in.PerRevisionPods = map[string]int32{"new": 1, "newer": 1}
			mustReconcile(t, isvc, in)
			if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
				t.Fatalf("the split holds at the manual gate, got %q", phaseOf(isvc))
			}
			expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
				"newer": {percent: 50, latest: true},
				"new":   {percent: 50},
			})
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
			if got := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).StableRevisionHash; got != "old" {
				t.Fatalf("the traffic substitute must not rewrite the rollback identity, got %q", got)
			}
		})
	}
}
