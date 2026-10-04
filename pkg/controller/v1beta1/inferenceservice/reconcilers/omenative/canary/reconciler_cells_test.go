package canary

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The canary state × event grid, one row per authored cell. A row's id is
// canary/<State>/<event>; its fixture drives Reconcile into the state, its
// event is one input change, and its want is the decided outcome. A row
// that disagrees with the code is a finding about one or the other, never
// something to paper over in the fixture.
type cell struct {
	id      string
	fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)
	event   func(isvc *v1beta1.InferenceService, in *ReconcileInputs)
	want    cellWant
}

type cellWant struct {
	state    rollout.CanaryState
	phase    v1beta1.RolloutPhase // empty: not asserted
	step     int32
	active   bool
	rolledBk bool
	complete bool
	consume  []string // annotation keys the pass hands back for removal
}

const cellReadyTimeout = 15 * time.Minute

func mustReconcile(t *testing.T, isvc *v1beta1.InferenceService, in ReconcileInputs) *Result {
	t.Helper()
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// twoStepTimed is a timed soak then full traffic; twoStep() is the manual
// ladder.
func twoStepTimed(d time.Duration) []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: d}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

func fixtureIdle(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := canaryISVC(twoStep(), nil)
	runWithTargets(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"})
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash, in.StableRevisionHash, in.TargetID = "old", "old", "t-idle"
	return isvc, in
}

func fixtureStaging(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 4})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // arms and parks on the capacity gate
	return isvc, in
}

func fixtureServingManual(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureStaging(t)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	mustReconcile(t, isvc, in) // gate met, split serving, manual hold
	return isvc, in
}

func fixtureServingTimed(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixturePreHold(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureServingManual(t)
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).PreStepHold = true
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDraining is the final step at 100% traffic inside its drain window.
// The step holds the last stable instance, so three instances are Ready on
// the canary revision and one still serves the stable one.
func fixtureDraining(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(twoStep(), i32(3600)))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the promote advances to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in) // 100% traffic, drain window open
	return isvc, in
}

// fixtureDrainingHeld is the final step at 100% traffic with a manual gate
// still to open: the drain waits on the operator.
func fixtureDrainingHeld(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in) // 100% traffic, the final gate holds
	return isvc, in
}

// fixtureDrainingShort is the final step held on capacity past its drain
// window: 100% traffic landed on three canary instances, the window elapsed,
// and a canary pod is gone when the release would be read. The stable
// instance stays held until the capacity is back.
func fixtureDrainingShort(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	in.Now = in.Now.Add(2 * time.Hour)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDrainingShortInWindow is the final step held on capacity inside its
// drain window: 100% traffic landed and a canary pod went before the window
// elapsed.
func fixtureDrainingShortInWindow(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	in.Now = in.Now.Add(time.Minute)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureFailedAtTheRelease is the final step parked on the ready timeout of
// its capacity wait: the stable instance is still held and carries the
// traffic again.
func fixtureFailedAtTheRelease(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDrainingShort(t)
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDoneRolling is the done sentinel with the released stable instance
// still rolling: the drain elapsed, the floor is gone, one instance still
// serves the stable revision.
func fixtureDoneRolling(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	in.Now = in.Now.Add(2 * time.Hour)
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDone is the completed cutover: every instance serves the canary
// revision and the unit reads Stable.
func fixtureDone(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDoneRolling(t)
	in.PerRevisionPods = map[string]int32{"new": 4}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureRollingBack(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureServingManual(t)
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureRolledBack(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureRollingBack(t)
	in.PerRevisionPods = map[string]int32{"old": 4}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureFailed(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureStaging(t)
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureFailedStableMissing is a rollback that found no stable revision to
// return to. The park is decided where the revision history is read, in
// Dispatch, so the fixture records it the way Dispatch does; the request is
// gone the way the controller's removal after the park's flush leaves it.
func fixtureFailedStableMissing(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureRollingBack(t)
	parkFailed(isvc, v1beta1.EngineComponent, rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent), v1beta1.CanaryFailureStableRevisionMissing, in.Now)
	delete(isvc.Annotations, constants.RolloutRollbackAnnotation)
	return isvc, in
}

// fixtureRollingBackPD is a rollback in a two-member unit (engine primary,
// decoder secondary) with both members still serving their rejected
// revisions. The primary view carries the engine's ready pods; the group view
// carries every pod of both members, ready or not, and their stable identities.
func fixtureRollingBackPD(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "dec-new", StableRevision: "dec-old"},
	)
	in := baseInputs(isvc, nil)
	in.TargetID = "t1"
	in.GroupStableRevisionHashes = map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "old", v1beta1.DecoderComponent: "dec-old"}
	setGroupPods(&in, map[string]int32{"new": 2, "old": 2}, nil, map[string]int32{"dec-new": 2, "dec-old": 2})
	mustReconcile(t, isvc, in) // arms, gate met, split serving, manual hold
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// setGroupPods writes the engine's ready pods (the primary view) and every
// pod of the engine and the decoder (the group view). A nil engineTotal means
// every engine pod is ready.
func setGroupPods(in *ReconcileInputs, engineReady, engineTotal, decoderTotal map[string]int32) {
	if engineTotal == nil {
		engineTotal = engineReady
	}
	in.PerRevisionPods = engineReady
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent:  engineTotal,
		v1beta1.DecoderComponent: decoderTotal,
	}
}

func annotate(isvc *v1beta1.InferenceService, key, value string) {
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[key] = value
}

func evPause(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.PausedRolloutAnnotation, "true")
}
func evPromoteMatch(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	annotate(isvc, constants.RolloutPromoteAnnotation, in.CanaryRevisionHash)
}
func evPromoteStale(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutPromoteAnnotation, "stale")
}
func evRollback(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
}

// evResumeOwn names the unit's own canary revision; evResumeStale names the
// stable revision, which no canary carries.
func evResumeOwn(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutResumeAnnotation, rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CanaryRevisionHash)
}
func evResumeStale(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutResumeAnnotation, "old")
}
func evNewTarget(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.CanaryRevisionHash, in.TargetID = "newer", "t2"
	in.PerRevisionPods = map[string]int32{"old": 4}
}

// evNewTargetAtRunOpen presents the new target the way the controller does:
// the run layer reopens the run toward it and BindRun re-arms the unit before
// the executor's pass, so the pass sees the run's target as its own.
func evNewTargetAtRunOpen(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "newer", StableRevision: "old"},
	}
	g := rollout.CanaryGroup(isvc)
	if err := BindRun(context.Background(), nil, isvc, g, false); err != nil {
		panic(err)
	}
	in.CanaryRevisionHash, in.TargetID = "newer", activeCanaryTargetID(isvc, g)
	in.PerRevisionPods = map[string]int32{"old": 4}
}

// The rejected revision is still draining when the new target appears.
func evNewTargetWhileDraining(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evNewTarget(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
}
func evNewTargetAtRunOpenWhileDraining(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evNewTargetAtRunOpen(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
}

// evOtherGroupRun: another group's retarget opened a fresh run for the whole
// service. The run layer pins a unit holding a rejected revision at its
// stable revision, so the unit's TargetID changes while its IR target is
// still the rejected revision and BindRun leaves it alone.
func evOtherGroupRun(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: "rnew", StableRevision: "rold"},
		{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"},
	}
	in.TargetID = activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc))
}
func evReplicasUp(_ *v1beta1.InferenceService, in *ReconcileInputs) { in.DesiredReplicas = 8 }
func evCapacityDropped(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 2}
}
func evDrainingCapacityDropped(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
}

// evDrainingCapacityDroppedPastTheWindow: the drain window has elapsed and a
// canary pod is gone when the release would be read.
func evDrainingCapacityDroppedPastTheWindow(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evClockElapsed(isvc, in)
	evDrainingCapacityDropped(isvc, in)
}

// evDrainingCapacityReached: every staged canary instance serves again.
func evDrainingCapacityReached(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
}

// evReplacementReady: the released instance's replacement is Ready on the
// canary revision while the stable instance it replaces still serves.
func evReplacementReady(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 4, "old": 1}
}

// evLastInstanceRolled: nothing serves the stable revision any more.
func evLastInstanceRolled(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 4}
}

// evCanaryPodLost: a canary pod is lost after the cutover completed.
func evCanaryPodLost(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 3}
}
func evCapacityReached(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
}
func evReadyTimeout(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
}
func evClockElapsed(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Hour)
}

// evRestartInSoak: the soak since the split first served has elapsed, but a
// canary pod died and came back half a soak ago.
func evRestartInSoak(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Hour)
	in.CanaryRestartedAt = in.Now.Add(-30 * time.Minute)
}

// evRestartSoaked: a canary pod died and came back inside the step, and has
// served a full soak since.
func evRestartSoaked(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Hour)
	in.CanaryRestartedAt = in.Now.Add(-61 * time.Minute)
}

// evPromoteAfterRestartInSoak: the operator promotes while the restarted
// canary pod is still inside its soak.
func evPromoteAfterRestartInSoak(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evRestartInSoak(isvc, in)
	evPromoteMatch(isvc, in)
}

// evCanaryRestarted: a canary pod died and came back a minute into the
// step; the kubelet has it serving again.
func evCanaryRestarted(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.CanaryRestartedAt = in.Now
	in.Now = in.Now.Add(time.Minute)
}
func evRejectedPodsGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"old": 4}
}

// evPrimaryReverted: the engine is back on stable while a decoder pod still
// exists on the decoder's rejected revision; the group view counts it whether
// or not it is ready.
func evPrimaryReverted(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	setGroupPods(in, map[string]int32{"old": 4}, nil, map[string]int32{"dec-old": 3, "dec-new": 1})
}
func evGroupReverted(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	setGroupPods(in, map[string]int32{"old": 4}, nil, map[string]int32{"dec-old": 4})
}

// evPrimaryRejectedPodUnready: every ready engine pod is on stable, but an
// engine pod on the rejected revision still exists, not ready.
func evPrimaryRejectedPodUnready(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	setGroupPods(in, map[string]int32{"old": 4}, map[string]int32{"old": 4, "new": 1}, map[string]int32{"dec-old": 4})
}

// evMemberStableUnknown: the decoder still has a rejected pod but no stable
// identity on record, so it is sent no revert signal and is not waited on.
func evMemberStableUnknown(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evPrimaryReverted(isvc, in)
	in.GroupStableRevisionHashes[v1beta1.DecoderComponent] = ""
}

// evCanaryCrashed: a canary pod is in a crash loop after its Instance was
// serving. The kubelet has it back up for the moment, so the capacity counts
// are unchanged.
func evCanaryCrashed(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Crash = &CanaryCrash{Component: v1beta1.EngineComponent, PodName: "svc-engine-3-default-0", Detail: "Error (exit 1)"}
}

// evCanaryPodDeleted: a canary pod is deleted by hand a minute into the
// step. The pass reads the pod on its way out, its runner stopped by the
// kubelet, and sees the dip.
func evCanaryPodDeleted(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(time.Minute)
	in.Crash = deletedCanaryPodReading(isvc, in.Now)
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 2}
}
func evRunLost(_ *v1beta1.InferenceService, in *ReconcileInputs) { in.RunActive = false }
func evRunLostAfterSoak(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.RunActive = false
	in.Now = in.Now.Add(2 * time.Hour)
}
func evNothing(*v1beta1.InferenceService, *ReconcileInputs) {}

func TestReconcileCells(t *testing.T) {
	const (
		idle        = rollout.CanaryStateIdle
		staging     = rollout.CanaryStateStaging
		serving     = rollout.CanaryStateServing
		preHold     = rollout.CanaryStatePreHold
		draining    = rollout.CanaryStateDraining
		done        = rollout.CanaryStateDone
		rollingBack = rollout.CanaryStateRollingBack
		rolledBack  = rollout.CanaryStateRolledBack
		failed      = rollout.CanaryStateFailed
	)
	cells := []cell{
		// pause: the executor observes and writes nothing.
		{"canary/Idle/pause", fixtureIdle, evPause, cellWant{state: idle}},
		{"canary/Staging/pause", fixtureStaging, evPause, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/pause", fixtureServingManual, evPause, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/pause", fixturePreHold, evPause, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining/pause", fixtureDraining, evPause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/pause", fixtureDrainingShort, evPause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Failed[release]/pause", fixtureFailedAtTheRelease, evPause, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Done/pause", fixtureDone, evPause, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/pause", fixtureDoneRolling, evPause, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2}},
		{"canary/RollingBack/pause", fixtureRollingBack, evPause, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/pause", fixtureRolledBack, evPause, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/pause", fixtureFailed, evPause, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// promote
		{"canary/Idle/promote", fixtureIdle, evPromoteMatch, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Staging/promote", fixtureStaging, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/promote[match]", fixtureServingManual, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving/promote[stale]", fixtureServingManual, evPromoteStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/promote", fixtureServingTimed, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/PreHold/promote", fixturePreHold, evPromoteMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		// An ungated final step has no gate for a promote to open: the hold on capacity keeps
		// it live, unconsumed, for the release to record.
		{"canary/Draining[capacityShort]/promote", fixtureDrainingShort, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Done/promote", fixtureDone, evPromoteMatch, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/RolledBack/promote", fixtureRolledBack, evPromoteMatch, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/promote", fixtureFailed, evPromoteMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[release]/promote", fixtureFailedAtTheRelease, evPromoteMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// rollback
		{"canary/Idle/rollback", fixtureIdle, evRollback, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable, consume: []string{constants.RolloutRollbackAnnotation}}},
		{"canary/Staging/rollback", fixtureStaging, evRollback, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Serving/rollback", fixtureServingManual, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/PreHold/rollback", fixturePreHold, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Draining/rollback", fixtureDraining, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[capacityShort]/rollback", fixtureDrainingShort, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Done/rollback", fixtureDone, evRollback, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutRollbackAnnotation}}},
		{"canary/Done[rolling]/rollback", fixtureDoneRolling, evRollback, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutRollbackAnnotation}}},
		{"canary/RolledBack/rollback", fixtureRolledBack, evRollback, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/rollback", fixtureFailed, evRollback, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// the park at the release still has canary pods to drain, so its rollback drains first
		{"canary/Failed[release]/rollback", fixtureFailedAtTheRelease, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		// A park for a missing stable revision already carries the rejection the
		// request asked for: the request is refused and handed back, the park unmoved.
		{"canary/Failed[stableMissing]/rollback", fixtureFailedStableMissing, evRollback, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutRollbackAnnotation}}},
		// resume: honored at a park it names; everywhere else, and for a hash
		// no canary carries, consumed and ignored.
		{"canary/Idle/resume", fixtureIdle, evResumeStale, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Staging/resume", fixtureStaging, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Staging/resume[stale]", fixtureStaging, evResumeStale, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving/resume", fixtureServingManual, evResumeOwn, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving/resume[stale]", fixtureServingManual, evResumeStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/PreHold/resume", fixturePreHold, evResumeOwn, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining/resume", fixtureDraining, evResumeOwn, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining[capacityShort]/resume", fixtureDrainingShort, evResumeOwn, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Done/resume", fixtureDone, evResumeOwn, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Done/resume[stale]", fixtureDone, evResumeStale, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RollingBack/resume", fixtureRollingBack, evResumeOwn, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RolledBack/resume", fixtureRolledBack, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RolledBack/resume[stale]", fixtureRolledBack, evResumeStale, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed/resume", fixtureFailed, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed/resume[stale]", fixtureFailed, evResumeStale, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed[stableMissing]/resume", fixtureFailedStableMissing, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed[release]/resume", fixtureFailedAtTheRelease, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		// new target
		{"canary/Staging/newTarget", fixtureStaging, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/newTarget", fixtureServingManual, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/PreHold/newTarget", fixturePreHold, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining/newTarget", fixtureDraining, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining[capacityShort]/newTarget", fixtureDrainingShort, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Done/newTarget", fixtureDone, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Done[rolling]/newTarget", fixtureDoneRolling, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RollingBack/newTarget", fixtureRollingBack, evNewTargetWhileDraining, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RollingBack/newTarget[runOpen]", fixtureRollingBack, evNewTargetAtRunOpenWhileDraining, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RolledBack/newTarget", fixtureRolledBack, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RolledBack/newTarget[runOpen]", fixtureRolledBack, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		// A run another group opened offers the held unit no target: the hold stays.
		{"canary/RollingBack/newTarget[otherGroup]", fixtureRollingBack, evOtherGroupRun, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/newTarget[otherGroup]", fixtureRolledBack, evOtherGroupRun, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/newTarget", fixtureFailed, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Failed[stableMissing]/newTarget", fixtureFailedStableMissing, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Failed[release]/newTarget", fixtureFailedAtTheRelease, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		// replicas: more capacity to stage holds the step where it is.
		{"canary/Serving/replicasUp", fixtureServingManual, evReplicasUp, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		// run lost: the canary holds where it is; even an elapsed soak does not advance it.
		// A revert keeps draining (the run layer adopts it in place) and a settled hold stays settled.
		{"canary/Serving/runLost", fixtureServingManual, evRunLost, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/runLost", fixtureServingTimed, evRunLostAfterSoak, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining[capacityShort]/runLost", fixtureDrainingShort, evRunLost, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/RollingBack/runLost", fixtureRollingBack, evRunLost, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/runLost", fixtureRolledBack, evRunLost, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// capacity reached
		{"canary/Staging/capacityReached", fixtureStaging, evCapacityReached, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Failed/capacityReached", fixtureFailed, evCapacityReached, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// the capacity a held release waited for is back: the pass reads it and releases
		{"canary/Draining[capacityShort]/capacityReached", fixtureDrainingShort, evDrainingCapacityReached, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		// inside the window the capacity back resumes the drain; a park is not re-evaluated by it
		{"canary/Draining[capacityShort,inWindow]/capacityReached", fixtureDrainingShortInWindow, evDrainingCapacityReached, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Failed[release]/capacityReached", fixtureFailedAtTheRelease, evDrainingCapacityReached, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// capacity dropped: a serving split keeps its phase and weight; a drain runs on, and past
		// its window the release waits for the capacity with the stable instance still held.
		{"canary/Serving/capacityDropped", fixtureServingManual, evCapacityDropped, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/capacityDropped", fixturePreHold, evCapacityDropped, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining/capacityDropped", fixtureDraining, evDrainingCapacityDropped, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/capacityDropped[drainElapsed]", fixtureDraining, evDrainingCapacityDroppedPastTheWindow, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		// a canary pod lost after the cutover completed is the workload's repair, not a canary state
		{"canary/Done/capacityDropped", fixtureDone, evCanaryPodLost, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		// canary crashed: a pod that served and keeps dying parks a live ladder at its step, whatever
		// the counts say now; a hold, a revert and a finished cutover are not moved by it.
		{"canary/Idle/canaryCrashed", fixtureIdle, evCanaryCrashed, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Staging/canaryCrashed", fixtureStaging, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving/canaryCrashed", fixtureServingManual, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[T]/canaryCrashed", fixtureServingTimed, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/PreHold/canaryCrashed", fixturePreHold, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Draining/canaryCrashed", fixtureDraining, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[capacityShort]/canaryCrashed", fixtureDrainingShort, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Failed[release]/canaryCrashed", fixtureFailedAtTheRelease, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Done/canaryCrashed", fixtureDone, evCanaryCrashed, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/canaryCrashed", fixtureDoneRolling, evCanaryCrashed, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/RollingBack/canaryCrashed", fixtureRollingBack, evCanaryCrashed, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/canaryCrashed", fixtureRolledBack, evCanaryCrashed, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/canaryCrashed", fixtureFailed, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// canary pod deleted: a pod on its way out is a loss, so the step keeps its phase and
		// weight like any dip, and a held split stays held.
		{"canary/Staging/canaryPodDeleted", fixtureStaging, evCanaryPodDeleted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/canaryPodDeleted", fixtureServingManual, evCanaryPodDeleted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/canaryPodDeleted", fixturePreHold, evCanaryPodDeleted, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		// rejected pods gone
		{"canary/RollingBack/rejectedPodsGone", fixtureRollingBack, evRejectedPodsGone, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// rejected pods gone in a two-member unit: the rollback completes for the group, not the primary
		// alone, and a rejected pod holds it for as long as it exists, ready or not.
		{"canary/RollingBack[PD]/resync", fixtureRollingBackPD, evNothing, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/primaryReverted", fixtureRollingBackPD, evPrimaryReverted, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/primaryRejectedPodUnready", fixtureRollingBackPD, evPrimaryRejectedPodUnready, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/groupReverted", fixtureRollingBackPD, evGroupReverted, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/memberStableUnknown", fixtureRollingBackPD, evMemberStableUnknown, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// ready timeout
		{"canary/Staging/readyTimeout", fixtureStaging, evReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// a held release's capacity wait runs the same ready timeout, from the wait, inside or past
		// the drain window: the park keeps the final step and the held stable instance, and returns
		// the traffic to it
		{"canary/Draining[capacityShort]/readyTimeout", fixtureDrainingShort, evReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/readyTimeout", fixtureDrainingShortInWindow, evReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// pause elapsed
		{"canary/Serving[T]/pauseElapsed", fixtureServingTimed, evClockElapsed, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		// a canary pod died and came back: a timed soak measures from the newest restart, so the
		// step moves only once the restarted pod has served a full soak; a manual hold is unmoved
		// by it, and a promote is the operator's decision on any gate.
		{"canary/Serving[T]/pauseElapsed[restartInSoak]", fixtureServingTimed, evRestartInSoak, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/pauseElapsed[restartSoaked]", fixtureServingTimed, evRestartSoaked, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Serving[T]/promote[restartInSoak]", fixtureServingTimed, evPromoteAfterRestartInSoak, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving/canaryRestarted", fixtureServingManual, evCanaryRestarted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Staging/canaryRestarted", fixtureStaging, evCanaryRestarted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		// a held final gate: the drain waits, the phase says so
		{"canary/Draining[M]/gateHolds", fixtureDrainingHeld, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/promote", fixtureDrainingHeld, evPromoteMatch, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		// drain elapsed: the done sentinel releases the held stable instance; the unit is not
		// Stable until that instance has rolled
		{"canary/Draining/drainElapsed", fixtureDraining, evClockElapsed, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		// the released instance rolls: the replacement serving is not enough while the stable
		// instance still serves; the pass that finds nothing on the stable revision completes
		{"canary/Done[rolling]/replacementReady", fixtureDoneRolling, evReplacementReady, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/lastInstanceRolled", fixtureDoneRolling, evLastInstanceRolled, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, active: true, complete: true}},
		// nothing changed
		{"canary/Staging/resync", fixtureStaging, evNothing, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/resync", fixtureServingManual, evNothing, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining/resync", fixtureDraining, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/resync", fixtureDrainingShort, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/resync", fixtureDrainingShortInWindow, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Failed[release]/resync", fixtureFailedAtTheRelease, evNothing, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Done/resync", fixtureDone, evNothing, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/resync", fixtureDoneRolling, evNothing, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/RolledBack/resync", fixtureRolledBack, evNothing, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/resync", fixtureFailed, evNothing, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/resync", fixtureFailedStableMissing, evNothing, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
	}
	for _, c := range cells {
		t.Run(c.id, func(t *testing.T) {
			isvc, in := c.fixture(t)
			c.event(isvc, &in)
			res := mustReconcile(t, isvc, in)
			cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
			steps := len(isvc.Spec.Rollout.Groups[0].Canary.Steps)
			if got := rollout.StateOf(cs, phaseOf(isvc), steps); got != c.want.state {
				t.Errorf("state %q, want %q", got, c.want.state)
			}
			if c.want.phase != "" && phaseOf(isvc) != c.want.phase {
				t.Errorf("phase %q, want %q", phaseOf(isvc), c.want.phase)
			}
			if cs != nil && cs.CurrentStep != c.want.step {
				t.Errorf("step %d, want %d", cs.CurrentStep, c.want.step)
			}
			if len(c.want.consume) > 0 || len(res.Consume) > 0 {
				got := append([]string(nil), res.Consume...)
				if len(got) != len(c.want.consume) {
					t.Errorf("consume %v, want %v", got, c.want.consume)
				} else {
					for i := range got {
						if got[i] != c.want.consume[i] {
							t.Errorf("consume %v, want %v", got, c.want.consume)
							break
						}
					}
				}
			}
			if res.Active != c.want.active || res.RolledBack != c.want.rolledBk || res.Complete != c.want.complete {
				t.Errorf("result active=%v rolledBack=%v complete=%v, want active=%v rolledBack=%v complete=%v",
					res.Active, res.RolledBack, res.Complete, c.want.active, c.want.rolledBk, c.want.complete)
			}
			// Whatever the cell decided, every weighted target it leaves behind
			// names a revision with a serving pod in the pass's own view.
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
			// RollingBack is a wait on the drain: the pass must ask to come back.
			if c.want.state == rollingBack && res.RequeueAfter == 0 && !res.Requeue {
				t.Errorf("RollingBack must requeue, got %+v", res)
			}
			// A released cutover still rolling is a wait on the workload, which moves no
			// annotation and no step: the pass must ask to come back, at partition 0.
			if c.want.state == done && c.want.active && !c.want.complete {
				if res.RequeueAfter == 0 && !res.Requeue {
					t.Errorf("a rolling Done must requeue, got %+v", res)
				}
				if res.Partition != 0 {
					t.Errorf("a rolling Done projects partition 0, got %d", res.Partition)
				}
			}
		})
	}
}
