package canary

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A step's traffic never moves onto capacity the step has not observed Ready
// on the pods of its own pass, and the release of the held stable instance
// is read the same way: the pass that moves traffic to 100% ends there, and
// a later pass writes the done sentinel only on canary capacity it has
// itself counted. A cutover whose canary capacity vanished between the two
// holds in the drain with the stable instance still held, until the capacity
// is back or the ready timeout parks it with the traffic returned to that
// instance.

// twoInstanceManualLadder is the manual two-step ladder on a two-instance
// Component, brought to its first step with the split serving: one instance
// held on the stable revision, one serving the canary.
func twoInstanceManualLadder(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	t.Helper()
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"old": 2})
	in.DesiredReplicas = 2
	in.TargetID = "t1"
	if res := mustReconcile(t, isvc, in); res.Partition != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("staging must hold one stable instance, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	if res := mustReconcile(t, isvc, in); phaseOf(isvc) != v1beta1.RolloutPhasePaused || res.Partition != 1 {
		t.Fatalf("the split must serve under the manual hold, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 50, latest: true},
		"old": {percent: 50},
	})
	return isvc, in
}

// promoteToFinalStep applies the operator's promote the way the controller
// does: the pass advances and records it, and the annotation is removed
// after the flush.
func promoteToFinalStep(t *testing.T, isvc *v1beta1.InferenceService, in ReconcileInputs) {
	t.Helper()
	annotate(isvc, constants.RolloutPromoteAnnotation, in.CanaryRevisionHash)
	res := mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if !res.Stepped || cs.CurrentStep != 1 || len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteAnnotation {
		t.Fatalf("the promote must advance to the final step and be handed back, got res=%+v status=%+v", res, cs)
	}
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
}

// expectHeldAtFinalStep asserts a pass that holds the final step without
// releasing the stable instance: the step index and the held partition are
// unchanged, no sentinel is written, and the pass asks to come back.
func expectHeldAtFinalStep(t *testing.T, isvc *v1beta1.InferenceService, res *Result, phase v1beta1.RolloutPhase) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 {
		t.Fatalf("the hold must keep the final step, not write the sentinel; got step %d (status %+v)", cs.CurrentStep, cs)
	}
	if !res.Active || res.Complete || res.Partition != 1 {
		t.Fatalf("the hold must keep the stable instance held at partition 1, got %+v", res)
	}
	if res.RequeueAfter == 0 && !res.Requeue {
		t.Fatalf("the hold must ask to come back, got %+v", res)
	}
	if phaseOf(isvc) != phase {
		t.Fatalf("phase %q, want %q", phaseOf(isvc), phase)
	}
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2)}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("the projected partition must keep the stable instance, got %v", fmtPartition(p))
	}
}

// Promote consumed while the canary's only serving pod is gone: the step
// holds on capacity, the split it inherited rests on the stable instance
// while no canary pod serves, and the ladder resumes once capacity is back.
// The resume moves traffic to 100% on the capacity the pass counted and
// leaves the release to a later pass with its own count.
func TestReconcile_PromoteConsumedWhileTheOnlyCanaryPodIsGone(t *testing.T) {
	isvc, in := twoInstanceManualLadder(t)
	promoteToFinalStep(t, isvc, in)

	// The only canary pod is gone when the final step first runs: the step
	// holds on capacity with the weight it inherited, and the split rests
	// on the stable instance until a canary pod serves.
	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePending)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.ObservedTrafficWeight != 50 || cs.CapacityWaitSince == nil {
		t.Fatalf("the hold keeps the recorded weight and records the capacity wait, got weight %d wait %v", cs.ObservedTrafficWeight, cs.CapacityWaitSince)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})

	// Capacity is back: the step's traffic moves onto it. The pass that
	// moved it does not also release the stable instance.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
	if cs.CapacityWaitSince != nil {
		t.Fatalf("capacity back clears the wait, got %v", cs.CapacityWaitSince)
	}

	// The next pass counts the capacity again and releases.
	in.Now = in.Now.Add(time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	// The released instance rolls: the cutover is complete.
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// The cutover re-reads canary capacity before releasing the stable instance
// and holds when the pods are gone: the pass that moves traffic to 100% on a
// count the pod loss had not reached yet writes no sentinel, and the pass
// that finds the canary pod gone holds the drain with the stable instance
// still held until the capacity is back.
func TestReconcile_CutoverReReadsCapacityBeforeReleasingTheStableInstance(t *testing.T) {
	isvc, in := twoInstanceManualLadder(t)
	promoteToFinalStep(t, isvc, in)

	// The pass still counts the canary pod: traffic moves to 100%, the
	// stable instance stays held, no sentinel.
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	// The pod is gone before the release: the cutover holds on capacity.
	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CapacityWaitSince == nil {
		t.Fatal("the hold records the capacity wait")
	}
	waitSince := cs.CapacityWaitSince.Time

	// The wait keeps its anchor: a later pass of the hold neither re-stamps
	// it nor parks before the ready timeout has run from it.
	in.Now = in.Now.Add(in.DefaultReadyTimeout / 2)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	if cs.Failed != nil || !cs.CapacityWaitSince.Time.Equal(waitSince) {
		t.Fatalf("a drain's capacity wait keeps its anchor inside the timeout, got failed=%+v wait=%v", cs.Failed, cs.CapacityWaitSince)
	}

	// Capacity is back: the release reads it and the ladder finishes.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	if cs.CapacityWaitSince != nil {
		t.Fatalf("the release clears the capacity wait, got %v", cs.CapacityWaitSince)
	}
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// A deleted pod still in the cache is not capacity: a canary pod the pass
// reads Running and Ready but under deletion counts for nothing in the
// capacity gate, so the final step holds rather than moving traffic onto
// it, and the split it inherited rests on the stable instance, the pod on
// its way out serving nothing.
func TestDispatch_ADeletedPodStillInTheCacheIsNotCapacity(t *testing.T) {
	ns := "default"
	n2 := 2
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace, isvc.Name = ns, "deleting"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	g := rollout.CanaryGroup(isvc)
	start := time.Unix(1000, 0)
	// The final step was just entered: the split served 50/50 at step 0.
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID:              activeCanaryTargetID(isvc, g),
		CanaryRevisionHash:    "new",
		StableRevisionHash:    "old",
		CurrentStep:           1,
		StepEnteredTime:       &metav1.Time{Time: start},
		ObservedTrafficWeight: 50,
	})
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePending)
	setTraffic(isvc, v1beta1.EngineComponent, coordination.BuildTrafficTargets(isvc.Name, v1beta1.EngineComponent,
		canaryWeights("new", "old", "", "", 50)))

	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-old"
	// The canary's only pod is on its way out: the deletion has reached the
	// controller while the kubelet still reports it Running and Ready.
	deletedAt := metav1.NewTime(start)
	leaving := canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-1")
	leaving.DeletionTimestamp = &deletedAt
	leaving.Finalizers = []string{"test/hold"}
	objs := []runtime.Object{isvc, engineIR,
		canaryPod(ns, isvc.Name, "engine", "old", isvc.Name+"-engine-0"),
		leaving,
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "new", 2),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: g, Now: start.Add(time.Minute)}

	out, err := Dispatch(context.Background(), deps)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePending {
		t.Fatalf("a canary pod under deletion is not capacity; the step must hold, got phase %q", got)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 || cs.ObservedTrafficWeight != 50 || cs.CapacityWaitSince == nil {
		t.Fatalf("the hold keeps the step and its recorded weight and records the wait, got %+v", cs)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	ext := &isvc.Spec.Engine.ComponentExtensionSpec
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("the stable instance stays held, got partition %v", fmtPartition(p))
	}
	if out.RequeueAfter == 0 && !out.Requeue {
		t.Fatalf("the hold must ask to come back, got %+v", out)
	}
}

// heldFinalStep is the final step held on capacity after its traffic write
// landed: the two-instance manual ladder promoted to the final step, 100%
// traffic moved onto the one canary instance, and that instance's only pod
// gone before the release. The stable instance is still held under it.
func heldFinalStep(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	t.Helper()
	isvc, in := twoInstanceManualLadder(t)
	promoteToFinalStep(t, isvc, in)
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.CapacityWaitSince == nil {
		t.Fatal("the hold records the capacity wait")
	}
	return isvc, in
}

// expectUnitState asserts the unit's state on the canary grid.
func expectUnitState(t *testing.T, isvc *v1beta1.InferenceService, steps int, want rollout.CanaryState, when string) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if got := rollout.StateOf(cs, phaseOf(isvc), steps); got != want {
		t.Fatalf("%s: state %q, want %q (phase %q, status %+v)", when, got, want, phaseOf(isvc), cs)
	}
}

// A held final step whose capacity has not returned is left by the
// operator's rollback like any live step: the request is taken from the hold, the
// stable instance it returns to was never released, and the traffic goes
// back onto it while the rejected revision drains.
func TestReconcile_HeldFinalStepRollsBackOnRequest(t *testing.T) {
	isvc, in := heldFinalStep(t)

	// Inside the ready timeout the capacity has not come back: the hold
	// stands with the stable instance still held.
	in.Now = in.Now.Add(in.DefaultReadyTimeout / 2)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.Failed != nil {
		t.Fatalf("the hold does not park, got %+v", cs.Failed)
	}

	// A replacement pod on the rejected revision is still coming up, which
	// the group view counts ready or not, so the revert drains first.
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent: {"new": 1, "old": 1},
	}
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	in.Now = in.Now.Add(time.Second)
	res := mustReconcile(t, isvc, in)
	if !res.RolledBack || cs.RolledBackRevisionHash != "new" || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rollback must leave the hold and drain the rejected revision, got res=%+v phase=%q status=%+v", res, phaseOf(isvc), cs)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")

	// The rejected pod is gone: the revert is complete.
	in.GroupTotalPerRevisionPods[v1beta1.EngineComponent] = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	res = mustReconcile(t, isvc, in)
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("the revert completes once nothing runs the rejected revision, got res=%+v phase=%q", res, phaseOf(isvc))
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
}

// A held final step is left by a new push like any live step: the unit
// re-arms at step 0 toward the new target, keeps the pre-canary stable
// identity, and the split it owned returns to the stable revision.
func TestReconcile_HeldFinalStepReArmsOnANewPush(t *testing.T) {
	isvc, in := heldFinalStep(t)
	in.CanaryRevisionHash, in.TargetID = "newer", "t2"
	in.Now = in.Now.Add(time.Second)
	res := mustReconcile(t, isvc, in)
	expectUnitState(t, isvc, 2, rollout.CanaryStateStaging, "after the push")
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 0 || cs.CanaryRevisionHash != "newer" || cs.StableRevisionHash != "old" || cs.TargetID != "t2" {
		t.Fatalf("the re-arm starts the ladder over toward the new target, got %+v", cs)
	}
	if !res.Active || res.Partition != 1 {
		t.Fatalf("the first step stages against the held stable instance, got %+v", res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
}

// The crash rules read the held final step like any live step: a canary pod
// that keeps dying parks the ladder at the final step with the traffic back
// on the stable revision and the stable instance still held, and the park
// keeps its exits: a rollback request, a new push, and a resume.
func TestReconcile_HeldFinalStepParksOnACrashLoop(t *testing.T) {
	parked := func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
		t.Helper()
		isvc, in := heldFinalStep(t)
		rec := record.NewFakeRecorder(8)
		in.Recorder = rec
		in.Crash = &CanaryCrash{Component: v1beta1.EngineComponent, PodName: "svc-engine-1", Detail: "container ome-container CrashLoopBackOff (exit 1)"}
		in.Now = in.Now.Add(time.Minute)
		res := mustReconcile(t, isvc, in)
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout || phaseOf(isvc) != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 1 {
			t.Fatalf("a crash loop parks the held final step, got phase=%q status=%+v", phaseOf(isvc), cs)
		}
		if !res.Active || res.Partition != 1 || res.RequeueAfter != failedRequeue {
			t.Fatalf("the park keeps the stable instance held at the parked cadence, got %+v", res)
		}
		expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
		select {
		case ev := <-rec.Events:
			if !strings.Contains(ev, EventReasonCanaryPodCrashed) {
				t.Fatalf("the park names the pod that keeps dying, got %q", ev)
			}
		default:
			t.Fatal("the park must be announced")
		}
		in.Crash, in.Recorder = nil, nil
		return isvc, in
	}

	expectParkKeepsItsExits(t, parked)
}

// expectParkKeepsItsExits runs the exits every park at the final step keeps,
// each against a fresh park from parked: the park holds with the stable
// instance, a rollback request ends it, a new push re-arms, a resume
// re-enters the ladder.
func expectParkKeepsItsExits(t *testing.T, parked func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)) {
	t.Helper()
	t.Run("the park holds with the stable instance", func(t *testing.T) {
		isvc, in := parked(t)
		in.Now = in.Now.Add(time.Hour)
		res := mustReconcile(t, isvc, in)
		expectUnitState(t, isvc, 2, rollout.CanaryStateFailed, "an hour into the park")
		if !res.Active || res.RequeueAfter != failedRequeue {
			t.Fatalf("the park holds at its cadence, got %+v", res)
		}
		ext := &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2)}
		if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
			t.Fatalf("the park keeps the stable instance held, got %v", fmtPartition(p))
		}
		expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
	})
	t.Run("a rollback request ends the park", func(t *testing.T) {
		isvc, in := parked(t)
		annotate(isvc, constants.RolloutRollbackAnnotation, "true")
		res := mustReconcile(t, isvc, in)
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if !res.RolledBack || cs.RolledBackRevisionHash != "new" || cs.Failed != nil {
			t.Fatalf("the rollback must end the park and reject the canary revision, got res=%+v status=%+v", res, cs)
		}
		expectUnitState(t, isvc, 2, rollout.CanaryStateRolledBack, "after the rollback")
		expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
	})
	t.Run("a new push re-arms", func(t *testing.T) {
		isvc, in := parked(t)
		in.CanaryRevisionHash, in.TargetID = "newer", "t2"
		mustReconcile(t, isvc, in)
		expectUnitState(t, isvc, 2, rollout.CanaryStateStaging, "after the push")
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if cs.CurrentStep != 0 || cs.CanaryRevisionHash != "newer" || cs.Failed != nil {
			t.Fatalf("the push re-arms at step 0, got %+v", cs)
		}
	})
	t.Run("a resume re-enters the ladder", func(t *testing.T) {
		isvc, in := parked(t)
		annotate(isvc, constants.RolloutResumeAnnotation, "new")
		res := mustReconcile(t, isvc, in)
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation || cs.CurrentStep != 0 || cs.Failed != nil {
			t.Fatalf("the resume re-enters at step 0 and is handed back, got res=%+v status=%+v", res, cs)
		}
		expectUnitState(t, isvc, 2, rollout.CanaryStateStaging, "after the resume")
	})
}

// The held final step is bounded by the ready timeout, measured from the
// capacity wait as the pre-shift wait is: once the canary capacity has not
// returned within it, the canary parks Failed at the final step and the
// traffic goes back to the still-held stable instance.
func TestReconcile_HeldFinalStepTimesOutIntoThePark(t *testing.T) {
	isvc, in := twoInstanceManualLadder(t)
	promoteToFinalStep(t, isvc, in)
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	shifted := in.Now

	// The pod is gone ten minutes into the drain: the wait starts there.
	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = shifted.Add(10 * time.Minute)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	waitSince := cs.CapacityWaitSince.Time

	// The timeout runs from the wait, not from the shift.
	in.Now = shifted.Add(in.DefaultReadyTimeout + time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	if cs.Failed != nil {
		t.Fatalf("the timeout is measured from the capacity wait, got %+v", cs.Failed)
	}
	in.Now = waitSince.Add(in.DefaultReadyTimeout)
	res := mustReconcile(t, isvc, in)
	if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout || !cs.Failed.Time.Time.Equal(in.Now) || phaseOf(isvc) != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 1 {
		t.Fatalf("the hold must park at the final step on the ready timeout, got phase=%q status=%+v", phaseOf(isvc), cs)
	}
	if !res.Active || res.Partition != 1 || res.RequeueAfter != failedRequeue {
		t.Fatalf("the park keeps the stable instance held at the parked cadence, got %+v", res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")

	// The park is not re-evaluated: capacity returning does not release it.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Minute)
	res = mustReconcile(t, isvc, in)
	expectUnitState(t, isvc, 2, rollout.CanaryStateFailed, "after the capacity returned")
	if !res.Active || res.RequeueAfter != failedRequeue || cs.CurrentStep != 1 {
		t.Fatalf("a park is not re-evaluated, got res=%+v status=%+v", res, cs)
	}
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2)}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("the park keeps the stable instance held, got %v", fmtPartition(p))
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
}

// Capacity back before the ready timeout releases as at any other time: the
// pass counts it, writes the sentinel and clears the wait.
func TestReconcile_HeldFinalStepReleasesWhenCapacityReturnsBeforeTheTimeout(t *testing.T) {
	isvc, in := heldFinalStep(t)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = cs.CapacityWaitSince.Time.Add(in.DefaultReadyTimeout - time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	if cs.Failed != nil || cs.CapacityWaitSince != nil {
		t.Fatalf("the release clears the wait and parks nothing, got failed=%+v wait=%v", cs.Failed, cs.CapacityWaitSince)
	}
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// The timeout park at the final step keeps the exits every park has.
func TestReconcile_HeldFinalStepTimeoutParkKeepsItsExits(t *testing.T) {
	expectParkKeepsItsExits(t, func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
		t.Helper()
		isvc, in := heldFinalStep(t)
		in.Now = in.Now.Add(in.DefaultReadyTimeout + time.Second)
		mustReconcile(t, isvc, in)
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout || cs.CurrentStep != 1 {
			t.Fatalf("the hold must have parked on the ready timeout, got %+v", cs)
		}
		expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
		return isvc, in
	})
}

// With no ready timeout configured anywhere the held final step waits as
// the pre-shift wait does: untimed, released by the capacity when it returns.
func TestReconcile_HeldFinalStepIsUntimedWithoutAReadyTimeout(t *testing.T) {
	isvc, in := twoInstanceManualLadder(t)
	in.DefaultReadyTimeout = 0
	promoteToFinalStep(t, isvc, in)
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	in.Now = in.Now.Add(24 * time.Hour)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.Failed != nil || cs.CapacityWaitSince == nil {
		t.Fatalf("an unconfigured timeout never parks the wait, got %+v", cs)
	}
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// A second promote at the held final step changes nothing: an ungated final
// step has no gate left to open, so the promote is neither consumed nor
// recorded and stays live, and the release that reads the capacity back
// records it and hands it back.
func TestReconcile_SecondPromoteAtTheHeldFinalStepWaitsWithIt(t *testing.T) {
	isvc, in := heldFinalStep(t)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	in.Now = in.Now.Add(time.Second)
	res := mustReconcile(t, isvc, in)
	expectHeldAtFinalStep(t, isvc, res, v1beta1.RolloutPhasePromoting)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if len(res.Consume) != 0 || cs.PromotedThrough != "" {
		t.Fatalf("a promote at the held final step stays live and unrecorded, got consume=%v record=%q", res.Consume, cs.PromotedThrough)
	}

	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Second)
	res = mustReconcile(t, isvc, in)
	expectReleased(t, isvc, res, 2)
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteAnnotation || cs.PromotedThrough != "new" {
		t.Fatalf("the release records the live promote and hands it back, got consume=%v record=%q", res.Consume, cs.PromotedThrough)
	}
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
	if cs.PromotedThrough != "" {
		t.Fatalf("the done sentinel clears the record once the annotation is gone, got %q", cs.PromotedThrough)
	}
}

// heldFinalGate is a manual gate on the final step holding for the
// operator with 100% traffic moved and the canary's only pod gone under it:
// the two-instance ladder with a manual hold on both steps, promoted once.
func heldFinalGate(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	t.Helper()
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"old": 2})
	in.DesiredReplicas = 2
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // stages one instance
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	mustReconcile(t, isvc, in) // the split serves under the manual hold
	promoteToFinalStep(t, isvc, in)

	// 100% moves; the final gate holds the drain for the operator.
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePaused)
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	// The canary pod is gone under the held gate.
	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePaused)
	return isvc, in
}

// A manual gate on the final step opens onto counted capacity only: with
// the canary pod gone, the promote that opens the gate holds the release
// with the stable instance still held, and stays live until the release
// reads the capacity back and records it.
func TestReconcile_PromoteOpensAHeldFinalGateOnlyOntoCountedCapacity(t *testing.T) {
	isvc, in := heldFinalGate(t)

	// The promote opens the gate; the release still waits for the capacity
	// and the promote stays live for it.
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	in.Now = in.Now.Add(time.Second)
	res := mustReconcile(t, isvc, in)
	expectHeldAtFinalStep(t, isvc, res, v1beta1.RolloutPhasePromoting)
	if len(res.Consume) != 0 {
		t.Fatalf("the promote stays live until the release, got consume %v", res.Consume)
	}
	in.Now = in.Now.Add(in.DefaultReadyTimeout / 2)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.Failed != nil {
		t.Fatalf("the wait inside the ready timeout is not a park, got %+v", cs.Failed)
	}

	// Capacity is back: the release reads it and records the promote.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Second)
	res = mustReconcile(t, isvc, in)
	expectReleased(t, isvc, res, 2)
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteAnnotation || cs.PromotedThrough != "new" {
		t.Fatalf("the release records the promote and hands it back, got consume=%v record=%q", res.Consume, cs.PromotedThrough)
	}
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// A held final gate is bounded the same way: the capacity wait under a gate
// still holding for the operator runs the ready timeout, and the park
// returns the traffic to the held stable instance.
func TestReconcile_HeldFinalGateTimesOutIntoThePark(t *testing.T) {
	isvc, in := heldFinalGate(t)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	in.Now = cs.CapacityWaitSince.Time.Add(in.DefaultReadyTimeout)
	res := mustReconcile(t, isvc, in)
	if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout || phaseOf(isvc) != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 1 {
		t.Fatalf("the held gate must park on the ready timeout of its capacity wait, got phase=%q status=%+v", phaseOf(isvc), cs)
	}
	if !res.Active || res.Partition != 1 || res.RequeueAfter != failedRequeue {
		t.Fatalf("the park keeps the stable instance held at the parked cadence, got %+v", res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
}

// twoInstanceTimedLadderAtFinalStep brings a timed two-step ladder on a
// two-instance Component through its soak to the final step, entered and
// staging with its capacity already up; finalPause gates the final step.
func twoInstanceTimedLadderAtFinalStep(t *testing.T, finalPause *v1beta1.RolloutPause) (*v1beta1.InferenceService, ReconcileInputs) {
	t.Helper()
	soak := &metav1.Duration{Duration: 20 * time.Second}
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: soak}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: finalPause},
	}
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 1})
	in.DesiredReplicas = 2
	in.TargetID = "t1"
	in.Now = time.Unix(100000, 0)
	mustReconcile(t, isvc, in) // the split serves at 50% and soaks
	in.Now = in.Now.Add(25 * time.Second)
	if res := mustReconcile(t, isvc, in); !res.Stepped || rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CurrentStep != 1 {
		t.Fatalf("the soaked step must advance on its own, got %+v", res)
	}
	return isvc, in
}

// The automatic ladder's final step follows the same rule: the pass that
// moves traffic to 100% ends there, the release reads the capacity on a
// later pass, and a canary pod gone in between holds the release with the
// stable instance until the pod serves again.
func TestReconcile_TimedLadderFinalStepReleasesOnItsOwnCount(t *testing.T) {
	isvc, in := twoInstanceTimedLadderAtFinalStep(t, nil)
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	in.Now = in.Now.Add(in.DefaultReadyTimeout / 2)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.Failed != nil || cs.CapacityWaitSince == nil {
		t.Fatalf("the hold records its wait and does not park inside the timeout, got %+v", cs)
	}

	// The pod serves again. The ungated final step has no soak to measure
	// from the restart, so the release reads the capacity at once.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.CanaryRestartedAt = in.Now
	in.Now = in.Now.Add(time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// A timed gate on the final step opens onto counted capacity only: the soak
// that elapses while the canary pod is gone opens the gate, the release
// waits for the capacity, and the pod that comes back soaks a full window
// again before the release reads it.
func TestReconcile_TimedFinalGateReleasesOnlyOntoCountedCapacity(t *testing.T) {
	isvc, in := twoInstanceTimedLadderAtFinalStep(t, &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 20 * time.Second}})
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePaused) // 100% moves, the gate soaks
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	in.PerRevisionPods = map[string]int32{"old": 1}
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePaused) // the gate holds first
	in.Now = in.Now.Add(30 * time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePromoting) // the gate opened, the release waits
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.Failed != nil {
		t.Fatalf("the wait does not park, got %+v", cs.Failed)
	}

	// The pod serves again: the soak measures from its restart before the
	// gate opens again, and the release then reads the capacity.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.CanaryRestartedAt = in.Now
	in.Now = in.Now.Add(time.Second)
	expectHeldAtFinalStep(t, isvc, mustReconcile(t, isvc, in), v1beta1.RolloutPhasePaused)
	in.Now = in.Now.Add(25 * time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	in.PerRevisionPods = map[string]int32{"new": 2}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}
