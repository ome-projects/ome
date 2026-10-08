package canary

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A traffic target names a revision only while that revision has a serving
// pod. A canary step whose revision loses its last serving pod keeps its
// state and its soak, but the split it writes rests the canary's share on
// the stable revision until a pod of the canary revision serves again; the
// share returns on the pass that sees one, met capacity or not. The stories
// below lose every canary pod at each step of a ladder, on one member and
// across a two-member unit, and check every write against the pass's own
// pod view.

// fourStepLadder soaks a tenth, then three tenths, then six tenths of the
// traffic before the rest follows.
func fourStepLadder(soak time.Duration) []v1beta1.RolloutGroupStep {
	timed := func() *v1beta1.RolloutPause {
		return &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: soak}}
	}
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("10%"), Traffic: 10, Pause: timed()},
		{Capacity: intstr.FromString("30%"), Traffic: 30, Pause: timed()},
		{Capacity: intstr.FromString("60%"), Traffic: 60, Pause: timed()},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// expectStepKept asserts the step machine stands where it was: the step,
// the phase, the recorded weight and the soak anchor.
func expectStepKept(t *testing.T, isvc *v1beta1.InferenceService, step int32, phase v1beta1.RolloutPhase, weight int32, soak time.Time, when string) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != step || phaseOf(isvc) != phase || cs.ObservedTrafficWeight != weight {
		t.Fatalf("%s: the step must keep its state, got step %d phase %q weight %d, want step %d phase %q weight %d",
			when, cs.CurrentStep, phaseOf(isvc), cs.ObservedTrafficWeight, step, phase, weight)
	}
	if cs.StepEnteredTime == nil || !cs.StepEnteredTime.Time.Equal(soak) {
		t.Fatalf("%s: the soak anchor must stand at %v, got %v", when, soak, cs.StepEnteredTime)
	}
}

// soakAnchor is the moment the step's soak measures from.
func soakAnchor(isvc *v1beta1.InferenceService) time.Time {
	return rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).StepEnteredTime.Time
}

// The only canary pod of the first step dies under a restart policy that
// rebuilds its Instance: while the replacement boots the split rests on the
// stable revision, the step keeps its phase, weight and soak, and the share
// returns on the pass that sees the replacement serve.
func TestReconcile_SplitFallsBackWhileTheCanaryRevisionHasNoServingPod(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(fourStepLadder(20*time.Second), nil))
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 7})
	in.DesiredReplicas = 8
	in.Now = t0
	mustReconcile(t, isvc, in) // the split serves at 10%; the soak anchors here
	soak := soakAnchor(isvc)
	tenth := map[string]trafficWant{"new": {percent: 10, latest: true}, "old": {percent: 90}}
	expectTargets(t, isvc, v1beta1.EngineComponent, tenth)

	// Ten seconds in the pod dies; the policy rebuilds the Instance and the
	// replacement is not serving yet.
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 7}
	in.CanaryRestartedAt = t0.Add(10 * time.Second)
	in.Now = t0.Add(10 * time.Second)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
	expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 10, soak, "the pod is gone")
	if w := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CapacityWaitSince; w == nil || !w.Time.Equal(in.Now) {
		t.Fatalf("the loss must record when the capacity wait began, got %v", w)
	}

	// The replacement is still booting on the next pass: the fallback stands.
	in.Now = t0.Add(20 * time.Second)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 10, soak, "the replacement is booting")

	// The replacement serves: the share returns, and the step soaks it.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 7}
	in.CanaryRestartedAt = t0.Add(30 * time.Second)
	in.Now = t0.Add(30 * time.Second)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, tenth)
	expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 10, soak, "the replacement serves")
	if w := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CapacityWaitSince; w != nil {
		t.Fatalf("the capacity wait must clear once the step's capacity is back, got %v", w)
	}

	// The soak measures from the replacement serving: one second short
	// holds, a full soak advances under the standing share.
	in.Now = t0.Add(49 * time.Second)
	mustReconcile(t, isvc, in)
	expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 10, soak, "one second short of a soak since the replacement served")
	in.Now = t0.Add(50 * time.Second)
	res := mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending || !res.Stepped {
		t.Fatalf("a full soak since the replacement served must advance, got step %d phase %q result %+v", cs.CurrentStep, phaseOf(isvc), res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, tenth)
}

// A step whose capacity is more than one Instance loses every canary pod:
// the share returns on the pass that sees the first of them serve again,
// before the step's capacity is back, and the capacity wait keeps counting
// until it is.
func TestReconcile_ShareReturnsOnTheFirstServingCanaryPod(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Now = t0
	mustReconcile(t, isvc, in)
	soak := soakAnchor(isvc)
	half := map[string]trafficWant{"new": {percent: 50, latest: true}, "old": {percent: 50}}
	expectTargets(t, isvc, v1beta1.EngineComponent, half)

	in.PerRevisionPods = map[string]int32{"new": 0, "old": 2}
	in.Now = t0.Add(time.Minute)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 50, soak, "every canary pod is gone")

	in.PerRevisionPods = map[string]int32{"new": 1, "old": 2}
	in.Now = t0.Add(2 * time.Minute)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, half)
	expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 50, soak, "one canary pod serves")
	if w := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CapacityWaitSince; w == nil {
		t.Fatal("one pod short of the step's capacity, the capacity wait must keep counting")
	}

	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	in.Now = t0.Add(3 * time.Minute)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, half)
	if w := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CapacityWaitSince; w != nil {
		t.Fatalf("the capacity wait must clear once the step's capacity is back, got %v", w)
	}
}

// A promote issued while the split rests on the stable revision waits for a
// canary pod, as any step move under a capacity wait does; the pass that
// sees one serve honors it, and the next step stages under the standing
// share. A loss while that step stages rests the standing share on the
// stable revision too, and the step's own gate writes its share once it
// passes.
func TestReconcile_PromoteDuringTheFallbackAdvancesOnceACanaryPodServes(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	half := map[string]trafficWant{"new": {percent: 50, latest: true}, "old": {percent: 50}}
	expectTargets(t, isvc, v1beta1.EngineComponent, half)

	in.PerRevisionPods = map[string]int32{"new": 0, "old": 2}
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})

	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	res := mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if len(res.Consume) != 0 || cs.CurrentStep != 0 || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("a promote waits out the capacity wait, got step %d phase %q consume %v", cs.CurrentStep, phaseOf(isvc), res.Consume)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})

	// The canary pods serve again: the promote lands and the final step
	// stages under the share the first step wrote back.
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	res = mustReconcile(t, isvc, in)
	cs = rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending || len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteAnnotation {
		t.Fatalf("the promote must advance once a canary pod serves, got step %d phase %q consume %v", cs.CurrentStep, phaseOf(isvc), res.Consume)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, half)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)

	// Every canary pod is lost while the final step stages.
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 2}
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
	cs = rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending || cs.ObservedTrafficWeight != 50 {
		t.Fatalf("a loss between steps keeps the staging step and its recorded weight, got step %d phase %q weight %d", cs.CurrentStep, phaseOf(isvc), cs.ObservedTrafficWeight)
	}

	// The final step's capacity, all but the held floor, serves on the
	// target: the pass moves traffic onto it, and the next pass's own count
	// releases the floor with the target alone.
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in)
	cs = rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("the final step moves traffic before the release, got step %d phase %q", cs.CurrentStep, phaseOf(isvc))
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
	res = mustReconcile(t, isvc, in)
	if cs.CurrentStep != 2 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("the ungated final step cuts over and rolls the floor, got step %d phase %q result %+v", cs.CurrentStep, phaseOf(isvc), res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
	in.PerRevisionPods = map[string]int32{"new": 4}
	if res = mustReconcile(t, isvc, in); !res.Complete || phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("the rolled floor completes the cutover, got phase %q result %+v", phaseOf(isvc), res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
}

// A rollback issued while the split rests on the stable revision lands as
// any rollback does: the stable revision owns the traffic, the rejected
// revision is recorded and gets nothing back when its pods return, and the
// unit holds RolledBack once nothing serves the rejected revision.
func TestReconcile_RollbackDuringTheFallbackHoldsOnStable(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	mustReconcile(t, isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 2}
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})

	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	res := mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if !res.RolledBack || cs.RolledBackRevisionHash != "new" {
		t.Fatalf("the request must roll the unit back, got result %+v status %+v", res, cs)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)

	// The rebuilt canary pod serves again under the revert: it drains.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 2}
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack || weightOn(isvc, v1beta1.EngineComponent, "new") != 0 {
		t.Fatalf("a rejected revision gets no traffic back, got phase %q traffic %+v", phaseOf(isvc), isvc.Status.Components[v1beta1.EngineComponent].Traffic)
	}
	in.PerRevisionPods = map[string]int32{"old": 4}
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("the revert completes once nothing serves the rejected revision, got %q", phaseOf(isvc))
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
}

// A pre-step hold keeps its hold through the loss and the return of the
// canary pods: the split rests on the stable revision while none serves,
// returns to the held weight once one does, capacity met or not, and the
// promote that releases the hold raises it from there.
func TestReconcile_PreHoldSplitFollowsTheCanaryPods(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	mustReconcile(t, isvc, in)
	half := map[string]trafficWant{"new": {percent: 50, latest: true}, "old": {percent: 50}}
	expectTargets(t, isvc, v1beta1.EngineComponent, half)
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).PreStepHold = true
	mustReconcile(t, isvc, in)
	expectState := func(want rollout.CanaryState, when string) {
		t.Helper()
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != want {
			t.Fatalf("%s: state %q, want %q (phase %q, weight %d)", when, got, want, phaseOf(isvc), cs.ObservedTrafficWeight)
		}
	}
	expectState(rollout.CanaryStatePreHold, "the hold is set")

	in.PerRevisionPods = map[string]int32{"new": 0, "old": 2}
	mustReconcile(t, isvc, in)
	expectState(rollout.CanaryStatePreHold, "every canary pod is gone")
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	if w := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).ObservedTrafficWeight; w != 50 {
		t.Fatalf("the hold keeps its recorded weight, got %d", w)
	}

	// The pods return with the step's capacity met: the hold still waits,
	// and the split it holds is back on the canary revision.
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	mustReconcile(t, isvc, in)
	expectState(rollout.CanaryStatePreHold, "the pods are back")
	expectTargets(t, isvc, v1beta1.EngineComponent, half)

	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	expectState(rollout.CanaryStateServing, "the promote released the hold")
	expectTargets(t, isvc, v1beta1.EngineComponent, half)
}

// At every step of the ladder, the final one included, the loss of every
// canary pod rests the split on the stable revision while it serves, and
// the return of one pod restores the step's share; with nothing serving on
// either revision the split stands as programmed, there being no serving
// revision to move it to.
func TestReconcile_FallbackNeverRoutesToARevisionWithNoServingPodAtAnyStep(t *testing.T) {
	ladder := fourStepLadder(20 * time.Second)
	const replicas = 8
	for step := range ladder {
		t.Run(fmt.Sprintf("step %d at %d%%", step, ladder[step].Traffic), func(t *testing.T) {
			isvc := pinActiveRun(canaryISVC(fourStepLadder(20*time.Second), i32(3600)))
			in := baseInputs(isvc, nil)
			in.DesiredReplicas = replicas
			in.Now = time.Unix(100000, 0)
			// Walk the ladder to the step: each step serves its capacity,
			// and each earlier one soaks and moves on.
			var staged int32
			for i := 0; i <= step; i++ {
				staged = stepStagedCount(ladder[i], replicas)
				in.PerRevisionPods = map[string]int32{"new": staged, "old": replicas - staged}
				mustReconcile(t, isvc, in)
				if i < step {
					in.Now = in.Now.Add(21 * time.Second)
					mustReconcile(t, isvc, in)
				}
			}
			share := ladder[step].Traffic
			cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
			if cs.CurrentStep != int32(step) || cs.ObservedTrafficWeight != share {
				t.Fatalf("the ladder must serve step %d at %d%%, got step %d weight %d phase %q", step, share, cs.CurrentStep, cs.ObservedTrafficWeight, phaseOf(isvc))
			}
			phase, soak := phaseOf(isvc), soakAnchor(isvc)
			served := map[string]trafficWant{"new": {percent: share, latest: true}}
			if share < 100 {
				served["old"] = trafficWant{percent: 100 - share}
			}
			expectTargets(t, isvc, v1beta1.EngineComponent, served)

			// Every canary pod is lost while the stable side serves.
			in.PerRevisionPods = map[string]int32{"new": 0, "old": replicas - staged}
			in.Now = in.Now.Add(5 * time.Second)
			mustReconcile(t, isvc, in)
			expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
			expectStepKept(t, isvc, int32(step), phase, share, soak, "every canary pod is lost")

			// Nothing serves on either revision: the split stands as programmed.
			in.PerRevisionPods = map[string]int32{"new": 0, "old": 0}
			in.Now = in.Now.Add(5 * time.Second)
			mustReconcile(t, isvc, in)
			expectTargets(t, isvc, v1beta1.EngineComponent, served)
			expectStepKept(t, isvc, int32(step), phase, share, soak, "nothing serves")

			// One canary pod serves again: the step's share returns.
			in.PerRevisionPods = map[string]int32{"new": 1, "old": replicas - staged}
			in.Now = in.Now.Add(5 * time.Second)
			mustReconcile(t, isvc, in)
			expectTargets(t, isvc, v1beta1.EngineComponent, served)
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
			expectStepKept(t, isvc, int32(step), phase, share, soak, "one canary pod serves")
		})
	}
}

// heldFinalStepWithFallback is the four-instance manual ladder at its final
// step with 100% traffic moved onto three canary instances and every canary
// pod gone before the release: the held stable instance is the one serving,
// and the split rests on it.
func heldFinalStepWithFallback(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	t.Helper()
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // the split serves under the manual hold
	promoteToFinalStep(t, isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	in.Now = in.Now.Add(time.Second)
	res := mustReconcile(t, isvc, in) // 100% moves; the release waits for its own count
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Partition != 1 {
		t.Fatalf("the pass that moves traffic holds the stable instance, got step %d phase %q result %+v", cs.CurrentStep, phaseOf(isvc), res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})

	in.PerRevisionPods = map[string]int32{"new": 0, "old": 1}
	in.Now = in.Now.Add(time.Second)
	res = mustReconcile(t, isvc, in)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Partition != 1 || cs.CapacityWaitSince == nil || cs.ObservedTrafficWeight != 100 {
		t.Fatalf("the drain holds the stable instance with its recorded weight and a capacity wait, got step %d phase %q result %+v status %+v", cs.CurrentStep, phaseOf(isvc), res, cs)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
	return isvc, in
}

// The held final step: the split rests on the held stable instance while no
// canary pod serves, returns to the canary revision on the first pod that
// does while the release still waits for the rest, and the release reads
// the full capacity on its own pass.
func TestReconcile_HeldFinalStepSplitRestsOnTheHeldStableInstance(t *testing.T) {
	isvc, in := heldFinalStepWithFallback(t)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)

	// The hold stands on the next pass with the split where it rests.
	in.Now = in.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, in)
	if cs.CurrentStep != 1 || res.Partition != 1 || cs.Failed != nil {
		t.Fatalf("the hold stands inside the ready timeout, got step %d result %+v status %+v", cs.CurrentStep, res, cs)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})

	// One canary pod serves again: the split returns to the canary revision
	// while the release still waits for the other two.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Second)
	res = mustReconcile(t, isvc, in)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Partition != 1 || cs.CapacityWaitSince == nil {
		t.Fatalf("one pod short of the capacity keeps the hold, got step %d phase %q result %+v status %+v", cs.CurrentStep, phaseOf(isvc), res, cs)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)

	// Every canary instance serves: the release reads the capacity.
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	in.Now = in.Now.Add(time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
	in.PerRevisionPods = map[string]int32{"new": 4}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// The ready timeout parks the held final step while the split already rests
// on the held stable instance: the park's write and the fallback agree, the
// recorded weight reads nothing, and the park keeps that split whatever
// serves afterwards.
func TestReconcile_HeldFinalStepTimeoutParkAgreesWithTheFallback(t *testing.T) {
	isvc, in := heldFinalStepWithFallback(t)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	waitSince := cs.CapacityWaitSince.Time

	in.Now = waitSince.Add(in.DefaultReadyTimeout / 2)
	res := mustReconcile(t, isvc, in)
	if cs.Failed != nil || cs.CurrentStep != 1 || res.Partition != 1 || cs.ObservedTrafficWeight != 100 {
		t.Fatalf("the hold keeps its recorded weight inside the timeout, got result %+v status %+v", res, cs)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})

	in.Now = waitSince.Add(in.DefaultReadyTimeout)
	res = mustReconcile(t, isvc, in)
	if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout || phaseOf(isvc) != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 1 {
		t.Fatalf("the hold must park at the final step on the ready timeout, got phase %q status %+v", phaseOf(isvc), cs)
	}
	if !res.Active || res.Partition != 1 || res.RequeueAfter != failedRequeue {
		t.Fatalf("the park keeps the stable instance held at the parked cadence, got %+v", res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	if cs.ObservedTrafficWeight != 0 {
		t.Fatalf("the park records nothing on the canary revision, got %d", cs.ObservedTrafficWeight)
	}

	// A canary pod serving again does not move a park's split.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	expectUnitState(t, isvc, 2, rollout.CanaryStateFailed, "after a canary pod returned")
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
	if cs.ObservedTrafficWeight != 0 {
		t.Fatalf("a park is not re-evaluated, got weight %d", cs.ObservedTrafficWeight)
	}
}

// In a two-member unit the canary path runs through every member: the loss
// of one member's last canary pod rests every member's split on its stable
// revision, the primary's included, and the return of that pod restores
// every member's share.
func TestReconcile_UnitFallsBackWhenAnyMemberLosesItsCanaryPods(t *testing.T) {
	engineServes := map[string]int32{"new": 1, "old": 1}
	decoderServes := map[string]int32{"decnew": 1, "decold": 1}
	for _, tc := range []struct {
		name    string
		engine  map[string]int32
		decoder map[string]int32
	}{
		{"the decoder's canary pod", engineServes, map[string]int32{"decold": 1}},
		{"the engine's canary pod", map[string]int32{"old": 1}, decoderServes},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			setPDReadyPods(&in, engineServes, decoderServes)
			mustReconcile(t, isvc, in)
			engineHalf := map[string]trafficWant{"new": {percent: 50, latest: true}, "old": {percent: 50}}
			decoderHalf := map[string]trafficWant{"decnew": {percent: 50, latest: true}, "decold": {percent: 50}}
			expectTargets(t, isvc, v1beta1.EngineComponent, engineHalf)
			expectTargets(t, isvc, v1beta1.DecoderComponent, decoderHalf)
			soak := soakAnchor(isvc)

			setPDReadyPods(&in, tc.engine, tc.decoder)
			in.Secondaries[v1beta1.DecoderComponent] = pdMember("decnew", tc.decoder["decnew"])
			in.SecondaryCapacityReady = tc.decoder["decnew"] > 0
			mustReconcile(t, isvc, in)
			expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
			expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100}})
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
			expectTrafficServes(t, isvc, v1beta1.DecoderComponent, in.GroupReadyPerRevisionPods[v1beta1.DecoderComponent])
			expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 50, soak, "a member lost its canary pod")

			setPDReadyPods(&in, engineServes, decoderServes)
			in.Secondaries[v1beta1.DecoderComponent] = pdMember("decnew", 1)
			in.SecondaryCapacityReady = true
			mustReconcile(t, isvc, in)
			expectTargets(t, isvc, v1beta1.EngineComponent, engineHalf)
			expectTargets(t, isvc, v1beta1.DecoderComponent, decoderHalf)
			expectStepKept(t, isvc, 0, v1beta1.RolloutPhasePaused, 50, soak, "the pod serves again")
		})
	}
}

// setServing marks one instance's pod of a Component as serving or not.
func (f *pdCanary) setServing(comp, index string, serving bool) {
	f.t.Helper()
	pod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.ns, Name: f.name + "-" + comp + "-" + index}, pod); err != nil {
		f.t.Fatal(err)
	}
	if serving {
		readyAgain(pod)
	} else {
		notReady(pod)
	}
	if err := f.c.Status().Update(context.Background(), pod); err != nil {
		f.t.Fatal(err)
	}
}

// The same loss read from the pods by the dispatcher: the decoder's canary
// pod stops serving, every member's split rests on its stable revision
// under that revision's protocol, and the pod serving again restores every
// member's share with the step still held.
func TestDispatch_UnitFallsBackWhileASecondaryCanaryPodIsNotServing(t *testing.T) {
	f := newPDCanary(t, "pdfall", true)
	f.dispatch("serve")
	engineHalf := map[string]trafficWant{
		"engnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"engold": {percent: 50, protocol: pdStableProtocol},
	}
	decoderHalf := map[string]trafficWant{
		"decnew": {percent: 50, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 50, protocol: pdStableProtocol},
	}
	f.expectTraffic(v1beta1.EngineComponent, engineHalf)
	f.expectTraffic(v1beta1.DecoderComponent, decoderHalf)

	f.setServing("decoder", "1", false)
	f.dispatch("the decoder's canary pod stops serving")
	if f.phase() != v1beta1.RolloutPhasePaused {
		t.Fatalf("the step keeps its hold through the loss, got %q", f.phase())
	}
	f.expectTraffic(v1beta1.EngineComponent, map[string]trafficWant{"engold": {percent: 100, protocol: pdStableProtocol}})
	f.expectTraffic(v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})

	f.setServing("decoder", "1", true)
	f.dispatch("the pod serves again")
	if f.phase() != v1beta1.RolloutPhasePaused {
		t.Fatalf("the step keeps its hold through the return, got %q", f.phase())
	}
	f.expectTraffic(v1beta1.EngineComponent, engineHalf)
	f.expectTraffic(v1beta1.DecoderComponent, decoderHalf)
}

// Read from the pods and the Instance rows: both canary Instances of the
// timed step are rebuilt at once, the split rests on the stable revision
// while the replacements boot, and the first replacement to serve restores
// the step's share with the step still soaking.
func TestDispatch_RebuiltCanaryPodsRestTheSplitOnStableUntilOneServes(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)
	half := map[string]trafficWant{"target": {percent: 50, latest: true}, "stable": {percent: 50}}
	expectTargets(t, w.isvc, v1beta1.EngineComponent, half)

	w.rebuildInstance(t, "target-2", 2, t0.Add(10*time.Second), t0.Add(11*time.Second))
	w.rebuildInstance(t, "target-3", 3, t0.Add(10*time.Second), t0.Add(11*time.Second))
	w.dispatchAt(t, t0.Add(12*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused || cs.ObservedTrafficWeight != 50 {
		t.Fatalf("the rebuilds keep the step and its recorded weight, got step %d phase %q weight %d", cs.CurrentStep, phase, cs.ObservedTrafficWeight)
	}
	expectTargets(t, w.isvc, v1beta1.EngineComponent, map[string]trafficWant{"stable": {percent: 100}})

	w.rebuiltServes(t, "target-2", 2, t0.Add(30*time.Second))
	w.dispatchAt(t, t0.Add(31*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the first replacement serving restores the share under the hold, got step %d phase %q", cs.CurrentStep, phase)
	}
	expectTargets(t, w.isvc, v1beta1.EngineComponent, half)
}

// The canary's share stands while the canary path serves, and while the
// stable side has nothing to carry it; it falls back to nothing only when
// the path is broken and the stable side serves.
func TestServedWeight(t *testing.T) {
	for _, tc := range []struct {
		name       string
		weight     int32
		pathServes bool
		stable     string
		pods       map[string]int32
		want       int32
	}{
		{"the canary path serves", 30, true, "old", map[string]int32{"new": 1, "old": 1}, 30},
		{"the path is broken and the stable side serves", 30, false, "old", map[string]int32{"old": 1}, 0},
		{"the path is broken and the stable side has no pod", 30, false, "old", map[string]int32{"old": 0}, 30},
		{"the path is broken with no pod view", 30, false, "old", nil, 30},
		{"the path is broken with no stable side", 30, false, "", map[string]int32{"new": 0}, 30},
		{"a substitute for the stable revision carries the share", 30, false, "mid", map[string]int32{"mid": 2}, 0},
		{"the final step falls back like any other", 100, false, "old", map[string]int32{"old": 1}, 0},
		{"a step with no share has none to move", 0, false, "old", map[string]int32{"old": 1}, 0},
		// The rule is symmetric: a stable side with no serving pod carries no
		// weight while the canary path serves, and the target carries the rest.
		{"the path serves and the stable side has no pod", 30, true, "old", map[string]int32{"new": 1, "old": 0}, 100},
		{"the path serves and the stable side is absent from the pod view", 30, true, "old", map[string]int32{"new": 1}, 100},
		{"the path serves with no pod view", 30, true, "old", nil, 30},
		{"the path serves with no stable side", 30, true, "", map[string]int32{"new": 1}, 30},
		{"a substitute serving in the stable's place keeps the step's share", 30, true, "mid", map[string]int32{"new": 1, "mid": 1}, 30},
		{"a step with no share hands it all to the only serving side", 0, true, "old", map[string]int32{"new": 1, "old": 0}, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := servedWeight(tc.weight, tc.pathServes, tc.stable, tc.pods); got != tc.want {
				t.Fatalf("servedWeight = %d, want %d", got, tc.want)
			}
		})
	}
}

// The canary path serves while every bumped member has a serving pod on its
// target: the primary's loss and a secondary's loss each break it, an
// unbumped member and a member with no pod view cannot, and a member with
// no stable revision breaks it like any other once its target has no pod.
func TestCanaryPathServes(t *testing.T) {
	unit := func(engine map[string]int32, decoder MemberRevisions, decoderPods map[string]int32) ReconcileInputs {
		in := ReconcileInputs{CanaryRevisionHash: "new", StableRevisionHash: "old", PerRevisionPods: engine}
		if decoder.CanaryRevisionHash != "" {
			in.Secondaries = map[v1beta1.ComponentType]MemberRevisions{v1beta1.DecoderComponent: decoder}
			in.GroupReadyPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{v1beta1.DecoderComponent: decoderPods}
		}
		return in
	}
	for _, tc := range []struct {
		name string
		in   ReconcileInputs
		want bool
	}{
		{"the primary's target serves", unit(map[string]int32{"new": 1, "old": 1}, MemberRevisions{}, nil), true},
		{"the primary's target has no pod", unit(map[string]int32{"old": 1}, MemberRevisions{}, nil), false},
		{"the primary has no pod view", unit(nil, MemberRevisions{}, nil), true},
		{"an unbumped primary", ReconcileInputs{CanaryRevisionHash: "same", StableRevisionHash: "same", PerRevisionPods: map[string]int32{"other": 1}}, true},
		{"every member serves", unit(map[string]int32{"new": 1, "old": 1}, bumpedDecoder(), map[string]int32{"decnew": 1, "decold": 1}), true},
		{"a secondary's target has no pod", unit(map[string]int32{"new": 1, "old": 1}, bumpedDecoder(), map[string]int32{"decold": 1}), false},
		{"a secondary has no pod view", unit(map[string]int32{"new": 1, "old": 1}, bumpedDecoder(), nil), true},
		{"an unbumped secondary with no pod", unit(map[string]int32{"new": 1, "old": 1},
			MemberRevisions{CanaryRevisionHash: "decold", StableRevisionHash: "decold"}, map[string]int32{"decold": 0}), true},
		{"a secondary with no stable revision and no target pod", unit(map[string]int32{"new": 1, "old": 1},
			MemberRevisions{CanaryRevisionHash: "decnew"}, map[string]int32{"decold": 1}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := canaryPathServes(tc.in); got != tc.want {
				t.Fatalf("canaryPathServes = %v, want %v", got, tc.want)
			}
		})
	}
}

// A bumped member whose unit's canary path is broken writes its share on
// its stable revision while that revision serves, and keeps it on its target
// while the stable revision has no pod; an unbumped member and a member with
// no stable revision serve their target alone either way.
func TestMemberWeightsFallBack(t *testing.T) {
	bumped := bumpedDecoder()
	if w := memberWeights(bumped, 30, false, map[string]int32{"decold": 1}); len(w) != 2 ||
		w[0].RevisionHash != "decnew" || w[0].Percent != 0 || w[1].RevisionHash != "decold" || w[1].Percent != 100 {
		t.Fatalf("a broken path rests the member's share on its serving stable revision, got %+v", w)
	}
	if w := memberWeights(bumped, 30, false, map[string]int32{"decold": 0}); len(w) != 2 ||
		w[0].RevisionHash != "decnew" || w[0].Percent != 30 || w[1].RevisionHash != "decold" || w[1].Percent != 70 {
		t.Fatalf("a stable revision with no pod leaves the member's share standing, got %+v", w)
	}
	if w := memberWeights(bumped, 30, false, map[string]int32{"decmid": 1}); len(w) != 2 ||
		w[0].RevisionHash != "decnew" || w[0].Percent != 0 || w[1].RevisionHash != "decmid" || w[1].Percent != 100 {
		t.Fatalf("a broken path rests the member's share on the revision serving in its stable's place, got %+v", w)
	}
	if w := memberWeights(bumped, 30, true, map[string]int32{"decnew": 1, "decold": 0}); len(w) != 2 ||
		w[0].RevisionHash != "decnew" || w[0].Percent != 100 || w[1].RevisionHash != "decold" || w[1].Percent != 0 {
		t.Fatalf("a stable revision with no pod while the target serves hands the member's share to the target, got %+v", w)
	}
	if w := memberWeights(MemberRevisions{CanaryRevisionHash: "d", StableRevisionHash: "d"}, 30, false, map[string]int32{"d": 1}); len(w) != 1 || w[0].Percent != 100 {
		t.Fatalf("an unbumped member serves its revision alone, got %+v", w)
	}
	if w := memberWeights(MemberRevisions{CanaryRevisionHash: "decnew"}, 30, false, map[string]int32{"decold": 1}); len(w) != 1 || w[0].RevisionHash != "decnew" || w[0].Percent != 100 {
		t.Fatalf("a member with no stable revision has nothing to fall back to, got %+v", w)
	}
}
