package canary

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// A canary pod's deaths after it served are the canary's for as long as its
// run is open, whatever step the ladder has reached: the first restarts the
// soak, the second parks the ladder at the step it is on. The stories below
// drive the four-Instance engine wire through a step advance between the two
// deaths, under the kubelet's in-place restart and under a restart policy
// that rebuilds the Instance; keep a boot death and a repair from before the
// run out of the count; and walk the exits of the park.

// threeStepTimed is a ladder with two soaked splits before the cutover, so a
// restart can be soaked out and advanced past twice.
func threeStepTimed(soak time.Duration) []v1beta1.RolloutGroupStep {
	pause := &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: soak}}
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 25, Pause: pause},
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: pause},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// runnerDiedAgain is the runner's status after restarts deaths: the run the
// last restart began at startedAt died at finishedAt, and the kubelet has
// started the next.
func runnerDiedAgain(restarts int32, startedAt, finishedAt time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:         constants.MainContainerName,
		RestartCount: restarts,
		State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(finishedAt.Add(2 * time.Second))}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", StartedAt: metav1.NewTime(startedAt), FinishedAt: metav1.NewTime(finishedAt),
		}},
	}
}

// expectAt asserts the ladder stands at step in phase.
func (w *timedCanaryWire) expectAt(t *testing.T, step int32, phase v1beta1.RolloutPhase, when string) {
	t.Helper()
	cs, got := w.status()
	if cs == nil || cs.CurrentStep != step || got != phase || cs.Failed != nil {
		t.Fatalf("%s: want step %d phase %q, got status %+v phase %q", when, step, phase, cs, got)
	}
}

// drainEvents returns every event the recorder holds.
func (w *timedCanaryWire) drainEvents() []string {
	var events []string
	for {
		select {
		case ev := <-w.rec.Events:
			events = append(events, ev)
		default:
			return events
		}
	}
}

// expectParked asserts the ladder parked at step for the named pod's deaths:
// Failed with its marker, the split back on the stable revision, and the
// park announced with the pod's name.
func (w *timedCanaryWire) expectParked(t *testing.T, step int32, pod, when string) {
	t.Helper()
	cs, phase := w.status()
	if phase != v1beta1.RolloutPhaseFailed || cs.Failed == nil || cs.CurrentStep != step {
		t.Fatalf("%s: the ladder must park at step %d, got phase %q step %d failed %+v", when, step, phase, cs.CurrentStep, cs.Failed)
	}
	traffic := w.isvc.Status.Components[v1beta1.EngineComponent].Traffic
	wantStable := coordination.PerRevisionServiceName(w.isvc.Name, v1beta1.EngineComponent, "stable")
	if len(traffic) != 1 || traffic[0].RevisionName != wantStable || traffic[0].Percent != 100 {
		t.Fatalf("%s: the park must return the split to the stable revision, got %+v", when, traffic)
	}
	for _, ev := range w.drainEvents() {
		if strings.Contains(ev, EventReasonCanaryPodCrashed) && strings.Contains(ev, pod) {
			return
		}
	}
	t.Fatalf("%s: the park must be announced with an event naming %s", when, pod)
}

// TestDispatch_SecondDeathAfterTheStepAdvancedParksTheNewStep: the canary
// pod dies once inside the first soak, the kubelet restarts it in place, it
// serves a full soak and the ladder advances; then the restarted run dies
// too. One death at each step is still two deaths of a pod that served, and
// the ladder parks at the step it is on.
func TestDispatch_SecondDeathAfterTheStepAdvancedParksTheNewStep(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	// Ten seconds into the soak target-3's runner dies; the kubelet has it
	// serving again two seconds later, and the soak measures from there.
	w.setRunner(t, "target-3", runnerDied(t0.Add(10*time.Second)), true)
	w.dispatchAt(t, t0.Add(25*time.Second))
	w.expectAt(t, 0, v1beta1.RolloutPhasePaused, "one death restarts the soak")
	w.dispatchAt(t, t0.Add(33*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePending, "a full soak since the restart advances")

	// Forty seconds after it began serving again, with the ladder staging
	// the next step, the restarted run dies as well.
	w.setRunner(t, "target-3", runnerDiedTwice(t0.Add(10*time.Second), t0.Add(52*time.Second)), true)
	w.dispatchAt(t, t0.Add(53*time.Second))
	w.expectParked(t, 1, "target-3", "the second death, one step later")

	// The pod serving again for longer than a soak does not move the park.
	w.dispatchAt(t, t0.Add(3*time.Minute))
	if cs, phase := w.status(); phase != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 1 {
		t.Fatalf("a parked ladder must not move, got phase %q step %d", phase, cs.CurrentStep)
	}
}

// TestDispatch_RebuiltCanaryDyingAtALaterStepParksThere: under a restart
// policy that rebuilds the Instance, the canary pod dies after serving at
// the second split, the rebuilt pod serves a full soak, the ladder advances,
// and the rebuilt pod dies after serving too. The row carries the death that
// opened the rebuild, and the Instance's second death parks the ladder at
// the step it reached.
func TestDispatch_RebuiltCanaryDyingAtALaterStepParksThere(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armLadderCanaryWire(t, threeStepTimed(20*time.Second), t0)
	w.dispatchAt(t, t0.Add(21*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePending, "the first soak elapsed")
	w.dispatchAt(t, t0.Add(22*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePaused, "the second split serves")

	// target-3 dies thirty seconds in and the policy rebuilds its Instance;
	// the replacement serves twenty seconds later and soaks from then.
	w.rebuildInstance(t, "target-3", 3, t0.Add(30*time.Second), t0.Add(31*time.Second))
	w.dispatchAt(t, t0.Add(32*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePaused, "a rebuild in flight is a dip that keeps the step")
	w.rebuiltServes(t, "target-3", 3, t0.Add(50*time.Second))
	w.dispatchAt(t, t0.Add(60*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePaused, "the soak measures from the rebuilt pod serving")
	w.dispatchAt(t, t0.Add(71*time.Second))
	w.expectAt(t, 2, v1beta1.RolloutPhasePending, "a full soak since the rebuilt pod served advances")

	// The rebuilt pod dies after serving, with the ladder staging the cutover.
	w.setRunner(t, "target-3", runnerStillDead(t0.Add(90*time.Second)), false)
	w.dispatchAt(t, t0.Add(91*time.Second))
	w.expectParked(t, 2, "target-3", "the rebuilt pod's own death, a step later")
}

// TestDispatch_BootDeathBeforeServingIsNotTheCanarys: the canary pod
// crashed once at boot, inside the run but before its Instance served. That
// death is not counted: the pod's first death after serving keeps the
// ladder, and its second parks it.
func TestDispatch_BootDeathBeforeServingIsNotTheCanarys(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)
	boot := t0.Add(-3 * time.Minute)
	w.setRunner(t, "target-3", runnerDied(boot), true)
	w.dispatchAt(t, t0.Add(21*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePending, "a boot death does not hold the soak")

	w.setRunner(t, "target-3", runnerDiedTwice(boot, t0.Add(40*time.Second)), true)
	w.dispatchAt(t, t0.Add(41*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePending, "one death after serving keeps the ladder")

	w.setRunner(t, "target-3", runnerDiedAgain(3, t0.Add(42*time.Second), t0.Add(80*time.Second)), true)
	w.dispatchAt(t, t0.Add(81*time.Second))
	w.expectParked(t, 1, "target-3", "the second death after serving")
}

// TestDispatch_RepairBeforeTheRunOpenedIsNotTheCanarys: the Instance was
// rebuilt for a death before the run opened, and the set that serves the
// canary revision is the rebuilt one. That repair is not the canary's: the
// pod's first death after serving keeps the ladder, and its second parks it.
func TestDispatch_RepairBeforeTheRunOpenedIsNotTheCanarys(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)
	w.mutateRow(t, 3, func(row *v1beta1.OMENativeInstanceStatus) {
		row.Incarnation = 2
		exit := int32(1)
		row.LastFailure = &v1beta1.InstanceTermination{PodName: "target-3", ContainerName: constants.MainContainerName,
			Reason: constants.StateReasonError, ExitCode: &exit, Time: metav1.NewTime(t0.Add(-10 * time.Minute))}
	})
	w.dispatchAt(t, t0.Add(21*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePending, "the soak elapsed")

	w.setRunner(t, "target-3", runnerDied(t0.Add(40*time.Second)), true)
	w.dispatchAt(t, t0.Add(41*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePending, "one death after serving keeps the ladder")

	w.setRunner(t, "target-3", runnerDiedTwice(t0.Add(40*time.Second), t0.Add(80*time.Second)), true)
	w.dispatchAt(t, t0.Add(81*time.Second))
	w.expectParked(t, 1, "target-3", "the second death after serving")
}

// TestCrashedCanaryPod_RunOpenedBeforeTheInstanceServed pins the reading
// against an anchor earlier than the Instance's entry into Ready: a death
// before the Instance served, and a repair from before the anchor, are not
// counted; a death after serving and a repair inside the anchor are.
func TestCrashedCanaryPod_RunOpenedBeforeTheInstanceServed(t *testing.T) {
	ready := time.Unix(100000, 0)
	since := ready.Add(-10 * time.Minute)
	after := ready.Add(5 * time.Minute)
	observed := func(rows ...v1beta1.OMENativeInstanceStatus) observedCanaryRevisions {
		return observedCanaryRevisions{targetHash: "new", rows: rows, fromIR: true, statusFresh: true}
	}
	servingRow := instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-decoder-new", &ready, 1)
	for _, tc := range []struct {
		name     string
		observed observedCanaryRevisions
		pods     []*corev1.Pod
		parks    bool
	}{
		{"a boot death and one death after serving", observed(servingRow),
			[]*corev1.Pod{readyAgain(crashPod("new", 5, 1, runnerDiedTwice(ready.Add(-time.Minute), after)))}, false},
		{"a boot death and two deaths after serving", observed(servingRow),
			[]*corev1.Pod{readyAgain(crashPod("new", 5, 1, runnerDiedAgain(3, after.Add(2*time.Second), after.Add(40*time.Second))))}, true},
		{"a repair from before the anchor and one death of the rebuilt pod", observed(rebuiltRow(5, since.Add(-time.Hour), ready, 2)),
			[]*corev1.Pod{readyAgain(crashPod("new", 5, 2, runnerDied(after)))}, false},
		{"a repair inside the anchor and one death of the rebuilt pod", observed(rebuiltRow(5, since.Add(time.Minute), ready, 2)),
			[]*corev1.Pod{readyAgain(crashPod("new", 5, 2, runnerDied(after)))}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := crashedCanaryPod(v1beta1.DecoderComponent, tc.observed, tc.pods, since)
			if (got != nil) != tc.parks {
				t.Fatalf("parks=%v, got %+v", tc.parks, got)
			}
		})
	}
}

// rollInstanceToTarget rolls the wire's Instance index onto the target
// revision: the stable pod is gone, a Ready target pod stands in its place,
// and the row serves the target since readyAt.
func (w *timedCanaryWire) rollInstanceToTarget(t *testing.T, index int32, readyAt time.Time) {
	t.Helper()
	if err := w.client.Delete(w.ctx, canaryPod(w.ns, w.isvc.Name, "engine", "stable", fmt.Sprintf("stable-%d", index))); err != nil {
		t.Fatal(err)
	}
	if err := w.client.Create(w.ctx, canaryPod(w.ns, w.isvc.Name, "engine", "target", fmt.Sprintf("target-%d", index))); err != nil {
		t.Fatal(err)
	}
	w.mutateRow(t, index, func(row *v1beta1.OMENativeInstanceStatus) {
		row.RunningRevision = "soak-wire-engine-target"
		row.Phase = v1beta1.OMENativeInstanceReady
		row.ReadySince = &metav1.Time{Time: readyAt}
	})
}

// TestDispatch_SecondDeathAtAHeldFinalStepParksThroughTheCrashReading: the
// canary pod died once at the first step and came back; the final step
// wrote 100% traffic and is then held short of capacity by a canary pod
// deleted by hand, the stable instance still held. The pod's second death
// is read before the gate that holds the drain: the ladder parks at the
// final step with the traffic back on the stable instance and the pod
// named, on the pass that read the death.
func TestDispatch_SecondDeathAtAHeldFinalStepParksThroughTheCrashReading(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)
	w.setRunner(t, "target-3", runnerDied(t0.Add(10*time.Second)), true)
	w.dispatchAt(t, t0.Add(33*time.Second))
	w.expectAt(t, 1, v1beta1.RolloutPhasePending, "a full soak since the restart advances to the final step")

	// A third Instance rolls onto the canary revision: the final step's
	// capacity is met, 100% traffic lands, and the drain keeps the stable
	// instance held for a later pass to release.
	w.rollInstanceToTarget(t, 1, t0.Add(40*time.Second))
	w.dispatchAt(t, t0.Add(45*time.Second))
	cs, phase := w.status()
	if phase != v1beta1.RolloutPhasePromoting || cs.CurrentStep != 1 || cs.ObservedTrafficWeight != 100 {
		t.Fatalf("the final step must serve 100%% with the stable instance held, got phase %q status %+v", phase, cs)
	}

	// target-2 is deleted by hand: the drain is short of capacity and holds.
	if err := w.client.Delete(w.ctx, canaryPod(w.ns, w.isvc.Name, "engine", "target", "target-2")); err != nil {
		t.Fatal(err)
	}
	w.dispatchAt(t, t0.Add(50*time.Second))
	cs, phase = w.status()
	if phase != v1beta1.RolloutPhasePromoting || cs.CurrentStep != 1 || cs.Failed != nil {
		t.Fatalf("a drain short of capacity holds the final step, got phase %q status %+v", phase, cs)
	}

	// target-3 dies again, the second death of a pod that served.
	w.setRunner(t, "target-3", runnerDiedTwice(t0.Add(10*time.Second), t0.Add(55*time.Second)), true)
	w.dispatchAt(t, t0.Add(56*time.Second))
	w.expectParked(t, 1, "target-3", "the second death at the held final step")
	if cs, _ := w.status(); cs.Failed.Time == nil || !cs.Failed.Time.Time.Equal(t0.Add(56*time.Second)) {
		t.Fatalf("the park must be the crash reading's, stamped on the pass that read the death, got %+v", cs.Failed)
	}
}

// parkByRememberedDeaths drives the wire to the park of
// TestDispatch_SecondDeathAfterTheStepAdvancedParksTheNewStep and returns
// it with its events drained.
func parkByRememberedDeaths(t *testing.T, t0 time.Time) *timedCanaryWire {
	t.Helper()
	w := armTimedCanaryWire(t, 20*time.Second, t0)
	w.setRunner(t, "target-3", runnerDied(t0.Add(10*time.Second)), true)
	w.dispatchAt(t, t0.Add(33*time.Second))
	w.setRunner(t, "target-3", runnerDiedTwice(t0.Add(10*time.Second), t0.Add(52*time.Second)), true)
	w.dispatchAt(t, t0.Add(53*time.Second))
	w.expectParked(t, 1, "target-3", "the park the exits leave")
	return w
}

// TestDispatch_ParkByRememberedDeathsLeavesOnEachExit walks the exits of a
// park for deaths remembered across a step: a rollback drains the rejected
// revision and completes, a new push arms a fresh canary toward the pushed
// revision, a resume re-enters the ladder and the deaths the pods still
// carry park it again, and a promote is not an exit.
func TestDispatch_ParkByRememberedDeathsLeavesOnEachExit(t *testing.T) {
	t0 := time.Unix(100000, 0)

	t.Run("rollback", func(t *testing.T) {
		w := parkByRememberedDeaths(t, t0)
		annotate(w.isvc, constants.RolloutRollbackAnnotation, "true")
		w.dispatchAt(t, t0.Add(time.Minute))
		cs, phase := w.status()
		if phase != v1beta1.RolloutPhaseRollingBack || cs.RolledBackRevisionHash != "target" || cs.Failed != nil {
			t.Fatalf("a rollback must leave the park and drain the rejected revision, got phase %q status %+v", phase, cs)
		}
		ir := &v1beta1.InferenceReplica{}
		if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: "soak-wire-engine"}, ir); err != nil {
			t.Fatal(err)
		}
		if ir.Spec.Pacing == nil || ir.Spec.Pacing.RollbackToRevision == nil || *ir.Spec.Pacing.RollbackToRevision != "soak-wire-engine-stable" {
			t.Fatalf("the revert must point the workload at the stable revision, got pacing %+v", ir.Spec.Pacing)
		}
		// The rejected pods drain and the stable revision takes their Instances.
		for _, name := range []string{"target-2", "target-3"} {
			if err := w.client.Delete(w.ctx, canaryPod(w.ns, w.isvc.Name, "engine", "target", name)); err != nil {
				t.Fatal(err)
			}
		}
		for _, name := range []string{"stable-2", "stable-3"} {
			if err := w.client.Create(w.ctx, canaryPod(w.ns, w.isvc.Name, "engine", "stable", name)); err != nil {
				t.Fatal(err)
			}
		}
		w.dispatchAt(t, t0.Add(2*time.Minute))
		if cs, phase := w.status(); phase != v1beta1.RolloutPhaseRolledBack || cs.RolledBackRevisionHash != "target" {
			t.Fatalf("the revert must complete with the rejected revision recorded, got phase %q status %+v", phase, cs)
		}
	})

	t.Run("new push", func(t *testing.T) {
		w := parkByRememberedDeaths(t, t0)
		// The push mints a revision, the run layer opens a fresh run toward
		// it and the replica names it as its target.
		opened := t0.Add(time.Minute)
		w.isvc.Status.Rollout.ActiveRun = &v1beta1.RolloutRun{
			RunID:           "test-next",
			OpenedAt:        metav1.NewTime(opened),
			PinnedAt:        metav1.NewTime(opened),
			TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "fixed", StableRevision: "stable"}},
			Plan:            w.isvc.Status.Rollout.ActiveRun.Plan,
		}
		if err := w.client.Create(w.ctx, canaryControllerRevision(w.ns, w.isvc.Name, "engine", "fixed", 3)); err != nil {
			t.Fatal(err)
		}
		ir := &v1beta1.InferenceReplica{}
		if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: "soak-wire-engine"}, ir); err != nil {
			t.Fatal(err)
		}
		ir.Status.UpdateRevision = "soak-wire-engine-fixed"
		if err := w.client.Status().Update(w.ctx, ir); err != nil {
			t.Fatal(err)
		}
		w.dispatchAt(t, opened.Add(time.Second))
		cs, phase := w.status()
		if phase != v1beta1.RolloutPhasePending || cs.CurrentStep != 0 || cs.CanaryRevisionHash != "fixed" || cs.Failed != nil {
			t.Fatalf("a new push must arm a fresh canary toward the pushed revision, got phase %q status %+v", phase, cs)
		}
	})

	t.Run("resume", func(t *testing.T) {
		w := parkByRememberedDeaths(t, t0)
		annotate(w.isvc, constants.RolloutResumeAnnotation, "target")
		w.deps.Now = t0.Add(time.Minute)
		out, err := Dispatch(w.ctx, w.deps)
		if err != nil {
			t.Fatal(err)
		}
		cs, phase := w.status()
		if phase != v1beta1.RolloutPhasePending || cs.CurrentStep != 0 || cs.Failed != nil {
			t.Fatalf("a resume must re-enter the ladder at step 0, got phase %q status %+v", phase, cs)
		}
		if len(out.Consume) != 1 || out.Consume[0] != constants.RolloutResumeAnnotation {
			t.Fatalf("the resume must be handed back for removal, got consume %v", out.Consume)
		}
		// The pods still carry their deaths, and the run is the same: the
		// re-entered ladder parks again where it stands.
		w.dispatchAt(t, t0.Add(70*time.Second))
		w.expectParked(t, 0, "target-3", "the pass after the resume")
	})

	t.Run("promote is not an exit", func(t *testing.T) {
		w := parkByRememberedDeaths(t, t0)
		annotate(w.isvc, constants.RolloutPromoteAnnotation, "target")
		w.deps.Now = t0.Add(time.Minute)
		out, err := Dispatch(w.ctx, w.deps)
		if err != nil {
			t.Fatal(err)
		}
		if cs, phase := w.status(); phase != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 1 || cs.Failed == nil {
			t.Fatalf("a promote must leave the park where it is, got phase %q status %+v", phase, cs)
		}
		if len(out.Consume) != 0 || w.isvc.Annotations[constants.RolloutPromoteAnnotation] != "target" {
			t.Fatalf("a promote at a park is not applied, got consume %v annotations %v", out.Consume, w.isvc.Annotations)
		}
	})
}
