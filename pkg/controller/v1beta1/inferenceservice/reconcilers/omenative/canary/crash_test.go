package canary

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A canary that serves and then keeps dying is the canary's verdict, not a
// capacity dip: the ladder parks at the step where it happened, nothing
// routes to the broken revision, and the operator's rollback or next push
// lands. A single restart is a capacity dip, not a crash. The stories
// below drive an eight-Instance engine through a timed ladder and break the
// canary inside the soak and after it.

// soakedLadder is the ladder the stories walk: one Instance soaks under a
// tenth of the traffic, then the rest follows.
func soakedLadder(soak time.Duration) []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("10%"), Traffic: 10, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: soak}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// engineCrash is the observation the dispatcher hands the step machine when
// the canary pod of Instance 7 is in a crash loop after it served.
func engineCrash() *CanaryCrash {
	return &CanaryCrash{Component: v1beta1.EngineComponent, PodName: "svc-engine-7-default-0", Detail: "Error (exit 1)"}
}

// weightOn is the percent the Component's traffic names for a revision hash;
// a revision with no target reads zero.
func weightOn(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) int32 {
	for _, target := range isvc.Status.Components[comp].Traffic {
		if query.RevisionFromName(target.RevisionName).Hash() == hash {
			return target.Percent
		}
	}
	return 0
}

// expectParkedOnCrash asserts the unit parked where it stood: Failed with a
// reason, at step, no traffic on the canary revision and every weighted
// target on a serving revision, and the crash named in an event.
func expectParkedOnCrash(t *testing.T, isvc *v1beta1.InferenceService, in ReconcileInputs, rec *record.FakeRecorder, step int32) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if got := rollout.StateOf(cs, phaseOf(isvc), len(isvc.Spec.Rollout.Groups[0].Canary.Steps)); got != rollout.CanaryStateFailed {
		t.Fatalf("a canary that keeps dying after serving must park Failed, got state %q phase %q status %+v", got, phaseOf(isvc), cs)
	}
	if cs.CurrentStep != step {
		t.Fatalf("the park must hold the step where the canary broke: step %d, want %d", cs.CurrentStep, step)
	}
	if cs.Failed == nil || cs.Failed.Reason == "" {
		t.Fatalf("the park must say why the ladder stopped, got %+v", cs.Failed)
	}
	if w := weightOn(isvc, in.Component, in.CanaryRevisionHash); w != 0 {
		t.Fatalf("no traffic may name the broken revision at the park, got %d%% (%+v)", w, isvc.Status.Components[in.Component].Traffic)
	}
	if cs.ObservedTrafficWeight != 0 {
		t.Fatalf("the recorded canary weight must read zero at the park, got %d", cs.ObservedTrafficWeight)
	}
	expectTrafficServes(t, isvc, in.Component, in.PerRevisionPods)
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, in.Crash.PodName) || !strings.Contains(ev, EventReasonCanaryPodCrashed) {
			t.Fatalf("the park must name the pod that died, got event %q", ev)
		}
	default:
		t.Fatalf("the park must be announced with an event naming the pod")
	}
}

func TestReconcile_CanaryCrashInSoakParksTheStep(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(soakedLadder(20*time.Second), nil))
	rec := record.NewFakeRecorder(8)
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 7})
	in.DesiredReplicas = 8
	in.Recorder = rec
	in.Now = t0
	mustReconcile(t, isvc, in) // the split serves at 10%; the soak anchors here
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused || weightOn(isvc, in.Component, "new") != 10 {
		t.Fatalf("the first step must serve its split before the story starts, got phase %q traffic %+v", phaseOf(isvc), isvc.Status.Components[in.Component].Traffic)
	}

	// Eight seconds into the soak the canary pod is caught in a crash loop,
	// down again after the kubelet restarted it; this pass sees it down.
	in.Crash = engineCrash()
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 7}
	in.Now = t0.Add(8 * time.Second)
	mustReconcile(t, isvc, in)
	expectParkedOnCrash(t, isvc, in, rec, 0)

	// The pod is back and the soak has elapsed: the park holds, the step
	// does not move and the broken revision gets no traffic back.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 7}
	in.Now = t0.Add(30 * time.Second)
	mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if cs.CurrentStep != 0 || phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("an elapsed soak must not move a parked ladder, got step %d phase %q", cs.CurrentStep, phaseOf(isvc))
	}
	if w := weightOn(isvc, in.Component, "new"); w != 0 {
		t.Fatalf("a parked canary gets no traffic back when its pod returns, got %d%%", w)
	}

	// The operator rolls back: the revert drains the broken revision and
	// holds at RolledBack with the rejected revision recorded.
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	res := mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack || !res.RolledBack {
		t.Fatalf("a rollback must land on the park, got phase %q result %+v", phaseOf(isvc), res)
	}
	in.Crash = nil
	in.PerRevisionPods = map[string]int32{"old": 8}
	mustReconcile(t, isvc, in)
	cs = rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack || cs.RolledBackRevisionHash != "new" {
		t.Fatalf("the revert must complete with the rejected revision recorded, got phase %q status %+v", phaseOf(isvc), cs)
	}
	if w := weightOn(isvc, in.Component, "old"); w != 100 {
		t.Fatalf("the stable revision must own the traffic after the revert, got %d%%", w)
	}
}

func TestReconcile_CanaryCrashAfterSoakParksTheNextStep(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(soakedLadder(20*time.Second), nil))
	rec := record.NewFakeRecorder(8)
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 7})
	in.DesiredReplicas = 8
	in.Recorder = rec
	in.Now = t0
	mustReconcile(t, isvc, in) // the split serves at 10%

	// The soak elapses with the canary healthy: the ladder moves on to stage
	// the final step while the first step's weight stands.
	in.Now = t0.Add(25 * time.Second)
	mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending || weightOn(isvc, in.Component, "new") != 10 {
		t.Fatalf("a healthy soak must advance to stage the next step under the standing weight, got step %d phase %q traffic %+v",
			cs.CurrentStep, phaseOf(isvc), isvc.Status.Components[in.Component].Traffic)
	}

	// The canary pod from the first step crash-loops while the next step stages.
	in.Crash = engineCrash()
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 5}
	in.Now = t0.Add(40 * time.Second)
	mustReconcile(t, isvc, in)
	expectParkedOnCrash(t, isvc, in, rec, 1)

	// More canary capacity coming up does not reopen the gate of a parked ladder.
	in.PerRevisionPods = map[string]int32{"new": 7, "old": 1}
	in.Now = t0.Add(90 * time.Second)
	mustReconcile(t, isvc, in)
	cs = rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhaseFailed || weightOn(isvc, in.Component, "new") != 0 {
		t.Fatalf("a parked ladder must not promote the broken revision, got step %d phase %q traffic %+v",
			cs.CurrentStep, phaseOf(isvc), isvc.Status.Components[in.Component].Traffic)
	}
}

// TestReconcile_CanaryCrashIsNotAPodLoss pins the line between the two: a
// pod that is merely gone keeps the split and the step, and the same counts
// with a crash observed park the step.
func TestReconcile_CanaryCrashIsNotAPodLoss(t *testing.T) {
	for _, tc := range []struct {
		name  string
		crash *CanaryCrash
		want  rollout.CanaryState
	}{
		{"pod gone", nil, rollout.CanaryStateServing},
		{"pod crash-looping after serving", engineCrash(), rollout.CanaryStateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := pinActiveRun(canaryISVC(soakedLadder(20*time.Second), nil))
			in := baseInputs(isvc, map[string]int32{"new": 1, "old": 7})
			in.DesiredReplicas = 8
			mustReconcile(t, isvc, in)
			in.PerRevisionPods = map[string]int32{"new": 0, "old": 7}
			in.Crash = tc.crash
			in.Now = in.Now.Add(5 * time.Second)
			mustReconcile(t, isvc, in)
			cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
			if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != tc.want {
				t.Fatalf("state %q, want %q", got, tc.want)
			}
		})
	}
}

// handLadder is the ladder of a two-Instance engine whose canary is held by a
// re-pin: a quarter, then half, each soaked, then the rest.
func handLadder() []v1beta1.RolloutGroupStep {
	soak := &metav1.Duration{Duration: 20 * time.Second}
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 25, Pause: &v1beta1.RolloutPause{Duration: soak}},
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: soak}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// deletedCanaryPodReading is what the dispatcher hands the step machine while
// the canary pod of Instance 1 is being deleted by hand: the kubelet stopped
// its runner with the term signal and the pod is still on its way out.
func deletedCanaryPodReading(isvc *v1beta1.InferenceService, now time.Time) *CanaryCrash {
	ready := now.Add(-10 * time.Minute)
	observed := observedCanaryRevisions{targetHash: "new", fromIR: true, statusFresh: true,
		rows: []v1beta1.OMENativeInstanceStatus{instanceRow(1, v1beta1.OMENativeInstanceReady, "svc-engine-new", &ready, 1)}}
	pods := []*corev1.Pod{deleting(stoppedPod("new", 1, 1, 143, "Error", now.Add(-time.Second), corev1.PodRunning))}
	return crashedCanaryPod(v1beta1.EngineComponent, observed, pods, crashAnchor(isvc, rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent), now))
}

// TestReconcile_CanaryPodDeletedAtAHoldSurvivesTheResumeAndTheRelease walks
// a two-Instance engine to a pre-step hold, deletes the canary pod by hand
// under it, applies the inert resume, waits out the pod's return and releases
// the hold with a promote: the hold survives the deletion, nothing parks, and
// the ladder reaches Stable.
func TestReconcile_CanaryPodDeletedAtAHoldSurvivesTheResumeAndTheRelease(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(handLadder(), nil))
	rec := record.NewFakeRecorder(8)
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 1})
	in.DesiredReplicas = 2
	in.Recorder = rec
	in.Now = t0
	mustReconcile(t, isvc, in) // the first split serves at 25%
	in.Now = t0.Add(25 * time.Second)
	mustReconcile(t, isvc, in) // the soak elapsed: the second step stages
	mustReconcile(t, isvc, in) // its capacity is already up: it serves at 50%
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePaused || weightOn(isvc, in.Component, "new") != 50 {
		t.Fatalf("the second split must serve before the hold, got step %d phase %q traffic %+v", cs.CurrentStep, phaseOf(isvc), isvc.Status.Components[in.Component].Traffic)
	}
	// A re-pin clamps the ladder: the step holds for an explicit promote.
	cs.PreStepHold = true
	in.Now = t0.Add(30 * time.Second)
	mustReconcile(t, isvc, in)
	expectState := func(want rollout.CanaryState, when string) {
		t.Helper()
		cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
		if got := rollout.StateOf(cs, phaseOf(isvc), len(handLadder())); got != want {
			t.Fatalf("%s: state %q, want %q (phase %q, step %d, failed %+v)", when, got, want, phaseOf(isvc), cs.CurrentStep, cs.Failed)
		}
	}
	expectState(rollout.CanaryStatePreHold, "after the re-pin")

	// The canary pod is deleted by hand: the pass reads the pod on its way
	// out and sees the capacity dip.
	in.Now = t0.Add(40 * time.Second)
	in.Crash = deletedCanaryPodReading(isvc, in.Now)
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 1}
	mustReconcile(t, isvc, in)
	expectState(rollout.CanaryStatePreHold, "after the deletion")
	cs = rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if cs.CurrentStep != 1 || cs.ObservedTrafficWeight != 50 {
		t.Fatalf("a deleted pod is a dip: the hold keeps its step and weight, got step %d weight %d", cs.CurrentStep, cs.ObservedTrafficWeight)
	}
	select {
	case ev := <-rec.Events:
		t.Fatalf("a deletion must not be announced as a death, got %q", ev)
	default:
	}

	// The resume is inert at a live hold: consumed, nothing moves.
	in.Crash = nil
	annotate(isvc, constants.RolloutResumeAnnotation, "new")
	in.Now = t0.Add(45 * time.Second)
	res := mustReconcile(t, isvc, in)
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutResumeAnnotation {
		t.Fatalf("a resume at a live hold is consumed and ignored, got consume %v", res.Consume)
	}
	delete(isvc.Annotations, constants.RolloutResumeAnnotation)
	expectState(rollout.CanaryStatePreHold, "after the resume")

	// The replacement pod returns; the hold still waits for its release.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = t0.Add(60 * time.Second)
	mustReconcile(t, isvc, in)
	expectState(rollout.CanaryStatePreHold, "after the pod's return")

	// The release: a promote opens the hold, the soaked step moves on, the
	// final step cuts over and the unit reads Stable.
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	in.Now = t0.Add(85 * time.Second)
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	for _, pods := range []map[string]int32{{"new": 1, "old": 1}, {"new": 2}} {
		in.PerRevisionPods = pods
		in.Now = in.Now.Add(10 * time.Second)
		mustReconcile(t, isvc, in)
		mustReconcile(t, isvc, in)
	}
	cs = rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable || cs.Failed != nil || int(cs.CurrentStep) != len(handLadder()) {
		t.Fatalf("the ladder must reach Stable after the release, got phase %q step %d failed %+v", phaseOf(isvc), cs.CurrentStep, cs.Failed)
	}
}

// crashPod is a Ready pod of hash on Instance index whose runner carries the
// given container status; incarnation labels the pod set it belongs to.
func crashPod(hash string, index int, incarnation int64, status corev1.ContainerStatus) *corev1.Pod {
	pod := canaryPod("default", "svc", "engine", hash, fmt.Sprintf("svc-engine-%d", index))
	pod.Labels[query.LabelInstanceIncarnation] = strconv.FormatInt(incarnation, 10)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{status}
	return pod
}

// runnerDied is the runner's status after the kubelet restarted it: the run
// that died finished at finishedAt, the next one is running.
func runnerDied(finishedAt time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:         constants.MainContainerName,
		RestartCount: 1,
		State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(finishedAt.Add(2 * time.Second))}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", FinishedAt: metav1.NewTime(finishedAt),
		}},
	}
}

// runnerDeadAgain is the runner's status when the run its first restart
// began has died too and the kubelet has not restarted it yet.
func runnerDeadAgain(first time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:         constants.MainContainerName,
		RestartCount: 1,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 137, Reason: "OOMKilled", StartedAt: metav1.NewTime(first.Add(2 * time.Second)), FinishedAt: metav1.NewTime(first.Add(30 * time.Second)),
		}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", FinishedAt: metav1.NewTime(first)}},
	}
}

// runnerStillDead is the runner's status between its death and the restart.
func runnerStillDead(finishedAt time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  constants.MainContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled", FinishedAt: metav1.NewTime(finishedAt)}},
	}
}

func runnerRunningSince(startedAt time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  constants.MainContainerName,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(startedAt)}},
	}
}

func instanceRow(index int32, phase v1beta1.OMENativeInstancePhase, revision string, readySince *time.Time, incarnation int64) v1beta1.OMENativeInstanceStatus {
	row := v1beta1.OMENativeInstanceStatus{Index: index, Phase: phase, RunningRevision: revision, Incarnation: incarnation}
	if readySince != nil {
		row.ReadySince = &metav1.Time{Time: *readySince}
	}
	return row
}

func TestCrashedCanaryPod(t *testing.T) {
	ready := time.Unix(100000, 0)
	since := ready.Add(time.Minute) // the canary's anchor, after the Instance entered Ready
	after := since.Add(30 * time.Second)
	observed := func(rows ...v1beta1.OMENativeInstanceStatus) observedCanaryRevisions {
		return observedCanaryRevisions{targetHash: "new", rows: rows, fromIR: true, statusFresh: true}
	}
	readyRow := instanceRow(3, v1beta1.OMENativeInstanceReady, "svc-engine-new", &ready, 1)
	for _, tc := range []struct {
		name     string
		observed observedCanaryRevisions
		pods     []*corev1.Pod
		wantPod  string
		detail   string
	}{
		{"a runner that died once after serving is a single restart, not a crash", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, runnerDied(after))}, "", ""},
		{"a runner that died twice after serving", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, runnerDiedTwice(after, after.Add(30*time.Second)))}, "svc-engine-3", "container ome-container Error (exit 1)"},
		{"a run with no recorded termination is a start, not a death", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, runnerRunningSince(after))}, "", ""},
		{"a runner dead once and not restarted yet is a single death", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, runnerStillDead(after))}, "", ""},
		{"a runner dead again before the kubelet restarts it", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, runnerDeadAgain(after))}, "svc-engine-3", "container ome-container OOMKilled (exit 137)"},
		{"a pod under deletion is never read, whatever it carries", observed(readyRow), []*corev1.Pod{deleting(crashPod("new", 3, 1, runnerDied(after)))}, "", ""},
		{"a pod deleted by hand stops its runner cleanly", observed(readyRow), []*corev1.Pod{deleting(stoppedPod("new", 3, 1, 0, "Completed", after, corev1.PodSucceeded))}, "", ""},
		{"a pod deleted by hand is stopped by the kubelet's term signal", observed(readyRow), []*corev1.Pod{deleting(stoppedPod("new", 3, 1, 143, "Error", after, corev1.PodRunning))}, "", ""},
		{"a pod deleted by hand is killed after its grace", observed(readyRow), []*corev1.Pod{deleting(stoppedPod("new", 3, 1, 137, "Error", after, corev1.PodFailed))}, "", ""},
		{"a terminal pod is a loss the workload recycles", observed(readyRow), []*corev1.Pod{stoppedPod("new", 3, 1, 137, "Error", after, corev1.PodFailed)}, "", ""},
		{"a boot crash before Ready is not a death after serving", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, runnerDied(ready.Add(-time.Second)))}, "", ""},
		{"a death before the canary began is not the canary's", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, runnerDied(since.Add(-time.Second)))}, "", ""},
		{"the stable revision's crash loop is not the canary's", observed(readyRow, instanceRow(1, v1beta1.OMENativeInstanceReady, "svc-engine-old", &ready, 1)), []*corev1.Pod{crashPod("old", 1, 1, runnerDiedTwice(after, after.Add(30*time.Second)))}, "", ""},
		{"a row that never served has no serving run to lose", observed(instanceRow(3, v1beta1.OMENativeInstanceReady, "svc-engine-new", nil, 1)), []*corev1.Pod{crashPod("new", 3, 1, runnerDiedTwice(after, after.Add(30*time.Second)))}, "", ""},
		{"a rolling row's restarts are the roll", observed(instanceRow(3, v1beta1.OMENativeInstanceUpdating, "svc-engine-old", &ready, 1)), []*corev1.Pod{crashPod("new", 3, 1, runnerDiedTwice(after, after.Add(30*time.Second)))}, "", ""},
		{"a repair drains the pod set that crash-looped", observed(instanceRow(3, v1beta1.OMENativeInstanceRestarting, "svc-engine-new", &ready, 2)), []*corev1.Pod{crashPod("new", 3, 1, runnerDiedTwice(after, after.Add(30*time.Second)))}, "svc-engine-3", "container ome-container Error (exit 1)"},
		{"a repair drains a pod set that died once: a single death", observed(instanceRow(3, v1beta1.OMENativeInstanceRestarting, "svc-engine-new", &ready, 2)), []*corev1.Pod{crashPod("new", 3, 1, runnerDied(after))}, "", ""},
		{"a repair's rebuilt pod boots like any fresh pod", observed(instanceRow(3, v1beta1.OMENativeInstanceRestarting, "svc-engine-new", &ready, 2)), []*corev1.Pod{crashPod("new", 3, 2, runnerDiedTwice(after, after.Add(30*time.Second)))}, "", ""},
		{"a sidecar's crash loop leaves the serving process alone", observed(readyRow), []*corev1.Pod{crashPod("new", 3, 1, func() corev1.ContainerStatus {
			s := runnerDiedTwice(after, after.Add(30*time.Second))
			s.Name = "sidecar"
			return s
		}())}, "", ""},
		{"a pod with no row has no anchor", observed(), []*corev1.Pod{crashPod("new", 3, 1, runnerDiedTwice(after, after.Add(30*time.Second)))}, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := crashedCanaryPod(v1beta1.EngineComponent, tc.observed, tc.pods, since)
			if tc.wantPod == "" {
				if got != nil {
					t.Fatalf("no crash expected, got %+v", got)
				}
				return
			}
			if got == nil || got.PodName != tc.wantPod || got.Detail != tc.detail || got.Component != v1beta1.EngineComponent {
				t.Fatalf("crash %+v, want pod %q detail %q", got, tc.wantPod, tc.detail)
			}
		})
	}
}

// stoppedPod is a pod of hash on Instance index whose runner stopped with
// exitCode at finishedAt and was not restarted; phase is what the kubelet
// reports for the pod.
func stoppedPod(hash string, index int, incarnation int64, exitCode int32, reason string, finishedAt time.Time, phase corev1.PodPhase) *corev1.Pod {
	pod := crashPod(hash, index, incarnation, corev1.ContainerStatus{
		Name:  constants.MainContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, Reason: reason, FinishedAt: metav1.NewTime(finishedAt)}},
	})
	pod.Status.Phase = phase
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	return pod
}

func deleting(pod *corev1.Pod) *corev1.Pod {
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	return pod
}

// TestCrashAnchor pins where the death count begins: the open of the run
// the canary is bound to, the same at every step, and nowhere for a unit
// that is not armed or has no run.
func TestCrashAnchor(t *testing.T) {
	now := time.Unix(100000, 0)
	opened := now.Add(-time.Hour)
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	isvc.Status.Rollout.ActiveRun.OpenedAt = metav1.NewTime(opened)
	if got := crashAnchor(isvc, nil, now); !got.Equal(now) {
		t.Fatalf("an unarmed unit anchors at now, got %v", got)
	}
	for _, cs := range []*v1beta1.CanaryStatus{
		{StepEnteredTime: &metav1.Time{Time: opened}},
		{CurrentStep: 2, StepEnteredTime: &metav1.Time{Time: now.Add(-time.Minute)}},
		{},
	} {
		if got := crashAnchor(isvc, cs, now); !got.Equal(opened) {
			t.Fatalf("an armed unit anchors at the open of its run at every step, got %v for %+v", got, cs)
		}
	}
	if got := crashAnchor(canaryISVC(twoStep(), nil), &v1beta1.CanaryStatus{CurrentStep: 1}, now); !got.Equal(now) {
		t.Fatalf("a unit with no pinned run reads no death, got %v", got)
	}
}

func TestMemberRetargeted(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	if !memberRetargeted(isvc, v1beta1.EngineComponent) {
		t.Fatal("a member the run recorded no target for is read as retargeted")
	}
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "rold", StableRevision: "rold"},
	)
	if !memberRetargeted(isvc, v1beta1.EngineComponent) {
		t.Fatal("a member moved off its stable revision is retargeted")
	}
	if memberRetargeted(isvc, v1beta1.RouterComponent) {
		t.Fatal("a member the run left on its stable revision is not")
	}
}

// TestDispatch_CanaryPodDeathParksFromPodsAndRows drives the wiring: the
// dispatcher reads the deaths from the live pods against the IR's rows and
// the step machine parks on a crash loop with the pod named, while a single
// restart, the same loop on a stable pod, a deletion seen on its way out
// and a deletion seen only through its fresh replacement leave the ladder
// where it is.
func TestDispatch_CanaryPodDeathParksFromPodsAndRows(t *testing.T) {
	type fault int
	const (
		restartedOnce fault = iota
		crashLooping
		deletedByHand
		replacedByFreshPod
	)
	for _, tc := range []struct {
		name      string
		victim    string
		fault     fault
		wantPhase v1beta1.RolloutPhase
	}{
		{"canary pod restarted once", "target-3", restartedOnce, v1beta1.RolloutPhasePaused},
		{"canary pod in a crash loop", "target-3", crashLooping, v1beta1.RolloutPhaseFailed},
		{"stable pod in a crash loop", "stable-1", crashLooping, v1beta1.RolloutPhasePaused},
		{"canary pod deleted by hand", "target-3", deletedByHand, v1beta1.RolloutPhasePaused},
		{"canary pod deleted and replaced by a fresh pod with no restart", "target-3", replacedByFreshPod, v1beta1.RolloutPhasePaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := "default"
			n4 := 4
			isvc := canaryISVC(twoStep(), nil)
			isvc.Namespace = ns
			isvc.Name = "crash-wire"
			isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
			pinActiveRun(isvc)
			t0 := time.Unix(100000, 0)
			// The run opened before the target pods served, as a push does.
			isvc.Status.Rollout.ActiveRun.OpenedAt = metav1.NewTime(t0.Add(-5 * time.Minute))
			readyAt := t0.Add(-2 * time.Minute)

			engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "crash-wire-engine", UID: canaryIRUID("crash-wire-engine"), Generation: 2}}
			engineIR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
			engineIR.Status.CurrentRevision = "crash-wire-engine-stable"
			engineIR.Status.UpdateRevision = "crash-wire-engine-target"
			engineIR.Status.ObservedGeneration = engineIR.Generation
			engineIR.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
				instanceRow(0, v1beta1.OMENativeInstanceReady, "crash-wire-engine-stable", &readyAt, 1),
				instanceRow(1, v1beta1.OMENativeInstanceReady, "crash-wire-engine-stable", &readyAt, 1),
				instanceRow(2, v1beta1.OMENativeInstanceReady, "crash-wire-engine-target", &readyAt, 1),
				instanceRow(3, v1beta1.OMENativeInstanceReady, "crash-wire-engine-target", &readyAt, 1),
			}
			c := fake.NewClientBuilder().
				WithScheme(canaryScheme(t)).
				WithStatusSubresource(&v1beta1.InferenceReplica{}).
				WithRuntimeObjects(
					isvc,
					engineIR,
					canaryPod(ns, isvc.Name, "engine", "stable", "stable-0"),
					canaryPod(ns, isvc.Name, "engine", "stable", "stable-1"),
					canaryPod(ns, isvc.Name, "engine", "target", "target-2"),
					canaryPod(ns, isvc.Name, "engine", "target", "target-3"),
					canaryControllerRevision(ns, isvc.Name, "engine", "stable", 1),
					canaryControllerRevision(ns, isvc.Name, "engine", "target", 2),
				).
				Build()
			ctx := context.Background()
			rec := record.NewFakeRecorder(16)
			deps := DispatchDeps{Client: c, Reader: c, Recorder: rec, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc), Now: t0}
			if _, err := Dispatch(ctx, deps); err != nil {
				t.Fatalf("Dispatch arming the canary: %v", err)
			}
			if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
				t.Fatalf("the split must serve before the story starts, got %q", got)
			}

			// Twenty seconds into the hold the victim's runner dies once and
			// the kubelet brings it back, or dies again after that; or the pod
			// is deleted by hand, seen on its way out with its runner stopped,
			// or seen only as a fresh replacement that is still coming up.
			pod := &corev1.Pod{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: tc.victim}, pod); err != nil {
				t.Fatal(err)
			}
			died := t0.Add(20 * time.Second)
			switch tc.fault {
			case replacedByFreshPod:
				if err := c.Delete(ctx, pod); err != nil {
					t.Fatal(err)
				}
				pod = canaryPod(ns, isvc.Name, "engine", "target", tc.victim)
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{runnerRunningSince(died.Add(5 * time.Second))}
				if err := c.Create(ctx, pod); err != nil {
					t.Fatal(err)
				}
			case restartedOnce:
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{runnerDied(died)}
			case crashLooping:
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{runnerDiedTwice(died, died.Add(5*time.Second))}
			case deletedByHand:
				pod.Finalizers = []string{"test.ome.io/hold"}
				if err := c.Update(ctx, pod); err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(ctx, pod); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: tc.victim}, pod); err != nil {
					t.Fatal(err)
				}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name:  constants.MainContainerName,
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 143, Reason: "Error", FinishedAt: metav1.NewTime(died)}},
				}}
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
			}
			if err := c.Status().Update(ctx, pod); err != nil {
				t.Fatal(err)
			}
			deps.Now = t0.Add(30 * time.Second)
			if _, err := Dispatch(ctx, deps); err != nil {
				t.Fatalf("Dispatch after the death: %v", err)
			}
			if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != tc.wantPhase {
				t.Fatalf("phase %q, want %q (canary %+v)", got, tc.wantPhase, isvc.Status.Canary)
			}
			if tc.wantPhase != v1beta1.RolloutPhaseFailed {
				return
			}
			if isvc.Status.Canary.Failed == nil || isvc.Status.Canary.CurrentStep != 0 {
				t.Fatalf("the park must carry a reason at the step that broke, got %+v", isvc.Status.Canary)
			}
			traffic := isvc.Status.Components[v1beta1.EngineComponent].Traffic
			wantStable := coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "stable")
			if len(traffic) != 1 || traffic[0].RevisionName != wantStable || traffic[0].Percent != 100 {
				t.Fatalf("the park must return the split to the stable revision, got %+v", traffic)
			}
			select {
			case ev := <-rec.Events:
				if !strings.Contains(ev, EventReasonCanaryPodCrashed) || !strings.Contains(ev, tc.victim) {
					t.Fatalf("the park must name the pod that died, got %q", ev)
				}
			default:
				t.Fatal("the park must be announced with an event")
			}
		})
	}
}
