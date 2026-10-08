package canary

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A timed soak measures the canary serving, not the clock since the step
// began: a canary pod that dies once and comes back keeps the step, and the
// step moves only once a full soak has passed since that restart. A pod
// that dies again inside that soak is a crash loop, and the ladder parks at
// the step it was soaking, never at the next one. The stories below drive
// the eight-Instance engine of soakedLadder through a restart inside the
// first step's soak.

// expectHeldAt asserts the ladder is still soaking step at the hold: the
// step's phase and its programmed weight stand.
func expectHeldAt(t *testing.T, isvc *v1beta1.InferenceService, step int32, when string) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != step || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("%s: the step must hold, got step %d phase %q", when, cs.CurrentStep, phaseOf(isvc))
	}
}

func TestReconcile_RestartInSoakHoldsTheStepForAFullSoak(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(soakedLadder(20*time.Second), nil))
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 7})
	in.DesiredReplicas = 8
	in.Now = t0
	mustReconcile(t, isvc, in) // the split serves at 10%; the soak anchors here
	expectHeldAt(t, isvc, 0, "the split serves")

	// Ten seconds in, the canary pod's runner dies and the kubelet restarts
	// it in place; this pass sees the pod down.
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 7}
	in.CanaryRestartedAt = t0.Add(10 * time.Second)
	in.Now = t0.Add(10 * time.Second)
	mustReconcile(t, isvc, in)
	expectHeldAt(t, isvc, 0, "the pod is down")

	// The restarted runner serves again two seconds later.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 7}
	in.CanaryRestartedAt = t0.Add(12 * time.Second)
	in.Now = t0.Add(12 * time.Second)
	mustReconcile(t, isvc, in)
	expectHeldAt(t, isvc, 0, "the pod is back")

	// The soak since the split first served has elapsed, but the restarted
	// pod has served for thirteen seconds: the step holds.
	in.Now = t0.Add(25 * time.Second)
	mustReconcile(t, isvc, in)
	expectHeldAt(t, isvc, 0, "the soak since step entry elapsed")
	in.Now = t0.Add(31 * time.Second)
	mustReconcile(t, isvc, in)
	expectHeldAt(t, isvc, 0, "one second short of a soak since the restart")

	// A full soak since the restart: the step advances to stage the next.
	in.Now = t0.Add(32 * time.Second)
	res := mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending || !res.Stepped {
		t.Fatalf("a full soak since the restart must advance, got step %d phase %q result %+v", cs.CurrentStep, phaseOf(isvc), res)
	}
}

// TestReconcile_RestartSeenAfterTheSoakElapsedHoldsTheAdvance: the restart
// happened after the soak had already elapsed, and the pass that would have
// advanced the step is the first to see it. The advance waits for a full
// soak since the restart all the same.
func TestReconcile_RestartSeenAfterTheSoakElapsedHoldsTheAdvance(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(soakedLadder(20*time.Second), nil))
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 7})
	in.DesiredReplicas = 8
	in.Now = t0
	mustReconcile(t, isvc, in)

	in.CanaryRestartedAt = t0.Add(22 * time.Second)
	in.Now = t0.Add(25 * time.Second)
	mustReconcile(t, isvc, in)
	expectHeldAt(t, isvc, 0, "a restart after the soak elapsed")
	in.Now = t0.Add(41 * time.Second)
	mustReconcile(t, isvc, in)
	expectHeldAt(t, isvc, 0, "one second short of a soak since the restart")

	in.Now = t0.Add(42 * time.Second)
	mustReconcile(t, isvc, in)
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.CurrentStep != 1 {
		t.Fatalf("a full soak since the restart must advance, got step %d", cs.CurrentStep)
	}
}

// TestReconcile_SecondDeathInsideTheRestartedSoakParksAtTheStep replays a
// canary whose runner exits shortly after it serves, every time: the first
// death keeps the step, the soak since the split first served elapses while
// the restarted pod is up, and the second death is read as the loop at the
// step that was soaking.
func TestReconcile_SecondDeathInsideTheRestartedSoakParksAtTheStep(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(soakedLadder(20*time.Second), nil))
	rec := record.NewFakeRecorder(8)
	t0 := time.Unix(100000, 0)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 7})
	in.DesiredReplicas = 8
	in.Recorder = rec
	in.Now = t0
	mustReconcile(t, isvc, in)

	in.PerRevisionPods = map[string]int32{"new": 0, "old": 7}
	in.CanaryRestartedAt = t0.Add(10 * time.Second)
	in.Now = t0.Add(10 * time.Second)
	mustReconcile(t, isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 7}
	in.CanaryRestartedAt = t0.Add(12 * time.Second)
	in.Now = t0.Add(25 * time.Second)
	mustReconcile(t, isvc, in)
	expectHeldAt(t, isvc, 0, "the soak since step entry elapsed with a restart inside it")

	// The restarted run dies too, sixteen seconds after it began.
	in.Crash = engineCrash()
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 7}
	in.CanaryRestartedAt = t0.Add(28 * time.Second)
	in.Now = t0.Add(28 * time.Second)
	mustReconcile(t, isvc, in)
	expectParkedOnCrash(t, isvc, in, rec, 0)

	// Every soak has elapsed and the pod is up between its deaths: the park
	// holds the step and the broken revision gets no traffic back.
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 7}
	in.Now = t0.Add(90 * time.Second)
	mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 0 || phaseOf(isvc) != v1beta1.RolloutPhaseFailed || weightOn(isvc, v1beta1.EngineComponent, "new") != 0 {
		t.Fatalf("a parked ladder must not move, got step %d phase %q traffic %+v", cs.CurrentStep, phaseOf(isvc), isvc.Status.Components[v1beta1.EngineComponent].Traffic)
	}
}

// timedCanaryWire is a four-Instance engine, two Instances on the target
// revision, armed through the dispatcher under a timed first step: the
// readings the step machine gets come from the pods and the Instance rows.
type timedCanaryWire struct {
	ctx    context.Context
	client client.Client
	rec    *record.FakeRecorder
	deps   DispatchDeps
	isvc   *v1beta1.InferenceService
	ns     string
}

func armTimedCanaryWire(t *testing.T, soak time.Duration, t0 time.Time) *timedCanaryWire {
	t.Helper()
	return armLadderCanaryWire(t, twoStepTimed(soak), t0)
}

// armLadderCanaryWire arms the wire under the given ladder. The run opened
// before the target pods served, as a push does: the pods roll under the
// pinned run and serve from then on.
func armLadderCanaryWire(t *testing.T, steps []v1beta1.RolloutGroupStep, t0 time.Time) *timedCanaryWire {
	t.Helper()
	ns := "default"
	n4 := 4
	isvc := canaryISVC(steps, nil)
	isvc.Namespace = ns
	isvc.Name = "soak-wire"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.OpenedAt = metav1.NewTime(t0.Add(-5 * time.Minute))
	readyAt := t0.Add(-2 * time.Minute)

	engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "soak-wire-engine", UID: canaryIRUID("soak-wire-engine"), Generation: 2}}
	engineIR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	engineIR.Status.CurrentRevision = "soak-wire-engine-stable"
	engineIR.Status.UpdateRevision = "soak-wire-engine-target"
	engineIR.Status.ObservedGeneration = engineIR.Generation
	engineIR.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		instanceRow(0, v1beta1.OMENativeInstanceReady, "soak-wire-engine-stable", &readyAt, 1),
		instanceRow(1, v1beta1.OMENativeInstanceReady, "soak-wire-engine-stable", &readyAt, 1),
		instanceRow(2, v1beta1.OMENativeInstanceReady, "soak-wire-engine-target", &readyAt, 1),
		instanceRow(3, v1beta1.OMENativeInstanceReady, "soak-wire-engine-target", &readyAt, 1),
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
	w := &timedCanaryWire{ctx: ctx, client: c, rec: rec, isvc: isvc, ns: ns,
		deps: DispatchDeps{Client: c, Reader: c, Recorder: rec, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{}), Now: t0}}
	if _, err := Dispatch(ctx, w.deps); err != nil {
		t.Fatalf("Dispatch arming the canary: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("the split must serve before the story starts, got %q", got)
	}
	return w
}

// setRunner rewrites the named pod's runner status and readiness.
func (w *timedCanaryWire) setRunner(t *testing.T, name string, status corev1.ContainerStatus, ready bool) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{status}
	if ready {
		readyAgain(pod)
	} else {
		notReady(pod)
	}
	if err := w.client.Status().Update(w.ctx, pod); err != nil {
		t.Fatal(err)
	}
}

func (w *timedCanaryWire) dispatchAt(t *testing.T, now time.Time) {
	t.Helper()
	w.deps.Now = now
	if _, err := Dispatch(w.ctx, w.deps); err != nil {
		t.Fatalf("Dispatch at %v: %v", now, err)
	}
}

func (w *timedCanaryWire) status() (*v1beta1.CanaryStatus, v1beta1.RolloutPhase) {
	return w.isvc.Status.Canary, w.isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase
}

// TestDispatch_RestartInSoakIsReadFromThePod: the single restart of a canary
// pod, read from its container status, holds the timed step past the soak
// since step entry, and the step advances once the restarted runner has
// served a full soak.
func TestDispatch_RestartInSoakIsReadFromThePod(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	// Ten seconds into the soak target-3's runner dies; the kubelet has it
	// running again two seconds later and it is serving.
	w.setRunner(t, "target-3", runnerDied(t0.Add(10*time.Second)), true)
	w.dispatchAt(t, t0.Add(25*time.Second))
	cs, phase := w.status()
	if cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("a restart inside the soak must hold the step until a full soak since it, got step %d phase %q", cs.CurrentStep, phase)
	}

	w.dispatchAt(t, t0.Add(33*time.Second))
	cs, phase = w.status()
	if cs.CurrentStep != 1 || phase != v1beta1.RolloutPhasePending {
		t.Fatalf("a full soak since the restart must advance, got step %d phase %q", cs.CurrentStep, phase)
	}
}

// TestDispatch_RestartInSoakThenCrashLoopParksAtTheStep is the canary whose
// runner keeps exiting after it serves, read from the pod: the first death
// is a restart that keeps the step, and the second death, inside the soak
// the restart began, parks the ladder at that step with the pod named.
func TestDispatch_RestartInSoakThenCrashLoopParksAtTheStep(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	w.setRunner(t, "target-3", runnerDied(t0.Add(10*time.Second)), true)
	w.dispatchAt(t, t0.Add(25*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("a restart inside the soak must hold the step, got step %d phase %q", cs.CurrentStep, phase)
	}

	// The restarted run dies too and the kubelet backs off before the next.
	w.setRunner(t, "target-3", runnerInCrashLoopBackOff(t0.Add(12*time.Second), t0.Add(28*time.Second)), false)
	w.dispatchAt(t, t0.Add(30*time.Second))
	cs, phase := w.status()
	if phase != v1beta1.RolloutPhaseFailed || cs.Failed == nil {
		t.Fatalf("a canary pod that dies again inside the soak must park, got phase %q status %+v", phase, cs)
	}
	if cs.CurrentStep != 0 {
		t.Fatalf("the park must hold the step that was soaking, got step %d", cs.CurrentStep)
	}
	select {
	case ev := <-w.rec.Events:
		if !strings.Contains(ev, EventReasonCanaryPodCrashed) || !strings.Contains(ev, "target-3") {
			t.Fatalf("the park must name the pod that died, got %q", ev)
		}
	default:
		t.Fatal("the park must be announced with an event")
	}
}

// TestLatestCanaryRestart pins what the soak's anchor reads: the newest
// death or restart of a live canary-revision pod's runner, or the failure
// that opened its Instance's last rebuild; nothing from a stable pod, a pod
// on its way out or gone, a sidecar, or a runner on its first run.
func TestLatestCanaryRestart(t *testing.T) {
	ready := time.Unix(100000, 0)
	died := ready.Add(5 * time.Minute)
	observed := func(rows ...v1beta1.OMENativeInstanceStatus) observedCanaryRevisions {
		return observedCanaryRevisions{targetHash: "new", rows: rows, fromIR: true, statusFresh: true}
	}
	servingRow := instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-engine-new", &ready, 1)
	sidecarRestarted := corev1.ContainerStatus{
		Name: "sidecar", RestartCount: 1,
		State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(died.Add(2 * time.Second))}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, FinishedAt: metav1.NewTime(died)}},
	}
	for _, tc := range []struct {
		name     string
		observed observedCanaryRevisions
		pods     []*corev1.Pod
		want     time.Time
	}{
		{"a runner on its first run", observed(servingRow), []*corev1.Pod{crashPod("new", 5, 1, runnerRunningSince(ready))}, time.Time{}},
		{"a runner the kubelet restarted", observed(servingRow), []*corev1.Pod{crashPod("new", 5, 1, runnerDied(died))}, died.Add(2 * time.Second)},
		{"a runner dead and not restarted yet", observed(servingRow), []*corev1.Pod{crashPod("new", 5, 1, runnerStillDead(died))}, died},
		{"a runner the kubelet is backing off", observed(servingRow), []*corev1.Pod{crashPod("new", 5, 1, runnerInCrashLoopBackOff(died.Add(-time.Minute), died))}, died},
		{"a runner that died twice", observed(servingRow), []*corev1.Pod{crashPod("new", 5, 1, runnerDiedTwice(died, died.Add(time.Minute)))}, died.Add(time.Minute + 2*time.Second)},
		{"a restarted runner that served again later anchors at the moment it served", observed(servingRow),
			[]*corev1.Pod{servedAgainAt(crashPod("new", 5, 1, runnerDied(died)), died.Add(15*time.Second))}, died.Add(15 * time.Second)},
		{"a restarted runner whose readiness moved before its death anchors at the restart", observed(servingRow),
			[]*corev1.Pod{servedAgainAt(crashPod("new", 5, 1, runnerDied(died)), ready)}, died.Add(2 * time.Second)},
		{"a runner on its first run that just served is not a restart", observed(servingRow),
			[]*corev1.Pod{servedAgainAt(crashPod("new", 5, 1, runnerRunningSince(died)), died.Add(15*time.Second))}, time.Time{}},
		{"a stable pod's restart is not the canary's", observed(servingRow), []*corev1.Pod{crashPod("old", 5, 1, runnerDied(died))}, time.Time{}},
		{"a pod on its way out", observed(servingRow), []*corev1.Pod{deleting(crashPod("new", 5, 1, runnerDied(died)))}, time.Time{}},
		{"a pod in a terminal phase", observed(servingRow), []*corev1.Pod{stoppedPod("new", 5, 1, 1, "Error", died, corev1.PodFailed)}, time.Time{}},
		{"a sidecar's restart", observed(servingRow), []*corev1.Pod{crashPod("new", 5, 1, sidecarRestarted)}, time.Time{}},
		{"a rebuilt pod that is not serving yet anchors at the death that opened the rebuild", observed(rebuiltRow(5, died, died.Add(time.Minute), 2)),
			[]*corev1.Pod{createdAt(crashPod("new", 5, 2, runnerRunningSince(died.Add(2*time.Second))), died.Add(time.Second))}, died},
		{"a rebuilt pod that serves anchors at the moment it served", observed(rebuiltRow(5, died, died.Add(20*time.Second), 2)),
			[]*corev1.Pod{servedAgainAt(createdAt(crashPod("new", 5, 2, runnerRunningSince(died.Add(2*time.Second))), died.Add(time.Second)), died.Add(20*time.Second))}, died.Add(20 * time.Second)},
		{"a pod created before the failure keeps its first readiness out", observed(rebuiltRow(5, died, died.Add(20*time.Second), 2)),
			[]*corev1.Pod{servedAgainAt(createdAt(crashPod("new", 5, 2, runnerRunningSince(died.Add(2*time.Second))), died.Add(-time.Hour)), died.Add(20*time.Second))}, died},
		{"the newest across the member's pods", observed(servingRow),
			[]*corev1.Pod{crashPod("new", 5, 1, runnerDied(died)), crashPod("new", 6, 1, runnerDied(died.Add(time.Minute)))}, died.Add(time.Minute + 2*time.Second)},
		{"no target revision", observedCanaryRevisions{}, []*corev1.Pod{crashPod("new", 5, 1, runnerDied(died))}, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := latestCanaryRestart(tc.observed, tc.pods); !got.Equal(tc.want) {
				t.Fatalf("restart at %v, want %v", got, tc.want)
			}
		})
	}
}

// TestShouldAdvanceAuto_SoakMeasuresFromTheNewestRestart pins the timed
// gate's anchor: the later of the split first serving and the newest canary
// restart, with a restart before the step left out of it.
func TestShouldAdvanceAuto_SoakMeasuresFromTheNewestRestart(t *testing.T) {
	served := time.Unix(100000, 0)
	cs := &v1beta1.CanaryStatus{StepEnteredTime: &metav1.Time{Time: served}}
	timed := soakedLadder(20 * time.Second)[0]
	bare := soakedLadder(20 * time.Second)[1]
	for _, tc := range []struct {
		name      string
		cs        *v1beta1.CanaryStatus
		step      v1beta1.RolloutGroupStep
		restarted time.Time
		now       time.Time
		want      bool
	}{
		{"no restart, one second short of the soak", cs, timed, time.Time{}, served.Add(19 * time.Second), false},
		{"no restart, the soak elapsed", cs, timed, time.Time{}, served.Add(20 * time.Second), true},
		{"a restart inside the soak holds past it", cs, timed, served.Add(10 * time.Second), served.Add(25 * time.Second), false},
		{"a restart inside the soak, one second short of a soak since it", cs, timed, served.Add(10 * time.Second), served.Add(29 * time.Second), false},
		{"a full soak since the restart", cs, timed, served.Add(10 * time.Second), served.Add(30 * time.Second), true},
		{"a restart after the soak elapsed holds the advance", cs, timed, served.Add(22 * time.Second), served.Add(25 * time.Second), false},
		{"a restart before the step is not this step's", cs, timed, served.Add(-5 * time.Second), served.Add(20 * time.Second), true},
		{"a bare step advances whatever restarted", cs, bare, served.Add(19 * time.Second), served.Add(19 * time.Second), true},
		{"a step that never served has no soak", nil, timed, served, served.Add(time.Hour), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldAdvanceAuto(tc.cs, tc.step, tc.restarted, tc.now); got != tc.want {
				t.Fatalf("advance=%v, want %v", got, tc.want)
			}
		})
	}
}

// createdAt stamps the pod's creation.
func createdAt(pod *corev1.Pod, t time.Time) *corev1.Pod {
	pod.CreationTimestamp = metav1.NewTime(t)
	return pod
}

// servedAgainAt marks the pod Ready with its readiness last moved at t: the
// moment its restarted runner passed its readiness probe.
func servedAgainAt(pod *corev1.Pod, t time.Time) *corev1.Pod {
	readyAgain(pod)
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(t)
		}
	}
	return pod
}

// setRunnerServedAgain rewrites the named pod's runner status and marks it Ready with
// its readiness last moved at readyAt.
func (w *timedCanaryWire) setRunnerServedAgain(t *testing.T, name string, status corev1.ContainerStatus, readyAt time.Time) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{status}
	servedAgainAt(pod, readyAt)
	if err := w.client.Status().Update(w.ctx, pod); err != nil {
		t.Fatal(err)
	}
}

// escalateRow stamps the engine's Instance row index Failed for the pod's
// crash loop at failedAt, as the workload's stuck-pod escalation does while
// the pod itself stays under restart policy None.
func (w *timedCanaryWire) escalateRow(t *testing.T, index int32, failedAt time.Time) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: "soak-wire-engine"}, ir); err != nil {
		t.Fatal(err)
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index != index {
			continue
		}
		ir.Status.InstanceStatuses[i].Phase = v1beta1.OMENativeInstanceFailed
		ir.Status.InstanceStatuses[i].LastFailure = &v1beta1.InstanceTermination{PodName: "target-3", ContainerName: constants.MainContainerName,
			Reason: constants.StateReasonCrashLoopBackOff, Time: metav1.NewTime(failedAt)}
	}
	if err := w.client.Status().Update(w.ctx, ir); err != nil {
		t.Fatal(err)
	}
}

// TestDispatch_RestartedRunnerServesAgainAfterItsStart: the kubelet starts
// the restarted runner at once, but the runner serves only once its startup
// is done and its readiness probe passes, well after its start. The soak
// measures from the moment it serves again: a pass at which a full soak has
// passed since the start, but not since the pod served, holds the step.
func TestDispatch_RestartedRunnerServesAgainAfterItsStart(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	// target-3's runner dies ten seconds in and is restarted at once; its
	// startup takes until +25s, when its readiness probe passes.
	w.setRunnerServedAgain(t, "target-3", runnerDied(t0.Add(10*time.Second)), t0.Add(25*time.Second))
	w.dispatchAt(t, t0.Add(33*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the soak measures from the pod serving again, got step %d phase %q", cs.CurrentStep, phase)
	}
	w.dispatchAt(t, t0.Add(44*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("one second short of a soak since the pod served again, got step %d phase %q", cs.CurrentStep, phase)
	}
	w.dispatchAt(t, t0.Add(45*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 1 || phase != v1beta1.RolloutPhasePending {
		t.Fatalf("a full soak since the pod served again must advance, got step %d phase %q", cs.CurrentStep, phase)
	}
}

// TestDispatch_SecondDeathOnAnEscalatedRowParksAtTheStep: the workload's
// stuck-pod escalation stamps the Instance row Failed for the crash loop
// before the pass that reads the second death. The row's verdict and the
// pod's agree: the ladder parks at the soaking step with the pod named.
func TestDispatch_SecondDeathOnAnEscalatedRowParksAtTheStep(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	w.setRunnerServedAgain(t, "target-3", runnerDied(t0.Add(10*time.Second)), t0.Add(25*time.Second))
	w.dispatchAt(t, t0.Add(33*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("a restart inside the soak must hold the step, got step %d phase %q", cs.CurrentStep, phase)
	}

	// The restarted run dies at +35s; the kubelet backs off, and the
	// workload escalates the row before the canary's pass reads the pod.
	w.setRunner(t, "target-3", runnerInCrashLoopBackOff(t0.Add(12*time.Second), t0.Add(35*time.Second)), false)
	w.escalateRow(t, 3, t0.Add(36*time.Second))
	w.dispatchAt(t, t0.Add(37*time.Second))
	cs, phase := w.status()
	if phase != v1beta1.RolloutPhaseFailed || cs.Failed == nil || cs.CurrentStep != 0 {
		t.Fatalf("a second death the workload already escalated must still park at the soaking step, got phase %q status %+v", phase, cs)
	}
	select {
	case ev := <-w.rec.Events:
		if !strings.Contains(ev, EventReasonCanaryPodCrashed) || !strings.Contains(ev, "target-3") {
			t.Fatalf("the park must name the pod that died, got %q", ev)
		}
	default:
		t.Fatal("the park must be announced with an event")
	}
}

// rebuildInstance replaces the named pod with the restart policy's rebuilt
// pod for Instance index, created at createdAt with a new UID and no
// restart, still starting, and records the death that opened the rebuild
// on the row, which is rebuilding at its next incarnation.
func (w *timedCanaryWire) rebuildInstance(t *testing.T, name string, index int32, failedAt, createdAt time.Time) {
	t.Helper()
	old := &corev1.Pod{}
	if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: name}, old); err != nil {
		t.Fatal(err)
	}
	if err := w.client.Delete(w.ctx, old); err != nil {
		t.Fatal(err)
	}
	fresh := canaryPod(w.ns, w.isvc.Name, "engine", "target", name)
	fresh.UID = types.UID(name + "-rebuilt")
	fresh.CreationTimestamp = metav1.NewTime(createdAt)
	fresh.Labels[query.LabelInstanceIncarnation] = "2"
	fresh.Status.ContainerStatuses = []corev1.ContainerStatus{runnerRunningSince(createdAt.Add(time.Second))}
	notReady(fresh)
	if err := w.client.Create(w.ctx, fresh); err != nil {
		t.Fatal(err)
	}
	w.mutateRow(t, index, func(row *v1beta1.OMENativeInstanceStatus) {
		row.Phase = v1beta1.OMENativeInstanceRestarting
		row.Incarnation = 2
		exit := int32(1)
		row.LastFailure = &v1beta1.InstanceTermination{PodName: name, ContainerName: constants.MainContainerName,
			Reason: constants.StateReasonError, ExitCode: &exit, Time: metav1.NewTime(failedAt)}
	})
}

// rebuiltServes marks the rebuilt pod Ready at readyAt and its row Ready
// since then.
func (w *timedCanaryWire) rebuiltServes(t *testing.T, name string, index int32, readyAt time.Time) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: name}, pod); err != nil {
		t.Fatal(err)
	}
	servedAgainAt(pod, readyAt)
	if err := w.client.Status().Update(w.ctx, pod); err != nil {
		t.Fatal(err)
	}
	w.mutateRow(t, index, func(row *v1beta1.OMENativeInstanceStatus) {
		row.Phase = v1beta1.OMENativeInstanceReady
		row.ReadySince = &metav1.Time{Time: readyAt}
	})
}

func (w *timedCanaryWire) mutateRow(t *testing.T, index int32, mutate func(*v1beta1.OMENativeInstanceStatus)) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := w.client.Get(w.ctx, types.NamespacedName{Namespace: w.ns, Name: "soak-wire-engine"}, ir); err != nil {
		t.Fatal(err)
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == index {
			mutate(&ir.Status.InstanceStatuses[i])
		}
	}
	if err := w.client.Status().Update(w.ctx, ir); err != nil {
		t.Fatal(err)
	}
}

// TestDispatch_RebuiltPodServesAfterTheFailure: under a restart policy that
// rebuilds the Instance, the pod that died is gone and its replacement
// carries no restart; the row keeps the death. The soak measures from the
// moment the replacement serves, not from the death it answers.
func TestDispatch_RebuiltPodServesAfterTheFailure(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	w.rebuildInstance(t, "target-3", 3, t0.Add(10*time.Second), t0.Add(11*time.Second))
	w.dispatchAt(t, t0.Add(12*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("a rebuild in flight is a dip that keeps the step, got step %d phase %q", cs.CurrentStep, phase)
	}

	// The replacement serves twenty seconds after the death.
	w.rebuiltServes(t, "target-3", 3, t0.Add(30*time.Second))
	w.dispatchAt(t, t0.Add(35*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the soak measures from the replacement serving, got step %d phase %q", cs.CurrentStep, phase)
	}
	w.dispatchAt(t, t0.Add(49*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("one second short of a soak since the replacement served, got step %d phase %q", cs.CurrentStep, phase)
	}
	w.dispatchAt(t, t0.Add(50*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 1 || phase != v1beta1.RolloutPhasePending {
		t.Fatalf("a full soak since the replacement served must advance, got step %d phase %q", cs.CurrentStep, phase)
	}
}

// TestDispatch_RebuiltPodDyingAgainInsideItsSoakParksAtTheStep: the
// replacement serves and dies inside the soak its serving began: the
// Instance's second death under one repair parks the ladder at the step
// that was soaking.
func TestDispatch_RebuiltPodDyingAgainInsideItsSoakParksAtTheStep(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	w.rebuildInstance(t, "target-3", 3, t0.Add(10*time.Second), t0.Add(11*time.Second))
	w.rebuiltServes(t, "target-3", 3, t0.Add(30*time.Second))
	w.dispatchAt(t, t0.Add(35*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the soak measures from the replacement serving, got step %d phase %q", cs.CurrentStep, phase)
	}

	w.setRunner(t, "target-3", runnerStillDead(t0.Add(40*time.Second)), false)
	w.dispatchAt(t, t0.Add(41*time.Second))
	cs, phase := w.status()
	if phase != v1beta1.RolloutPhaseFailed || cs.Failed == nil || cs.CurrentStep != 0 {
		t.Fatalf("a rebuilt pod that dies again inside its soak must park at the soaking step, got phase %q status %+v", phase, cs)
	}
	select {
	case ev := <-w.rec.Events:
		if !strings.Contains(ev, EventReasonCanaryPodCrashed) || !strings.Contains(ev, "target-3") {
			t.Fatalf("the park must name the pod that died, got %q", ev)
		}
	default:
		t.Fatal("the park must be announced with an event")
	}
}

// TestDispatch_FirstDeathAtTheInstanceReadyStampIsOneDeath replays the
// surged canary Instance whose runner exits as the surge completes: the row
// enters Ready as the old pod finishes draining, in the same second the new
// runner dies, and the kubelet restarts it in that second too. Neither the
// workload nor the canary dates that death after the Instance served. The
// restarted runner serves and dies again, and the kubelet backs off: that
// is the step's first death, a restart the soak waits out; the policy's
// rebuild answers it, and the rebuilt pod's own death parks the step.
func TestDispatch_FirstDeathAtTheInstanceReadyStampIsOneDeath(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	// The surge completes ten seconds after the split served, as the runner
	// dies and is restarted in place; the restarted runner serves and dies
	// thirty-one seconds later, and the kubelet backs off.
	stamp := t0.Add(10 * time.Second)
	w.mutateRow(t, 3, func(row *v1beta1.OMENativeInstanceStatus) { row.ReadySince = &metav1.Time{Time: stamp} })
	w.setRunner(t, "target-3", runnerInCrashLoopBackOff(stamp, stamp.Add(31*time.Second)), false)
	w.dispatchAt(t, stamp.Add(32*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("one death after the Instance served is a restart, not a loop, got step %d phase %q (%+v)", cs.CurrentStep, phase, cs)
	}

	// The policy rebuilds the Instance for that death; the replacement
	// serves twenty seconds later and the soak measures from it.
	w.rebuildInstance(t, "target-3", 3, stamp.Add(31*time.Second), stamp.Add(33*time.Second))
	w.dispatchAt(t, stamp.Add(34*time.Second))
	w.rebuiltServes(t, "target-3", 3, stamp.Add(53*time.Second))
	w.dispatchAt(t, stamp.Add(60*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the rebuilt pod is soaking, got step %d phase %q", cs.CurrentStep, phase)
	}

	// The replacement dies inside its soak: the Instance's second death.
	w.setRunner(t, "target-3", runnerStillDead(stamp.Add(63*time.Second)), false)
	w.dispatchAt(t, stamp.Add(64*time.Second))
	if cs, phase := w.status(); phase != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 0 {
		t.Fatalf("the rebuilt pod's death parks the soaking step, got step %d phase %q", cs.CurrentStep, phase)
	}
}

// TestDispatch_DyingPodAndRebuildStampInOnePassIsOneDeath: the pass that
// reads the pod's first death also sees the policy's rebuild stamp on the
// row, and the next pass sees only the rebuilt pod. One death, carried by
// the pod and by the row, is one restart: step 0 stays Paused until a
// second death inside the soak.
func TestDispatch_DyingPodAndRebuildStampInOnePassIsOneDeath(t *testing.T) {
	t0 := time.Unix(100000, 0)
	w := armTimedCanaryWire(t, 20*time.Second, t0)

	died := t0.Add(10 * time.Second)
	w.setRunner(t, "target-3", runnerDied(died), false)
	w.mutateRow(t, 3, func(row *v1beta1.OMENativeInstanceStatus) {
		row.Phase = v1beta1.OMENativeInstanceRestarting
		row.Incarnation = 2
		exit := int32(1)
		row.LastFailure = &v1beta1.InstanceTermination{PodName: "target-3", ContainerName: constants.MainContainerName,
			Reason: constants.StateReasonError, ExitCode: &exit, Time: metav1.NewTime(died)}
	})
	w.dispatchAt(t, died.Add(time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the dying pod and the rebuild's stamp carry one death, got step %d phase %q (%+v)", cs.CurrentStep, phase, cs)
	}

	w.rebuildInstance(t, "target-3", 3, died, died.Add(2*time.Second))
	w.dispatchAt(t, died.Add(3*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the rebuilt pod alone carries no death, got step %d phase %q", cs.CurrentStep, phase)
	}

	w.rebuiltServes(t, "target-3", 3, died.Add(20*time.Second))
	w.dispatchAt(t, died.Add(35*time.Second))
	if cs, phase := w.status(); cs.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
		t.Fatalf("the rebuilt pod is soaking, got step %d phase %q", cs.CurrentStep, phase)
	}

	w.setRunner(t, "target-3", runnerStillDead(died.Add(38*time.Second)), false)
	w.dispatchAt(t, died.Add(39*time.Second))
	if cs, phase := w.status(); phase != v1beta1.RolloutPhaseFailed || cs.CurrentStep != 0 {
		t.Fatalf("a second death inside the soak parks the soaking step, got step %d phase %q", cs.CurrentStep, phase)
	}
}
