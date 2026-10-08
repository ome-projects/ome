package evidence_test

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPodStuckInTerminalWaiting pins the wait-window + reason classifier:
// fires only on terminal kubelet waiting reasons, after grace, and
// not on freshly-created pods (which legitimately pass through
// transient ContainerCreating).
func TestPodStuckInTerminalWaiting(t *testing.T) {
	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	transient := corev1.ContainerStateWaiting{Reason: "ContainerCreating"}

	cases := []struct {
		name       string
		pod        *corev1.Pod
		wantStuck  bool
		wantReason string
	}{
		{
			name:      "nil pod returns (false, '')",
			pod:       nil,
			wantStuck: false,
		},
		{
			name: "pod with empty CreationTimestamp returns (false, '')",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck: false,
		},
		{
			name: "within grace: not stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck: false,
		},
		{
			name: "past grace, terminal reason: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck:  true,
			wantReason: "ImagePullBackOff",
		},
		{
			name: "past grace, transient reason: not stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &transient}}},
				},
			},
			wantStuck: false,
		},
		{
			name: "init container stuck counts too",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck:  true,
			wantReason: "ImagePullBackOff",
		},
		// CrashLoopBackOff is the steady-state for a container
		// that pulls cleanly but exits immediately. kubelet's restart
		// backoff caps at ~5 min/attempt; without the carve-out the
		// rollout would sit at Phase=Updating until the Deadline fired.
		{
			name: "past grace, CrashLoopBackOff: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}},
				},
			},
			wantStuck:  true,
			wantReason: "CrashLoopBackOff",
		},
		// RunContainerError fires on runtime-rejected starts (missing
		// .so, exec-format mismatch). Same permanence as ImagePullBackOff.
		{
			name: "past grace, RunContainerError: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "RunContainerError"}}}},
				},
			},
			wantStuck:  true,
			wantReason: "RunContainerError",
		},
		{
			name: "past grace, CreateContainerError: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerError"}}}},
				},
			},
			wantStuck:  true,
			wantReason: "CreateContainerError",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			reason, stuck := evidence.PodStuckInTerminalWaiting(tt.pod, now, 2*time.Second)
			if stuck != tt.wantStuck {
				t.Errorf("stuck: got %v want %v", stuck, tt.wantStuck)
			}
			if reason != tt.wantReason {
				t.Errorf("reason: got %q want %q", reason, tt.wantReason)
			}
		})
	}
}

// TestFirstStuckPodForInstance_SkipsDeletingPods pins the per-Instance escalator probe:
// skips nil pods + pods marked for deletion + non-stuck pods, returns
// the first eligible stuck pod + reason.
func TestFirstStuckPodForInstance_SkipsDeletingPods(t *testing.T) {
	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	mkPod := func(name string, age time.Duration, deleting bool, reason *corev1.ContainerStateWaiting) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.NewTime(now.Add(-age)),
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: reason}}},
			},
		}
		if deleting {
			t := metav1.NewTime(now)
			p.ObjectMeta.DeletionTimestamp = &t
		}
		return p
	}

	pods := []*corev1.Pod{
		nil, // skipped
		mkPod("deleting", 10*time.Second, true, &terminal), // skipped (deletion)
		mkPod("ok", 10*time.Second, false, nil),            // skipped (no Waiting)
		mkPod("stuck", 10*time.Second, false, &terminal),   // first eligible
	}
	got, reason := evidence.FirstStuckPodForInstance(pods, now, 2*time.Second)
	if got == nil || got.Name != "stuck" {
		t.Errorf("got %+v want pod 'stuck'", got)
	}
	if reason != "ImagePullBackOff" {
		t.Errorf("reason: got %q want ImagePullBackOff", reason)
	}

	// No stuck pods: returns (nil, "").
	none, noneReason := evidence.FirstStuckPodForInstance(
		[]*corev1.Pod{mkPod("ok1", 10*time.Second, false, nil)},
		now, 2*time.Second,
	)
	if none != nil || noneReason != "" {
		t.Errorf("no stuck pods: got (%v, %q) want (nil, '')", none, noneReason)
	}
}

// TestFirstCrashLoopingPod_ReadsBothFacesOfTheLoop pins the crash-loop
// reader on the shapes a pass can catch a looping container in: parked
// in CrashLoopBackOff, Running again between two crashes with the
// restarts and a non-zero last termination on its status, or just
// exited non-zero and awaiting the restart. A container that restarted
// and is Ready again has recovered; a clean restart proves nothing; a
// pod of another revision is not the attempt's.
func TestFirstCrashLoopingPod_ReadsBothFacesOfTheLoop(t *testing.T) {
	crashed := &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}
	clean := &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}
	podWith := func(name string, cs corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{cs}},
		}
	}
	running := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"parked in CrashLoopBackOff", podWith("a", corev1.ContainerStatus{Name: "main",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}), true},
		{"running between two crashes", podWith("b", corev1.ContainerStatus{Name: "main",
			State: running, RestartCount: 2, LastTerminationState: corev1.ContainerState{Terminated: crashed}}), true},
		{"exited non-zero, restart pending", podWith("c", corev1.ContainerStatus{Name: "main",
			State: corev1.ContainerState{Terminated: crashed}}), true},
		{"restarted and ready again", podWith("d", corev1.ContainerStatus{Name: "main", Ready: true,
			State: running, RestartCount: 2, LastTerminationState: corev1.ContainerState{Terminated: crashed}}), false},
		{"clean restart", podWith("e", corev1.ContainerStatus{Name: "main",
			State: running, RestartCount: 1, LastTerminationState: corev1.ContainerState{Terminated: clean}}), false},
		{"running, never restarted", podWith("f", corev1.ContainerStatus{Name: "main", State: running}), false},
		{"waiting on an image pull", podWith("g", corev1.ContainerStatus{Name: "main",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod, reason := evidence.FirstCrashLoopingPod([]*corev1.Pod{tc.pod}, "")
			if (pod != nil) != tc.want {
				t.Fatalf("crash-looping: got pod=%v reason=%q want found=%v", pod != nil, reason, tc.want)
			}
			if tc.want && reason != evidence.ReasonCrashLoop {
				t.Errorf("reason: got %q want %q", reason, evidence.ReasonCrashLoop)
			}
		})
	}

	// The attempt's scope: a crash on another revision is not its own.
	other := podWith("h", corev1.ContainerStatus{Name: "main",
		State: running, RestartCount: 2, LastTerminationState: corev1.ContainerState{Terminated: crashed}})
	other.Labels = map[string]string{"ome.io/revision-hash": "oldhash"}
	if pod, _ := evidence.FirstCrashLoopingPod([]*corev1.Pod{other}, "own-engine-newhash"); pod != nil {
		t.Errorf("a crash on another revision must not be read as the attempt's: got %s", pod.Name)
	}
	deleting := podWith("i", corev1.ContainerStatus{Name: "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}})
	now := metav1.NewTime(time.Now())
	deleting.DeletionTimestamp = &now
	if pod, _ := evidence.FirstCrashLoopingPod([]*corev1.Pod{deleting}, ""); pod != nil {
		t.Errorf("a pod on its way out is not evidence: got %s", pod.Name)
	}
}

// servedThenParkedPod is a pod created long ago whose containers were
// ready until brokeAgo, when the runner parked in reason: the shape of
// a promoted pod that crashes after it served.
func servedThenParkedPod(now time.Time, reason string, brokeAgo time.Duration) *corev1.Pod {
	broke := metav1.NewTime(now.Add(-brokeAgo))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: broke},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: broke},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "main",
				RestartCount: 1,
				State:        corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1, Reason: "Error", FinishedAt: broke,
				}},
			}},
		},
	}
}

// The escalation's stuck reading runs the grace from the start of the
// pod's current waiting episode, not from its creation. A pod that served
// and then broke is not stuck until the grace has elapsed since its
// containers stopped being ready; one that broke before the grace is
// stuck as a never-ready pod of its age is, and a never-ready pod is
// still measured from its creation. The repair's wedge reading of the
// same pods measures their age: a pod older than the grace that parks is
// wedged at once, and it is the wedge reading the grace left belongs to.
func TestPodStuckInTerminalWaiting_MeasuresFromTheWaitingEpisode(t *testing.T) {
	now := time.Now()
	const grace = 30 * time.Second
	neverReady := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
		Status: corev1.PodStatus{
			Conditions:        []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))}},
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}},
		},
	}
	noConditions := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}},
		},
	}
	young := servedThenParkedPod(now, "CrashLoopBackOff", 5*time.Second)
	young.CreationTimestamp = metav1.NewTime(now.Add(-10 * time.Second))
	for _, tc := range []struct {
		name       string
		pod        *corev1.Pod
		wantStuck  bool
		wantWedged bool
		wantLeft   time.Duration
	}{
		{name: "old pod, broke five seconds ago", pod: servedThenParkedPod(now, "CrashLoopBackOff", 5*time.Second), wantWedged: true},
		{name: "old pod, broke at the grace", pod: servedThenParkedPod(now, "CrashLoopBackOff", grace), wantStuck: true, wantWedged: true},
		{name: "old pod, broke before the grace", pod: servedThenParkedPod(now, "CrashLoopBackOff", 2*time.Minute), wantStuck: true, wantWedged: true},
		{name: "old pod, never ready", pod: neverReady, wantStuck: true, wantWedged: true},
		{name: "old pod, no conditions reported", pod: noConditions, wantStuck: true, wantWedged: true},
		{name: "young pod, broke five seconds ago", pod: young, wantLeft: 20 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, stuck := evidence.PodStuckInTerminalWaiting(tc.pod, now, grace); stuck != tc.wantStuck {
				t.Errorf("stuck = %v, want %v", stuck, tc.wantStuck)
			}
			if _, wedged := evidence.PodWedgedPastGrace(tc.pod, now, grace); wedged != tc.wantWedged {
				t.Errorf("wedged = %v, want %v", wedged, tc.wantWedged)
			}
			if left := evidence.TerminalWaitingGraceLeft(tc.pod, now, grace); left != tc.wantLeft {
				t.Errorf("grace left = %s, want %s", left, tc.wantLeft)
			}
		})
	}
}

// WaitingEpisodeStart is the later of the pod's creation and the moment
// its containers last stopped being ready; a pod that cannot prove a
// duration has no start.
func TestWaitingEpisodeStart(t *testing.T) {
	now := time.Now()
	served := servedThenParkedPod(now, "CrashLoopBackOff", 5*time.Second)
	if got, want := evidence.WaitingEpisodeStart(served), now.Add(-5*time.Second); !got.Equal(want) {
		t.Errorf("served then broke: start = %v, want the break at %v", got, want)
	}
	fresh := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-time.Minute))}}
	if got, want := evidence.WaitingEpisodeStart(fresh), now.Add(-time.Minute); !got.Equal(want) {
		t.Errorf("never ready: start = %v, want the creation at %v", got, want)
	}
	ready := servedThenParkedPod(now, "CrashLoopBackOff", 5*time.Second)
	ready.Status.Conditions[1].Status = corev1.ConditionTrue
	if got, want := evidence.WaitingEpisodeStart(ready), now.Add(-time.Hour); !got.Equal(want) {
		t.Errorf("containers ready: start = %v, want the creation at %v", got, want)
	}
	if got := evidence.WaitingEpisodeStart(nil); !got.IsZero() {
		t.Errorf("nil pod: start = %v, want zero", got)
	}
	if got := evidence.WaitingEpisodeStart(&corev1.Pod{}); !got.IsZero() {
		t.Errorf("no creation stamp: start = %v, want zero", got)
	}
}

// A Restart is judged on the pods at the row's incarnation: the set of
// the incarnation before it is what the repair drains and deletes, so a
// wedge on one of those is the repair's reason, never its failure. A pod
// with no incarnation label stays in scope, an Update keeps its
// revision scope, and any other attempt is judged on every pod.
func TestAttemptStuckPods_RestartIsJudgedOnItsOwnIncarnation(t *testing.T) {
	podAt := func(name, incarnation string) *corev1.Pod {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
		if incarnation != "" {
			pod.Labels["ome.io/instance-incarnation"] = incarnation
		}
		return pod
	}
	old, rebuilt, unlabelled := podAt("old", "1"), podAt("rebuilt", "2"), podAt("unlabelled", "")
	pods := []*corev1.Pod{old, rebuilt, unlabelled}
	names := func(pods []*corev1.Pod) []string {
		out := make([]string, 0, len(pods))
		for _, pod := range pods {
			out = append(out, pod.Name)
		}
		return out
	}
	restarting := types.InstanceStatus{Index: 0, Incarnation: 2, Phase: types.InstancePhaseRestarting,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart}}
	if got := names(evidence.AttemptStuckPods(restarting, pods, "")); strings.Join(got, ",") != "rebuilt,unlabelled" {
		t.Errorf("Restart at incarnation 2 is judged on %v, want the rebuilt and the unlabelled pod", got)
	}
	creating := types.InstanceStatus{Index: 0, Incarnation: 2, Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate}}
	if got := names(evidence.AttemptStuckPods(creating, pods, "")); len(got) != 3 {
		t.Errorf("Create is judged on %v, want every pod", got)
	}
	settled := types.InstanceStatus{Index: 0, Incarnation: 2, Phase: types.InstancePhaseReady}
	if got := names(evidence.AttemptStuckPods(settled, pods, "")); len(got) != 3 {
		t.Errorf("a settled row is judged on %v, want every pod", got)
	}
}

// TestPodUnreadyPastGrace: a promoted pod whose containers have stopped
// passing readiness for at least the grace is unready past it. Inside the
// grace, with no grace configured, before the pod ever served, under a
// node that stopped reporting (Ready withdrawn while the kubelet's own
// ContainersReady still says True), while serving, deleting or terminal,
// it is not.
func TestPodUnreadyPastGrace(t *testing.T) {
	grace := time.Minute
	past := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	inside := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-30*time.Second))
	nodeLost := servingPod("engine-0-default-0")
	for i := range nodeLost.Status.Conditions {
		if nodeLost.Status.Conditions[i].Type == corev1.PodReady {
			nodeLost.Status.Conditions[i].Status = corev1.ConditionFalse
			nodeLost.Status.Conditions[i].LastTransitionTime = metav1.NewTime(tNow.Add(-2 * time.Minute))
		}
	}
	deleting := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	ts := metav1.NewTime(tNow)
	deleting.DeletionTimestamp = &ts
	terminal := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	terminal.Status.Phase = corev1.PodFailed

	for _, tc := range []struct {
		name  string
		pod   *corev1.Pod
		grace time.Duration
		want  bool
	}{
		{name: "unready past the grace", pod: past, grace: grace, want: true},
		{name: "unready inside the grace", pod: inside, grace: grace},
		{name: "no grace configured", pod: past},
		{name: "never promoted", pod: runningNotReadyPod("engine-0-default-0", tNow.Add(-2*time.Minute)), grace: grace},
		{name: "node stopped reporting", pod: nodeLost, grace: grace},
		{name: "serving", pod: servingPod("engine-0-default-0"), grace: grace},
		{name: "deleting", pod: deleting, grace: grace},
		{name: "terminal", pod: terminal, grace: grace},
		{name: "nil", pod: nil, grace: grace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.PodUnreadyPastGrace(tc.pod, tNow, tc.grace); got != tc.want {
				t.Fatalf("PodUnreadyPastGrace = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUnreadyGraceLeft pins the per-pod wait: the grace left counts from
// the containers' last transition out of readiness, is zero once past,
// and needs a promoted pod that is unready.
func TestUnreadyGraceLeft(t *testing.T) {
	grace := time.Minute
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want time.Duration
	}{
		{name: "ten seconds into a minute", pod: unreadyAfterServingPod("engine-0-default-0", tNow.Add(-10*time.Second)), want: 50 * time.Second},
		{name: "past the grace", pod: unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))},
		{name: "never promoted", pod: runningNotReadyPod("engine-0-default-0", tNow.Add(-10*time.Second))},
		{name: "serving", pod: servingPod("engine-0-default-0")},
		{name: "nil", pod: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.UnreadyGraceLeft(tc.pod, tNow, grace); got != tc.want {
				t.Fatalf("UnreadyGraceLeft = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestContainersUnreadyPastGrace pins the gate-agnostic reading: a running
// pod whose kubelet readiness has failed for the grace reads unready
// whether its serving gate is on, as a promoted break leaves it, or off,
// as a drain leaves it; inside the grace, with no grace, serving, deleting,
// terminal or with Ready alone withdrawn it does not.
func TestContainersUnreadyPastGrace(t *testing.T) {
	grace := time.Minute
	nodeLost := servingPod("engine-0-default-0")
	for i := range nodeLost.Status.Conditions {
		if nodeLost.Status.Conditions[i].Type == corev1.PodReady {
			nodeLost.Status.Conditions[i].Status = corev1.ConditionFalse
			nodeLost.Status.Conditions[i].LastTransitionTime = metav1.NewTime(tNow.Add(-2 * time.Minute))
		}
	}
	deleting := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	ts := metav1.NewTime(tNow)
	deleting.DeletionTimestamp = &ts
	terminal := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	terminal.Status.Phase = corev1.PodFailed
	for _, tc := range []struct {
		name  string
		pod   *corev1.Pod
		grace time.Duration
		want  bool
	}{
		{name: "promoted, unready past the grace", pod: unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute)), grace: grace, want: true},
		{name: "drained, unready past the grace", pod: runningNotReadyPod("engine-0-default-0", tNow.Add(-2*time.Minute)), grace: grace, want: true},
		{name: "unready inside the grace", pod: unreadyAfterServingPod("engine-0-default-0", tNow.Add(-30*time.Second)), grace: grace},
		{name: "no grace configured", pod: unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))},
		{name: "node stopped reporting", pod: nodeLost, grace: grace},
		{name: "serving", pod: servingPod("engine-0-default-0"), grace: grace},
		{name: "deleting", pod: deleting, grace: grace},
		{name: "terminal", pod: terminal, grace: grace},
		{name: "nil", pod: nil, grace: grace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.ContainersUnreadyPastGrace(tc.pod, tNow, tc.grace); got != tc.want {
				t.Fatalf("ContainersUnreadyPastGrace = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPodStuckInTerminalWaitingSince pins the floor: a waiting episode
// older than the floor is measured from the floor, so a pod wedged long
// before an attempt began acting on it is not stuck for that attempt until
// the grace has run from the floor; an episode younger than the floor and
// a zero floor measure from the episode alone.
func TestPodStuckInTerminalWaitingSince(t *testing.T) {
	now := time.Now()
	const grace = time.Minute
	wedgedFor := func(age time.Duration) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
			Status: corev1.PodStatus{
				Conditions:        []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-age))}},
				ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}},
			},
		}
	}
	for _, tc := range []struct {
		name      string
		pod       *corev1.Pod
		floor     time.Time
		wantStuck bool
	}{
		{name: "old episode, floor inside the grace: not stuck", pod: wedgedFor(10 * time.Minute), floor: now.Add(-5 * time.Second)},
		{name: "old episode, floor past the grace: stuck", pod: wedgedFor(10 * time.Minute), floor: now.Add(-2 * time.Minute), wantStuck: true},
		{name: "episode younger than the floor: measured from the episode", pod: wedgedFor(5 * time.Second), floor: now.Add(-time.Hour)},
		{name: "zero floor: measured from the episode", pod: wedgedFor(10 * time.Minute), wantStuck: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, stuck := evidence.PodStuckInTerminalWaitingSince(tc.pod, now, grace, tc.floor)
			if stuck != tc.wantStuck {
				t.Fatalf("stuck: got %v (%q) want %v", stuck, reason, tc.wantStuck)
			}
			if stuck && reason != "CrashLoopBackOff" {
				t.Errorf("reason: got %q want CrashLoopBackOff", reason)
			}
			if pod, _ := evidence.FirstStuckPodForInstanceSince([]*corev1.Pod{tc.pod}, now, grace, tc.floor); (pod != nil) != tc.wantStuck {
				t.Errorf("FirstStuckPodForInstanceSince: got pod=%v want stuck=%v", pod != nil, tc.wantStuck)
			}
		})
	}
}
