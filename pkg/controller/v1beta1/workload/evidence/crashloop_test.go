package evidence_test

// The crash-loop wedge on a row no operation owns. The shape is narrow on
// purpose: a Ready row with no operation, a pod on the revision the row
// runs (the Component's current revision when it records none), wedged
// past the configured grace, and a pod set that is not fully serving.

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const clRevision = "engine-abc123"

// crashLoopPod is a pod of the current revision parked in
// CrashLoopBackOff since age ago.
func crashLoopPod(name string, age time.Duration, hash string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(tNow.Add(-age)),
			Labels:            map[string]string{query.LabelRevisionHash: hash},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}},
		},
	}
}

func crashLoopInput(grace time.Duration) types.ReconcileInput {
	return types.ReconcileInput{
		Clock:         clocktesting.NewFakeClock(tNow),
		StuckPodGrace: grace,
		ObservedState: types.WorkloadObservedState{CurrentRevision: clRevision},
	}
}

func TestCrashLoopWedge_NamesAStuckPodOnTheCurrentRevision(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	ready := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	wedged := crashLoopPod("engine-0-default-0", 10*time.Minute, hash)

	for _, tc := range []struct {
		name  string
		input types.ReconcileInput
		row   *types.InstanceStatus
		pods  []*corev1.Pod
		want  bool
	}{
		{
			name:  "Ready row, wedged pod on the current revision",
			input: crashLoopInput(time.Minute),
			row:   ready,
			pods:  []*corev1.Pod{wedged},
			want:  true,
		},
		{
			name:  "unset grace disables the shape",
			input: crashLoopInput(0),
			row:   ready,
			pods:  []*corev1.Pod{wedged},
		},
		{
			name:  "still inside the grace",
			input: crashLoopInput(time.Hour),
			row:   ready,
			pods:  []*corev1.Pod{wedged},
		},
		{
			name:  "an operation in flight owns the row",
			input: crashLoopInput(time.Minute),
			row: &types.InstanceStatus{
				Index: 0, Phase: types.InstancePhaseReady,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate},
			},
			pods: []*corev1.Pod{wedged},
		},
		{
			name:  "a row that is not Ready is some other pass's",
			input: crashLoopInput(time.Minute),
			row:   &types.InstanceStatus{Index: 0, Phase: types.InstancePhasePending},
			pods:  []*corev1.Pod{wedged},
		},
		{
			name:  "an off-revision wedge is a leftover the escalation owns",
			input: crashLoopInput(time.Minute),
			row:   ready,
			pods:  []*corev1.Pod{crashLoopPod("engine-0-default-0", 10*time.Minute, "deadbeef")},
		},
		{
			name:  "a deleting pod is on its way out",
			input: crashLoopInput(time.Minute),
			row:   ready,
			pods: func() []*corev1.Pod {
				p := crashLoopPod("engine-0-default-0", 10*time.Minute, hash)
				ts := metav1.NewTime(tNow)
				p.DeletionTimestamp = &ts
				return []*corev1.Pod{p}
			}(),
		},
		{
			name:  "no row at all",
			input: crashLoopInput(time.Minute),
			row:   nil,
			pods:  []*corev1.Pod{wedged},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, got, graceLeft := evidence.CrashLoopWedge(tc.input, tc.row, 1, tc.pods)
			if got != tc.want {
				t.Fatalf("wedged: got %v (%q) want %v", got, reason, tc.want)
			}
			if got && reason == "" {
				t.Error("a wedge must name itself for the repair's reason")
			}
			if got && graceLeft != 0 {
				t.Errorf("a wedge past the grace reports %s of grace left, want none", graceLeft)
			}
		})
	}
}

// TestCrashLoopWedge_FullyServingSetIsNeverAWedge: the Instance is
// answering traffic, so a container waiting reason is not grounds to
// recycle it.
func TestCrashLoopWedge_FullyServingSetIsNeverAWedge(t *testing.T) {
	serving := servingPod("engine-0-default-0")
	serving.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(clRevision).Hash()}
	serving.Status.ContainerStatuses = append(serving.Status.ContainerStatuses, corev1.ContainerStatus{
		Name:  "sidecar",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	})
	row := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	if _, got, _ := evidence.CrashLoopWedge(crashLoopInput(time.Minute), row, 1, []*corev1.Pod{serving}); got {
		t.Error("fully serving pod set: got a wedge, want none")
	}
}

// TestCrashLoopWedge_ReportsTheGraceLeftOnAParkedPod: a pod parked in a
// terminal waiting reason inside the grace is not a wedge yet, and the
// reading says how long until it is. A gang reports the earliest grace
// end among its parked members, in either order; a pod that is merely
// not ready, an off-revision leftover, a deleting pod and a fully
// serving set report none, as does a wedge already past the grace.
func TestCrashLoopWedge_ReportsTheGraceLeftOnAParkedPod(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	ready := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	notReady := crashLoopPod("engine-0-default-0", 10*time.Second, hash)
	notReady.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	deleting := crashLoopPod("engine-0-default-0", 10*time.Second, hash)
	ts := metav1.NewTime(tNow)
	deleting.DeletionTimestamp = &ts

	for _, tc := range []struct {
		name     string
		expected int32
		pods     []*corev1.Pod
		want     time.Duration
	}{
		{name: "ten seconds into a minute", expected: 1, pods: []*corev1.Pod{crashLoopPod("engine-0-default-0", 10*time.Second, hash)}, want: 50 * time.Second},
		{name: "gang, leader older", expected: 2, pods: []*corev1.Pod{crashLoopPod("engine-0-leader-0", 40*time.Second, hash), crashLoopPod("engine-0-worker-0", 10*time.Second, hash)}, want: 20 * time.Second},
		{name: "gang, worker older", expected: 2, pods: []*corev1.Pod{crashLoopPod("engine-0-leader-0", 10*time.Second, hash), crashLoopPod("engine-0-worker-0", 40*time.Second, hash)}, want: 20 * time.Second},
		{name: "gang, one member past the grace", expected: 2, pods: []*corev1.Pod{crashLoopPod("engine-0-leader-0", 10*time.Second, hash), crashLoopPod("engine-0-worker-0", 2*time.Minute, hash)}},
		{name: "running, not yet ready", expected: 1, pods: []*corev1.Pod{notReady}},
		{name: "off-revision leftover", expected: 1, pods: []*corev1.Pod{crashLoopPod("engine-0-default-0", 10*time.Second, "deadbeef")}},
		{name: "deleting pod", expected: 1, pods: []*corev1.Pod{deleting}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, got := evidence.CrashLoopWedge(crashLoopInput(time.Minute), ready, tc.expected, tc.pods)
			if got != tc.want {
				t.Fatalf("grace left = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestTerminalWaitingGraceLeft pins the per-pod reading: the grace left
// counts from the pod's creation, is zero once past, and needs both a
// terminal waiting reason and a creation stamp.
func TestTerminalWaitingGraceLeft(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	unstamped := crashLoopPod("engine-0-default-0", 10*time.Second, hash)
	unstamped.CreationTimestamp = metav1.Time{}
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want time.Duration
	}{
		{name: "inside the grace", pod: crashLoopPod("engine-0-default-0", 10*time.Second, hash), want: 50 * time.Second},
		{name: "at the grace end", pod: crashLoopPod("engine-0-default-0", time.Minute, hash)},
		{name: "past the grace", pod: crashLoopPod("engine-0-default-0", 2*time.Minute, hash)},
		{name: "no creation stamp", pod: unstamped},
		{name: "serving", pod: servingPod("engine-0-default-0")},
		{name: "no pod", pod: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.TerminalWaitingGraceLeft(tc.pod, tNow, time.Minute); got != tc.want {
				t.Fatalf("grace left = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestPodSetServesNothing: the pod set is provably out of service only
// when nothing serves and every live pod is wedged or terminal. A pod
// that is merely not yet Ready keeps the set out of the predicate, and
// so does an empty set.
func TestPodSetServesNothing(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	wedged := crashLoopPod("engine-0-default-0", 10*time.Minute, hash)
	serving := servingPod("engine-0-default-0")
	notReady := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	terminal := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	deleting := crashLoopPod("engine-0-default-0", 10*time.Minute, hash)
	ts := metav1.NewTime(tNow)
	deleting.DeletionTimestamp = &ts
	nodeLost := servingPod("engine-0-default-0")
	for i := range nodeLost.Status.Conditions {
		if nodeLost.Status.Conditions[i].Type == corev1.PodReady {
			nodeLost.Status.Conditions[i].Status = corev1.ConditionFalse
		}
	}

	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{name: "crash-looping pod", pods: []*corev1.Pod{wedged}, want: true},
		{name: "terminal pod", pods: []*corev1.Pod{terminal}, want: true},
		{name: "wedged beside terminal", pods: []*corev1.Pod{wedged, terminal}, want: true},
		{name: "serving pod", pods: []*corev1.Pod{serving}, want: false},
		{name: "wedged beside serving", pods: []*corev1.Pod{wedged, serving}, want: false},
		{name: "running, not yet ready", pods: []*corev1.Pod{notReady}, want: false},
		{name: "node lost under a serving pod", pods: []*corev1.Pod{nodeLost}, want: false},
		{name: "only a deleting pod", pods: []*corev1.Pod{deleting}, want: false},
		{name: "no pods", pods: nil, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.PodSetServesNothing(tc.pods, tNow, time.Minute); got != tc.want {
				t.Fatalf("PodSetServesNothing = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPodSetServesNothing_GangServesThroughItsLeader: a gang's workers are
// never in rotation, so the verdict reads the leader alone. A wedged
// leader puts the gang out of service whatever its worker reports, and a
// serving leader keeps it in service whatever happens to the worker.
func TestPodSetServesNothing_GangServesThroughItsLeader(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	runner := func(pod *corev1.Pod, name string) *corev1.Pod {
		pod.Labels = map[string]string{query.LabelRevisionHash: hash, query.LabelRunner: name}
		return pod
	}
	wedgedLeader := runner(crashLoopPod("engine-0-leader-0", 10*time.Minute, hash), "leader")
	servingLeader := runner(servingPod("engine-0-leader-0"), "leader")
	servingWorker := runner(servingPod("engine-0-worker-0"), "worker")
	wedgedWorker := runner(crashLoopPod("engine-0-worker-0", 10*time.Minute, hash), "worker")
	bootingWorker := runner(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-worker-0"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}, "worker")

	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{name: "wedged leader beside a serving worker", pods: []*corev1.Pod{wedgedLeader, servingWorker}, want: true},
		{name: "wedged leader beside a booting worker", pods: []*corev1.Pod{wedgedLeader, bootingWorker}, want: true},
		{name: "wedged leader beside a wedged worker", pods: []*corev1.Pod{wedgedLeader, wedgedWorker}, want: true},
		{name: "serving leader beside a wedged worker", pods: []*corev1.Pod{servingLeader, wedgedWorker}, want: false},
		{name: "serving leader beside a serving worker", pods: []*corev1.Pod{servingLeader, servingWorker}, want: false},
		{name: "only the worker is left", pods: []*corev1.Pod{servingWorker}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.PodSetServesNothing(tc.pods, tNow, time.Minute); got != tc.want {
				t.Fatalf("PodSetServesNothing = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPodSetUnavailableOnParkedMember: a set is out of the gate's serving
// count on a parked member only when a live pod is parked and fewer than
// desired pods serve, whichever member is parked. A member that is merely
// booting, a set fully in rotation, a parked pod that is deleting or
// terminal, and a parked pod beside enough serving pods all keep the set
// in the count.
func TestPodSetUnavailableOnParkedMember(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	runner := func(pod *corev1.Pod, name string) *corev1.Pod {
		pod.Labels = map[string]string{query.LabelRevisionHash: hash, query.LabelRunner: name}
		return pod
	}
	servingLeader := runner(servingPod("engine-0-leader-0"), "leader")
	wedgedLeader := runner(crashLoopPod("engine-0-leader-0", 10*time.Minute, hash), "leader")
	servingWorker := runner(servingPod("engine-0-worker-0"), "worker")
	wedgedWorker := runner(crashLoopPod("engine-0-worker-0", 10*time.Minute, hash), "worker")
	bootingWorker := runner(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-worker-0"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}, "worker")
	deletingWorker := runner(crashLoopPod("engine-0-worker-0", 10*time.Minute, hash), "worker")
	ts := metav1.NewTime(tNow)
	deletingWorker.DeletionTimestamp = &ts
	terminalWorker := runner(crashLoopPod("engine-0-worker-0", 10*time.Minute, hash), "worker")
	terminalWorker.Status.Phase = corev1.PodFailed

	for _, tc := range []struct {
		name    string
		pods    []*corev1.Pod
		desired int32
		want    bool
	}{
		{name: "worker parked beside a serving leader", pods: []*corev1.Pod{servingLeader, wedgedWorker}, desired: 2, want: true},
		{name: "leader parked beside a serving worker", pods: []*corev1.Pod{wedgedLeader, servingWorker}, desired: 2, want: true},
		{name: "both members parked", pods: []*corev1.Pod{wedgedLeader, wedgedWorker}, desired: 2, want: true},
		{name: "single parked pod", pods: []*corev1.Pod{wedgedLeader}, desired: 1, want: true},
		{name: "worker booting beside a serving leader", pods: []*corev1.Pod{servingLeader, bootingWorker}, desired: 2, want: false},
		{name: "both members serving", pods: []*corev1.Pod{servingLeader, servingWorker}, desired: 2, want: false},
		{name: "parked worker is deleting", pods: []*corev1.Pod{servingLeader, deletingWorker}, desired: 2, want: false},
		{name: "parked worker is terminal", pods: []*corev1.Pod{servingLeader, terminalWorker}, desired: 2, want: false},
		{name: "parked pod beside enough serving pods", pods: []*corev1.Pod{servingLeader, servingWorker, wedgedWorker}, desired: 2, want: false},
		{name: "no pods", pods: nil, desired: 2, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.PodSetUnavailableOnParkedMember(tc.pods, tc.desired, tNow, time.Minute); got != tc.want {
				t.Fatalf("PodSetUnavailableOnParkedMember = %v, want %v", got, tc.want)
			}
		})
	}
}

// A row the roll promoted onto the update revision runs that revision
// ahead of the Component's current one. Read against the revision it
// runs, a wedged pod of that revision is the row's own and a pod of the
// revision the roll replaced, or of no known revision, is a leftover; the
// plain reading stays on the current revision, so the same pushed pod is
// no wedge to it.
func TestCrashLoopWedgedPodOn_ReadsTheRowAgainstTheRevisionItRuns(t *testing.T) {
	const pushed = "engine-def456"
	row := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: pushed}
	for _, tc := range []struct {
		name string
		hash string
		want bool
	}{
		{name: "pod on the revision the row runs", hash: query.RevisionFromName(pushed).Hash(), want: true},
		{name: "pod on the current revision the roll replaced", hash: query.RevisionFromName(clRevision).Hash()},
		{name: "pod on a revision the Component does not know", hash: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := crashLoopPod("engine-0-default-0", 10*time.Minute, tc.hash)
			got, _, _ := evidence.CrashLoopWedgedPodOn(crashLoopInput(time.Minute), row, 1, []*corev1.Pod{pod}, pushed, true)
			if (got != nil) != tc.want {
				t.Fatalf("wedge on the running revision = %v, want %v", got != nil, tc.want)
			}
		})
	}
	pushedPod := crashLoopPod("engine-0-default-0", 10*time.Minute, query.RevisionFromName(pushed).Hash())
	if _, got, _ := evidence.CrashLoopWedge(crashLoopInput(time.Minute), row, 1, []*corev1.Pod{pushedPod}); got {
		t.Fatalf("the plain reading stays on the current revision; a pushed pod's wedge with no counted crash behind it is the escalation's")
	}
}

// A promoted pod that has failed readiness past the grace is out of
// service: a set of such pods serves nothing, and a gang whose routed
// leader is one does too. Inside the grace, or on a pod that never
// served, the set keeps its reading: its wait may still resolve.
func TestPodSetServesNothing_UnreadyPastTheGraceIsDark(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	runner := func(pod *corev1.Pod, name string) *corev1.Pod {
		pod.Labels = map[string]string{query.LabelRevisionHash: hash, query.LabelRunner: name}
		return pod
	}
	past := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	inside := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-30*time.Second))
	never := runningNotReadyPod("engine-0-default-0", tNow.Add(-2*time.Minute))

	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{name: "unready past the grace", pods: []*corev1.Pod{past}, want: true},
		{name: "unready inside the grace", pods: []*corev1.Pod{inside}},
		{name: "never promoted", pods: []*corev1.Pod{never}},
		{name: "unready past the grace beside a serving pod", pods: []*corev1.Pod{past, servingPod("engine-0-default-1")}},
		{name: "leader unready past the grace beside a serving worker", pods: []*corev1.Pod{runner(unreadyAfterServingPod("engine-0-leader-0", tNow.Add(-2*time.Minute)), "leader"), runner(servingPod("engine-0-worker-0"), "worker")}, want: true},
		{name: "worker unready past the grace beside a serving leader", pods: []*corev1.Pod{runner(servingPod("engine-0-leader-0"), "leader"), runner(unreadyAfterServingPod("engine-0-worker-0", tNow.Add(-2*time.Minute)), "worker")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.PodSetServesNothing(tc.pods, tNow, time.Minute); got != tc.want {
				t.Fatalf("PodSetServesNothing = %v, want %v", got, tc.want)
			}
			if got := evidence.PodSetServesNothing(tc.pods, tNow, 0); got {
				t.Fatalf("PodSetServesNothing = true with no grace configured: the window does not exist")
			}
		})
	}
}

// A member unready past the grace takes its set out of the gate's
// serving count exactly as a parked member does; inside the grace it is
// a member still booting.
func TestPodSetUnavailableOnParkedMember_UnreadyMemberPastTheGrace(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	runner := func(pod *corev1.Pod, name string) *corev1.Pod {
		pod.Labels = map[string]string{query.LabelRevisionHash: hash, query.LabelRunner: name}
		return pod
	}
	leader := runner(servingPod("engine-0-leader-0"), "leader")
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{name: "worker unready past the grace", pods: []*corev1.Pod{leader, runner(unreadyAfterServingPod("engine-0-worker-0", tNow.Add(-2*time.Minute)), "worker")}, want: true},
		{name: "worker unready inside the grace", pods: []*corev1.Pod{leader, runner(unreadyAfterServingPod("engine-0-worker-0", tNow.Add(-30*time.Second)), "worker")}},
		{name: "worker never promoted", pods: []*corev1.Pod{leader, runner(runningNotReadyPod("engine-0-worker-0", tNow.Add(-2*time.Minute)), "worker")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.PodSetUnavailableOnParkedMember(tc.pods, 2, tNow, time.Minute); got != tc.want {
				t.Fatalf("PodSetUnavailableOnParkedMember = %v, want %v", got, tc.want)
			}
		})
	}
}

// A promoted pod of the row's revision that has failed readiness past
// the grace is a wedge of the row, named by the kubelet's own reason;
// inside the grace the reading says how long until it is one, and a pod
// that never served is no wedge at any age.
func TestCrashLoopWedge_UnreadyPastTheGraceIsAWedge(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	own := func(pod *corev1.Pod) *corev1.Pod {
		pod.Labels = map[string]string{query.LabelRevisionHash: hash}
		return pod
	}
	row := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	input := crashLoopInput(time.Minute)

	reason, wedged, left := evidence.CrashLoopWedge(input, row, 1, []*corev1.Pod{own(unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute)))})
	if !wedged || left != 0 {
		t.Fatalf("unready past the grace: wedged = %v, grace left = %s, want a wedge", wedged, left)
	}
	if !strings.Contains(reason, "engine-0-default-0") || !strings.Contains(reason, evidence.ReasonContainersNotReady) {
		t.Fatalf("reason %q must name the pod and the kubelet's reason", reason)
	}
	if _, wedged, left := evidence.CrashLoopWedge(input, row, 1, []*corev1.Pod{own(unreadyAfterServingPod("engine-0-default-0", tNow.Add(-10*time.Second)))}); wedged || left != 50*time.Second {
		t.Fatalf("unready inside the grace: wedged = %v, grace left = %s, want no wedge with 50s left", wedged, left)
	}
	if _, wedged, left := evidence.CrashLoopWedge(input, row, 1, []*corev1.Pod{own(runningNotReadyPod("engine-0-default-0", tNow.Add(-2*time.Minute)))}); wedged || left != 0 {
		t.Fatalf("never promoted: wedged = %v, grace left = %s, want neither", wedged, left)
	}
	if _, wedged, _ := evidence.CrashLoopWedge(input, row, 1, []*corev1.Pod{unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))}); !wedged {
		t.Fatalf("a pod with no revision label stays in scope and reads as the row's own wedge; got none")
	}
}

// TestPodSetOutOfRotation: the roll's ordering reading. A non-empty pod
// set with no routed pod in rotation is out of rotation now: a promoted
// pod unready inside its grace counts as one past it does, as do a parked
// pod, a terminal pod and a pod that never served; a serving pod anywhere
// in the set keeps it in rotation, a gang reads its leader alone, and a
// set of nothing but deleting pods, or no pods, is not out of rotation.
// Every set that serves nothing by the graced reading is out of rotation.
func TestPodSetOutOfRotation(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	runner := func(pod *corev1.Pod, name string) *corev1.Pod {
		pod.Labels = map[string]string{query.LabelRevisionHash: hash, query.LabelRunner: name}
		return pod
	}
	inside := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-30*time.Second))
	past := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	never := runningNotReadyPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	wedged := crashLoopPod("engine-0-default-0", 10*time.Minute, hash)
	terminal := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	deleting := unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))
	ts := metav1.NewTime(tNow)
	deleting.DeletionTimestamp = &ts

	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{name: "unready inside the grace", pods: []*corev1.Pod{inside}, want: true},
		{name: "unready past the grace", pods: []*corev1.Pod{past}, want: true},
		{name: "never promoted", pods: []*corev1.Pod{never}, want: true},
		{name: "crash-looping pod", pods: []*corev1.Pod{wedged}, want: true},
		{name: "terminal pod", pods: []*corev1.Pod{terminal}, want: true},
		{name: "serving pod", pods: []*corev1.Pod{servingPod("engine-0-default-0")}},
		{name: "unready beside a serving pod", pods: []*corev1.Pod{inside, servingPod("engine-0-default-1")}},
		{name: "leader unready inside the grace beside a serving worker", pods: []*corev1.Pod{runner(unreadyAfterServingPod("engine-0-leader-0", tNow.Add(-30*time.Second)), "leader"), runner(servingPod("engine-0-worker-0"), "worker")}, want: true},
		{name: "worker unready beside a serving leader", pods: []*corev1.Pod{runner(servingPod("engine-0-leader-0"), "leader"), runner(unreadyAfterServingPod("engine-0-worker-0", tNow.Add(-2*time.Minute)), "worker")}},
		{name: "only a deleting pod", pods: []*corev1.Pod{deleting}},
		{name: "no pods", pods: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := evidence.PodSetOutOfRotation(tc.pods)
			if got != tc.want {
				t.Fatalf("PodSetOutOfRotation = %v, want %v", got, tc.want)
			}
			if evidence.PodSetServesNothing(tc.pods, tNow, time.Minute) && !got {
				t.Fatalf("a set that serves nothing must be out of rotation")
			}
		})
	}
}

// TestPodSetDarkOnReadiness pins the set reading a pass takes over pods it
// may itself have drained: every live routed pod running with its kubelet
// readiness failing for the grace reads dark, gate on or off; a pod in
// rotation, one inside its grace, an empty set and a set of workers alone
// do not.
func TestPodSetDarkOnReadiness(t *testing.T) {
	grace := time.Minute
	worker := func(pod *corev1.Pod) *corev1.Pod {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[query.LabelRunner] = "worker"
		return pod
	}
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want bool
	}{
		{name: "promoted pod dark", pods: []*corev1.Pod{unreadyAfterServingPod("engine-0-default-0", tNow.Add(-2*time.Minute))}, want: true},
		{name: "drained pod dark", pods: []*corev1.Pod{runningNotReadyPod("engine-0-default-0", tNow.Add(-2*time.Minute))}, want: true},
		{name: "gang leader dark beside a serving worker", pods: []*corev1.Pod{
			unreadyAfterServingPod("engine-0-leader-0", tNow.Add(-2*time.Minute)), worker(servingPod("engine-0-worker-0"))}, want: true},
		{name: "inside the grace", pods: []*corev1.Pod{unreadyAfterServingPod("engine-0-default-0", tNow.Add(-10*time.Second))}},
		{name: "serving", pods: []*corev1.Pod{servingPod("engine-0-default-0")}},
		{name: "workers alone", pods: []*corev1.Pod{worker(runningNotReadyPod("engine-0-worker-0", tNow.Add(-2*time.Minute)))}},
		{name: "empty", pods: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidence.PodSetDarkOnReadiness(tc.pods, tNow, grace); got != tc.want {
				t.Fatalf("PodSetDarkOnReadiness = %v, want %v", got, tc.want)
			}
		})
	}
}
