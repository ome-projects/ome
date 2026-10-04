package canary

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// A crash loop parks the canary; a single restart does not. The readings
// below are one canary Instance of a decoder, Ready since before the
// canary's anchor, whose pod the kubelet restarts in place or the restart
// policy rebuilds. A death is evidenced by a recorded termination or a
// restart count on the pod itself, never by the Instance's readiness.

// runnerDiedTwice is the runner's status after two deaths: the run the
// first restart began died at second, and the kubelet restarted it again.
func runnerDiedTwice(first, second time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:         constants.MainContainerName,
		RestartCount: 2,
		State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(second.Add(2 * time.Second))}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", StartedAt: metav1.NewTime(first.Add(2 * time.Second)), FinishedAt: metav1.NewTime(second),
		}},
	}
}

// runnerInCrashLoopBackOff is the runner's status while the kubelet backs
// off before its next restart: the run that began at startedAt died at
// finishedAt.
func runnerInCrashLoopBackOff(startedAt, finishedAt time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:         constants.MainContainerName,
		RestartCount: 1,
		State:        corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: constants.StateReasonCrashLoopBackOff}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", StartedAt: metav1.NewTime(startedAt), FinishedAt: metav1.NewTime(finishedAt),
		}},
	}
}

// readyAgain marks the pod Ready: its restarted runner is serving.
func readyAgain(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	for i := range pod.Status.ContainerStatuses {
		pod.Status.ContainerStatuses[i].Ready = true
	}
	return pod
}

// notReady marks the pod as not serving yet.
func notReady(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	return pod
}

// rebuiltRow is the row of an Instance the restart policy rebuilt after
// its pod died at failedAt: the rebuilt set entered Ready at readyAt and
// the death that opened the rebuild is recorded on the row.
func rebuiltRow(index int32, failedAt, readyAt time.Time, incarnation int64) v1beta1.OMENativeInstanceStatus {
	row := instanceRow(index, v1beta1.OMENativeInstanceReady, "svc-decoder-new", &readyAt, incarnation)
	exit := int32(1)
	row.LastFailure = &v1beta1.InstanceTermination{PodName: "svc-decoder-5-default-0", ContainerName: constants.MainContainerName,
		Reason: "Error", ExitCode: &exit, Time: metav1.NewTime(failedAt)}
	return row
}

// escalatedRow is the row of an Instance the workload escalated to Failed
// for its pod's crash loop at failedAt, the pod kept in place: the row still
// carries its entry into Ready and records the escalation as its last
// failure.
func escalatedRow(index int32, readyAt, failedAt time.Time) v1beta1.OMENativeInstanceStatus {
	row := instanceRow(index, v1beta1.OMENativeInstanceFailed, "svc-decoder-new", &readyAt, 1)
	row.LastFailure = &v1beta1.InstanceTermination{PodName: "svc-decoder-5-default-0", ContainerName: constants.MainContainerName,
		Reason: constants.StateReasonCrashLoopBackOff, Time: metav1.NewTime(failedAt)}
	return row
}

func TestCrashedCanaryPod_OneRestartIsNotACrashLoop(t *testing.T) {
	ready := time.Unix(100000, 0)
	since := ready.Add(time.Minute) // the canary's anchor
	death := since.Add(30 * time.Second)
	observed := func(rows ...v1beta1.OMENativeInstanceStatus) observedCanaryRevisions {
		return observedCanaryRevisions{targetHash: "new", rows: rows, fromIR: true, statusFresh: true}
	}
	servingRow := instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-decoder-new", &ready, 1)
	for _, tc := range []struct {
		name     string
		observed observedCanaryRevisions
		pods     []*corev1.Pod
		want     string
	}{
		{"one restart of a canary-revision decoder pod that returns Ready keeps the step",
			observed(servingRow), []*corev1.Pod{readyAgain(crashPod("new", 5, 1, runnerDied(death)))}, ""},
		{"one restart whose pod is still coming back keeps the step",
			observed(servingRow), []*corev1.Pod{notReady(crashPod("new", 5, 1, runnerDied(death)))}, ""},
		{"a second restart after the anchor parks",
			observed(servingRow), []*corev1.Pod{readyAgain(crashPod("new", 5, 1, runnerDiedTwice(death, death.Add(40*time.Second))))}, "container ome-container Error (exit 1)"},
		{"CrashLoopBackOff parks",
			observed(servingRow), []*corev1.Pod{notReady(crashPod("new", 5, 1, runnerInCrashLoopBackOff(death.Add(2*time.Second), death.Add(10*time.Second))))}, "container ome-container CrashLoopBackOff (exit 1)"},
		{"a back-off whose earlier crash came before the Instance served is one death",
			observed(instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-decoder-new", &since, 1)), []*corev1.Pod{notReady(crashPod("new", 5, 1, runnerInCrashLoopBackOff(since, since.Add(31*time.Second))))}, ""},
		{"a back-off whose earlier crash came before the anchor is one death",
			observed(servingRow), []*corev1.Pod{notReady(crashPod("new", 5, 1, runnerInCrashLoopBackOff(since.Add(-time.Second), death)))}, ""},
		{"a second death whose first was before the anchor is a single death",
			observed(servingRow), []*corev1.Pod{readyAgain(crashPod("new", 5, 1, runnerDiedTwice(since.Add(-time.Minute), death)))}, ""},
		{"a rebuilt Instance whose rebuilt pod dies after serving parks",
			observed(rebuiltRow(5, death, death.Add(20*time.Second), 2)), []*corev1.Pod{crashPod("new", 5, 2, runnerDied(death.Add(50*time.Second)))}, "container ome-container Error (exit 1)"},
		{"a rebuilt Instance whose rebuild was a deletion, not a death, is a dip",
			observed(instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-decoder-new", func() *time.Time { t := death.Add(20 * time.Second); return &t }(), 2)),
			[]*corev1.Pod{crashPod("new", 5, 2, runnerDied(death.Add(50*time.Second)))}, ""},
		{"a rebuild for a death before the anchor is not counted",
			observed(rebuiltRow(5, since.Add(-time.Hour), since.Add(-30*time.Minute), 2)), []*corev1.Pod{crashPod("new", 5, 2, runnerDied(death))}, ""},
		{"a row the workload escalated to Failed for the loop still parks on the pod's second death",
			observed(escalatedRow(5, ready, death.Add(45*time.Second))), []*corev1.Pod{readyAgain(crashPod("new", 5, 1, runnerDiedTwice(death, death.Add(40*time.Second))))}, "container ome-container Error (exit 1)"},
		{"a row the workload escalated to Failed while the kubelet backs off parks",
			observed(escalatedRow(5, ready, death.Add(12*time.Second))), []*corev1.Pod{notReady(crashPod("new", 5, 1, runnerInCrashLoopBackOff(death.Add(2*time.Second), death.Add(10*time.Second))))}, "container ome-container CrashLoopBackOff (exit 1)"},
		{"a row the workload escalated to Failed does not make one restart a loop",
			observed(escalatedRow(5, ready, death.Add(5*time.Second))), []*corev1.Pod{readyAgain(crashPod("new", 5, 1, runnerDied(death)))}, ""},
		{"the death that opened a rebuild is not counted twice on the pod being drained",
			observed(func() v1beta1.OMENativeInstanceStatus {
				r := rebuiltRow(5, death, ready, 2)
				r.Phase = v1beta1.OMENativeInstanceRestarting
				return r
			}()),
			[]*corev1.Pod{crashPod("new", 5, 1, runnerDied(death))}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := crashedCanaryPod(v1beta1.DecoderComponent, tc.observed, tc.pods, since)
			if tc.want == "" {
				if got != nil {
					t.Fatalf("no crash expected, got %+v", got)
				}
				return
			}
			if got == nil || got.Detail != tc.want || got.Component != v1beta1.DecoderComponent {
				t.Fatalf("crash %+v, want detail %q", got, tc.want)
			}
		})
	}
}

// TestCrashedCanaryPod_CrashAtEveryStartParks walks the readings of a
// canary pod that served and then crashes at every start: the first death
// is a single restart, and the loop parks as soon as the pod shows a second
// death, in whichever face the pass catches it.
func TestCrashedCanaryPod_CrashAtEveryStartParks(t *testing.T) {
	ready := time.Unix(100000, 0)
	since := ready.Add(time.Minute)
	first := since.Add(30 * time.Second)
	observed := observedCanaryRevisions{targetHash: "new", fromIR: true, statusFresh: true,
		rows: []v1beta1.OMENativeInstanceStatus{instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-decoder-new", &ready, 1)}}
	readings := []struct {
		name   string
		status corev1.ContainerStatus
		parks  bool
	}{
		{"the first death, restarted in place", runnerDied(first), false},
		{"the restarted run died at start and the kubelet backs off", runnerInCrashLoopBackOff(first.Add(2*time.Second), first.Add(5*time.Second)), true},
		{"the kubelet tried again and the run died again", runnerDiedTwice(first, first.Add(20*time.Second)), true},
	}
	for _, r := range readings {
		t.Run(r.name, func(t *testing.T) {
			pod := notReady(crashPod("new", 5, 1, r.status))
			got := crashedCanaryPod(v1beta1.DecoderComponent, observed, []*corev1.Pod{pod}, since)
			if (got != nil) != r.parks {
				t.Fatalf("parks=%v, got %+v", r.parks, got)
			}
		})
	}
}

// TestCrashedCanaryPod_DeletedPodWithPendingReplacementKeepsTheStep: the
// serving canary pod is deleted and its replacement is Pending: the step
// keeps its state, nothing parks. The replacement is a fresh pod with a
// new UID and no restart; the Instance row still carries the deleted
// pod's entry into Ready, whether or not the deleted pod was ever seen
// with its deletion stamp.
func TestCrashedCanaryPod_DeletedPodWithPendingReplacementKeepsTheStep(t *testing.T) {
	ready := time.Unix(100000, 0)
	since := ready.Add(time.Minute)
	deletedAt := since.Add(30 * time.Second)
	observed := observedCanaryRevisions{targetHash: "new", fromIR: true, statusFresh: true,
		rows: []v1beta1.OMENativeInstanceStatus{instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-decoder-new", &ready, 1)}}
	replacement := func(status *corev1.ContainerStatus) *corev1.Pod {
		pod := crashPod("new", 5, 1, corev1.ContainerStatus{Name: constants.MainContainerName})
		pod.UID = types.UID("replacement")
		pod.CreationTimestamp = metav1.NewTime(deletedAt.Add(time.Second))
		pod.Status.Phase = corev1.PodPending
		pod.Status.ContainerStatuses = nil
		if status != nil {
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{*status}
		}
		return notReady(pod)
	}
	waiting := corev1.ContainerStatus{Name: constants.MainContainerName, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}
	running := runnerRunningSince(deletedAt.Add(5 * time.Second))
	old := deleting(stoppedPod("new", 5, 1, 143, "Error", deletedAt, corev1.PodRunning))
	old.UID = types.UID("deleted")
	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
	}{
		{"the deleted pod is still on its way out beside the Pending replacement", []*corev1.Pod{old, replacement(nil)}},
		{"the deleted pod vanished before its deletion stamp was observed; the replacement is Pending", []*corev1.Pod{replacement(nil)}},
		{"the replacement's container is being created", []*corev1.Pod{replacement(&waiting)}},
		{"the replacement's container runs but is not Ready yet", []*corev1.Pod{replacement(&running)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := crashedCanaryPod(v1beta1.EngineComponent, observed, tc.pods, since); got != nil {
				t.Fatalf("a deletion with a fresh replacement is a dip, got %+v", got)
			}
		})
	}
	// The pod identity is what the reading keys on: the same reading with
	// the replacement carrying a recorded death after it served parks.
	row := instanceRow(5, v1beta1.OMENativeInstanceReady, "svc-decoder-new", &ready, 1)
	later := deletedAt.Add(2 * time.Minute)
	servedRow := row
	servedRow.ReadySince = &metav1.Time{Time: deletedAt.Add(20 * time.Second)}
	pod := readyAgain(replacement(func() *corev1.ContainerStatus { s := runnerDiedTwice(later, later.Add(30*time.Second)); return &s }()))
	if got := crashedCanaryPod(v1beta1.EngineComponent, observedCanaryRevisions{targetHash: "new", fromIR: true, statusFresh: true,
		rows: []v1beta1.OMENativeInstanceStatus{servedRow}}, []*corev1.Pod{pod}, since); got == nil {
		t.Fatalf("a replacement that served and then died twice is a crash loop")
	}
}
