package ops_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// newRolledRunnerCrash is a pushedCrashLoop whose promoted pod's runner
// died twice inside the crash window after the row entered Ready: the
// kubelet's record shows the run that died last had itself begun after
// Ready. The pod was created podAge before the pass. With servesAgain
// the kubelet has started the runner again and the pod serves; without
// it the pod sits in CrashLoopBackOff since the second exit.
func newRolledRunnerCrash(t *testing.T, policy workload.RestartPolicy, podAge time.Duration, servesAgain bool) *pushedCrashLoop {
	t.Helper()
	resetExpectations(t)
	now := time.Now().Truncate(time.Second)
	readySince := metav1.NewTime(now.Add(-time.Minute))
	firstExit := metav1.NewTime(readySince.Add(20 * time.Second))
	crashedAt := metav1.NewTime(readySince.Add(40 * time.Second))
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
	)
	pod := podForInstance(isvc, 0, false /* ready */, false /* serving */)
	pod.Spec.Containers[0].Name = constants.MainContainerName
	pod.CreationTimestamp = metav1.NewTime(now.Add(-podAge))
	pod.Status.Phase = corev1.PodRunning
	runner := corev1.ContainerStatus{
		Name:         constants.MainContainerName,
		RestartCount: 2,
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "Error", ExitCode: 1, StartedAt: firstExit, FinishedAt: crashedAt,
		}},
	}
	condition, since := corev1.ConditionFalse, crashedAt
	if servesAgain {
		since = metav1.NewTime(crashedAt.Add(10 * time.Second))
		runner.Ready = true
		runner.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: since}}
		condition = corev1.ConditionTrue
	} else {
		runner.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off restarting failed container"}}
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{runner}
	setPodConditionAt(pod, corev1.ContainersReady, condition, since)
	setPodConditionAt(pod, corev1.PodReady, condition, since)
	setPodConditionAt(pod, query.ServingConditionType, condition, since)
	row := &ir.Status.InstanceStatuses[0]
	row.ReadySince = &readySince
	row.RunningRevision = crashLoopRevision
	c := newFakeClient(t, isvc, ir, pod)
	h := &pushedCrashLoop{t: t, isvc: isvc, c: c, clk: clocktesting.NewFakeClock(now), pod: pod, readySince: readySince, crashedAt: crashedAt}
	h.plan = buildPlanSinglePodEngineForRestart(c, isvc)
	h.plan.RestartPolicy = policy
	return h
}

// retryRecordOf is the revision's retry record as persisted, nil when
// nothing has been counted on it.
func retryRecordOf(h *pushedCrashLoop) *workload.RetryBlock {
	return workload.FindRetryBlock(h.blocks, crashLoopRevision)
}

// requireRemembered checks that the row is Ready on its promoted set with
// nothing open and remembers the crash of that set, dated at the pass
// that read it.
func requireRemembered(t *testing.T, h *pushedCrashLoop) {
	t.Helper()
	row := h.row()
	if row.Phase != v1beta1.OMENativeInstanceReady || row.Operation != nil || row.Incarnation != 1 {
		t.Fatalf("row = %+v, want Ready on the promoted set with nothing open", row)
	}
	if row.LastFailure == nil || row.LastFailure.PodName != h.pod.Name || !row.LastFailure.Time.Time.Equal(h.clk.Now()) {
		t.Fatalf("the row must remember the crash of its promoted set, dated at this pass (%s); got %+v", h.clk.Now(), row.LastFailure)
	}
}

// The retry record counts a crash of a promoted set only at the pass the
// restart pass would rebuild the set for it: the pod waiting in a
// terminal kubelet reason past the grace a pod gets to come up, or the
// policy's rebuild of a runner that restarted. Under restart policy None
// a runner the kubelet restarts in place, and a pod waiting in
// CrashLoopBackOff that is younger than the grace, are rebuilt by
// nothing: the row remembers the crash and the record stands, so the
// roll's next start waits for no backoff the engine never earned. Once
// such a pod is older than the grace the repair opens, the crash counts
// once, and the record's backoff paces the rebuild as it paces every
// repair the record admits. A crash the engine answers with a rebuild of
// its own, the policy's rebuild under RecreateInstanceOnPodRestart,
// counts at its first sighting as before.
func TestRestart_CrashUnderNoneCountsWhenTheRepairOpens(t *testing.T) {
	t.Run("a runner the kubelet restarted in place twice is remembered and counts nothing", func(t *testing.T) {
		h := newRolledRunnerCrash(t, workload.RestartPolicyNone, time.Hour, true /* serves again */)
		selected, done := h.pass()
		if !selected || done {
			t.Fatalf("the first sighting selects the row to remember the crash; got selected=%v done=%v", selected, done)
		}
		requireRemembered(t, h)
		if b := retryRecordOf(h); b != nil {
			t.Fatalf("a restart the kubelet answered in place spent an attempt of the revision; got %+v", b)
		}
		if selected, _ := h.pass(); selected {
			t.Fatal("a remembered crash of a serving set is not selected again")
		}
		if b := retryRecordOf(h); b != nil {
			t.Fatalf("a later sighting of the same restart counted it; got %+v", b)
		}
	})
	t.Run("a pod waiting in CrashLoopBackOff younger than the grace is remembered and the pass returns at the grace end", func(t *testing.T) {
		h := newRolledRunnerCrash(t, workload.RestartPolicyNone, crashLoopWindow-20*time.Second, false /* waiting in CrashLoopBackOff */)
		selected, done := h.pass()
		if !selected || done {
			t.Fatalf("the first sighting selects the row to remember the crash; got selected=%v done=%v", selected, done)
		}
		requireRemembered(t, h)
		if b := retryRecordOf(h); b != nil {
			t.Fatalf("no repair opens inside the grace, so nothing counts; got %+v", b)
		}
		if nextPass := h.wake.Pending(); nextPass <= 0 || nextPass > 20*time.Second {
			t.Fatalf("the pass must come back when the grace ends, not when a backoff is due; got %v", nextPass)
		}
	})
	t.Run("the same pod past the grace opens the repair, counts once, and the backoff paces the rebuild", func(t *testing.T) {
		h := newRolledRunnerCrash(t, workload.RestartPolicyNone, crashLoopWindow-20*time.Second, false /* waiting in CrashLoopBackOff */)
		if selected, _ := h.pass(); !selected {
			t.Fatal("the first sighting selects the row to remember the crash")
		}
		if b := retryRecordOf(h); b != nil {
			t.Fatalf("nothing counts inside the grace; got %+v", b)
		}

		h.clk.Step(30 * time.Second)
		selected, done := h.pass()
		if !selected || done {
			t.Fatalf("the pod past the grace opens the repair; got selected=%v done=%v", selected, done)
		}
		requireRemembered(t, h)
		if b := retryRecordOf(h); b == nil || b.State != workload.RetryBlockBackoff || b.AttemptsStarted != 1 {
			t.Fatalf("the repair's first sighting of the pod past the grace counts one failed attempt; got %+v", b)
		}
		if nextPass := h.wake.Pending(); nextPass <= 0 || nextPass > 20*time.Second {
			t.Fatalf("the pass must come back when the backoff is due; got %v", nextPass)
		}

		h.clk.Step(20 * time.Second)
		if selected, done := h.pass(); !selected || done {
			t.Fatalf("the backoff is due and the pod still waits in CrashLoopBackOff, so the rebuild opens; got selected=%v done=%v", selected, done)
		}
		if row := h.row(); row.Phase != v1beta1.OMENativeInstanceRestarting || row.Incarnation != 2 || row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationRestart {
			t.Fatalf("row = %+v, want Restarting with the rebuild of the set open", row)
		}
		if b := retryRecordOf(h); b == nil || b.State != workload.RetryBlockRetryInProgress || b.AttemptsStarted != 1 {
			t.Fatalf("the rebuild is started as the record's attempt, with no second count; got %+v", b)
		}
	})
	t.Run("under RecreateInstanceOnPodRestart the policy's rebuild counts at the second death", func(t *testing.T) {
		h := newRolledRunnerCrash(t, workload.RestartPolicyRecreateInstance, time.Hour, true /* serves again */)
		selected, done := h.pass()
		if !selected || done {
			t.Fatalf("the second death selects the row; got selected=%v done=%v", selected, done)
		}
		requireRemembered(t, h)
		if b := retryRecordOf(h); b == nil || b.State != workload.RetryBlockBackoff || b.AttemptsStarted != 1 {
			t.Fatalf("a crash the policy answers with a rebuild counts at its first sighting; got %+v", b)
		}
		if nextPass := h.wake.Pending(); nextPass <= 0 || nextPass > 20*time.Second {
			t.Fatalf("the rebuild waits for the backoff; got %v", nextPass)
		}
	})
}
