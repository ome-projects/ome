package escalation_test

// Evidence refresh on an already-Failed row: the escalation pass takes no
// new edge there, but a row failed on the generic deadline must not keep
// reporting that when the kubelet has since named the real cause.

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// wedgedPod builds a pod parked in a terminal kubelet waiting reason,
// with the kubelet's own explanation attached.
func wedgedPod(name, reason string, created time.Time) *corev1.Pod {
	pod := waitingPod(name, reason, "node-a", created)
	pod.Status.ContainerStatuses[0].State.Waiting.Message = "back-off restarting failed container"
	return pod
}

// failedOnDeadline returns a Failed row whose LastFailure is the generic
// elapsed-deadline record, with the spent attempt preserved.
func failedOnDeadline(now time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseFailed,
		PodCount: 1,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationRestart,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
		LastFailure: &types.InstanceTermination{
			Reason:  escalation.DeadlineExceededReason,
			Message: "DeadlineExceeded: Restart/Drain exceeded InstanceReadyTimeout",
			Time:    metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
}

// A row failed on the generic deadline that later wedges on a terminal
// kubelet reason has its LastFailure overwritten once with the concrete
// cause. Nothing else moves: no second InstanceFailed event, no
// RetryBlock, the phase and the spent attempt untouched.
func TestEscalation_FailedRowRefreshesGenericDeadlineEvidence(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(failedOnDeadline(now))
	input.StuckPodGrace = time.Second

	stuck := wedgedPod("engine-0-default-0", "CrashLoopBackOff", now.Add(-time.Minute))
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (the refresh takes no edge)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Type != types.InstanceOperationRestart {
		t.Errorf("Operation: got %+v want the spent Restart attempt preserved", got.Operation)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != "CrashLoopBackOff" {
		t.Fatalf("LastFailure.Reason: got %+v want CrashLoopBackOff", got.LastFailure)
	}
	if got.LastFailure.PodName != "engine-0-default-0" {
		t.Errorf("LastFailure.PodName: got %q want engine-0-default-0", got.LastFailure.PodName)
	}
	if got.LastFailure.ContainerName != "main" {
		t.Errorf("LastFailure.ContainerName: got %q want main", got.LastFailure.ContainerName)
	}
	if got.LastFailure.Message == "" {
		t.Errorf("LastFailure.Message: got empty want the wedge detail")
	}
	// The refresh names the cause; it does not redate the failure.
	if want := metav1.NewTime(now.Add(-time.Hour)); !got.LastFailure.Time.Equal(&want) {
		t.Errorf("LastFailure.Time: got %v want the original failure time %v", got.LastFailure.Time, want)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none (the row already fired its event)", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
}

// The refresh is edge-triggered on the recorded reason: once the concrete
// cause is on LastFailure, a second pass over the same evidence writes
// nothing, so a permanently wedged row does not rewrite its status on
// every reconcile.
func TestEscalation_FailedRowEvidenceRefreshIsEdgeTriggered(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(failedOnDeadline(now))
	input.StuckPodGrace = time.Second

	stuck := wedgedPod("engine-0-default-0", "CrashLoopBackOff", now.Add(-time.Minute))
	byIdx := map[int32][]*corev1.Pod{0: {stuck}}
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1), byIdx); err != nil {
		t.Fatalf("first escalation pass: %v", err)
	}
	first := *rec.store[0].LastFailure

	// The second pass observes the refreshed row.
	input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1), byIdx); err != nil {
		t.Fatalf("second escalation pass: %v", err)
	}

	if second := *rec.store[0].LastFailure; second != first {
		t.Errorf("LastFailure rewritten on a repeat observation: got %+v want %+v", second, first)
	}
}

// Concrete evidence already on the record is never replaced: only the
// generic deadline reason is refreshable, so the FIRST named cause stays
// the one an operator reads.
func TestEscalation_FailedRowKeepsConcreteEvidence(t *testing.T) {
	now := time.Now()
	insts := failedOnDeadline(now)
	insts[0].LastFailure = &types.InstanceTermination{
		Reason:  "ImagePullBackOff",
		PodName: "engine-0-default-0",
		Message: "pod engine-0-default-0 container main stuck (ImagePullBackOff)",
		Time:    metav1.NewTime(now.Add(-time.Hour)),
	}
	input, rec := escalationFixture(insts)
	input.StuckPodGrace = time.Second

	stuck := wedgedPod("engine-0-default-0", "CrashLoopBackOff", now.Add(-time.Minute))
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].LastFailure; got == nil || got.Reason != "ImagePullBackOff" {
		t.Errorf("LastFailure: got %+v want the recorded ImagePullBackOff kept", got)
	}
}
