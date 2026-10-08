package escalation_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	parkedRunning = "own-engine-oldhash"
	parkedTarget  = "own-engine-newhash"
	// parkedGrace is the stuck-pod grace the parked reading measures a
	// restarting runner against.
	parkedGrace = 90 * time.Second
)

// parkedRow is a recreate attempt at parkedTarget parked after its
// disposition, in the phase its pods last gave it.
func parkedRow(phase workloadtypes.InstancePhase, waiting string) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: phase, PodCount: 1,
		RunningRevision: parkedRunning, TargetRevision: parkedTarget,
		Operation: &workloadtypes.InstanceOperation{
			ID: "update-0-1", Type: workloadtypes.InstanceOperationUpdate, Step: workloadtypes.UpdateStepParked,
			TargetRevision: parkedTarget, Waiting: waiting,
		},
		LastFailure: &workloadtypes.InstanceTermination{PodName: "engine-0-default-0", Reason: "CrashLoopBackOff"},
	}
}

// crashedPod is the parked set's pod as the kubelet reports it: serving
// between crashes, or down in the back-off between two runs.
func crashedPod(serving bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour))},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue}},
		},
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions,
			corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Ready: true, RestartCount: 2,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		return pod
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionFalse})
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", RestartCount: 2,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
	return pod
}

// restartingPod is the parked set's pod while the kubelet restarts its
// runner in place: Running, out of rotation, unready for unreadyFor.
func restartingPod(now time.Time, unreadyFor time.Duration) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-unreadyFor))},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-unreadyFor))},
			},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main", RestartCount: 3,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-unreadyFor))}}}},
		},
	}
	return pod
}

func followParked(t *testing.T, input workloadtypes.ReconcileInput, target *appsv1.ControllerRevision, pods []*corev1.Pod, excluded map[int32]struct{}) {
	t.Helper()
	err := escalation.FollowParkedAttempts(context.Background(), escalation.PassInput{
		Input:    input,
		Plan:     singleInstancePlan(0, 1),
		Target:   target,
		Pods:     func(context.Context) (map[int32][]*corev1.Pod, error) { return map[int32][]*corev1.Pod{0: pods}, nil },
		Excluded: excluded,
	})
	if err != nil {
		t.Fatalf("FollowParkedAttempts: %v", err)
	}
}

// The phase of a parked attempt is its pod set's: Updating once the full
// set serves; Failed once the set is gone or serves nothing - parked in a
// back-off, or unready past the stuck-pod grace; and otherwise the phase
// the row read before, so a runner restarting in place inside the grace
// moves nothing. A row whose attempt is not parked is left alone, as is a
// row the scale-down wave holds back.
func TestFollowParkedAttempts_PhaseFollowsThePodSet(t *testing.T) {
	now := time.Now()
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: parkedTarget}}
	for _, tc := range []struct {
		name  string
		row   workloadtypes.InstanceStatus
		pods  []*corev1.Pod
		want  workloadtypes.InstancePhase
		wrote bool
	}{
		{name: "down set serves again", row: parkedRow(workloadtypes.InstancePhaseFailed, "RetryBlock"), pods: []*corev1.Pod{crashedPod(true)}, want: workloadtypes.InstancePhaseUpdating, wrote: true},
		{name: "serving set parks in a back-off", row: parkedRow(workloadtypes.InstancePhaseUpdating, "RetryBlock"), pods: []*corev1.Pod{crashedPod(false)}, want: workloadtypes.InstancePhaseFailed, wrote: true},
		{name: "serving set restarting inside the grace keeps its reading", row: parkedRow(workloadtypes.InstancePhaseUpdating, "RetryBlock"), pods: []*corev1.Pod{restartingPod(now, parkedGrace/2)}, want: workloadtypes.InstancePhaseUpdating},
		{name: "serving set unready past the grace", row: parkedRow(workloadtypes.InstancePhaseUpdating, "RetryBlock"), pods: []*corev1.Pod{restartingPod(now, parkedGrace+time.Second)}, want: workloadtypes.InstancePhaseFailed, wrote: true},
		{name: "serving set is gone", row: parkedRow(workloadtypes.InstancePhaseUpdating, "RetryBlock"), pods: nil, want: workloadtypes.InstancePhaseFailed, wrote: true},
		{name: "serving set keeps serving", row: parkedRow(workloadtypes.InstancePhaseUpdating, "RetryBlock"), pods: []*corev1.Pod{crashedPod(true)}, want: workloadtypes.InstancePhaseUpdating},
		{name: "down set stays down", row: parkedRow(workloadtypes.InstancePhaseFailed, "RetryBlock"), pods: []*corev1.Pod{crashedPod(false)}, want: workloadtypes.InstancePhaseFailed},
		{name: "down set restarting inside the grace stays down", row: parkedRow(workloadtypes.InstancePhaseFailed, "RetryBlock"), pods: []*corev1.Pod{restartingPod(now, parkedGrace/2)}, want: workloadtypes.InstancePhaseFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture([]workloadtypes.InstanceStatus{tc.row})
			input.Clock = clocktesting.NewFakeClock(now)
			input.StuckPodGrace = parkedGrace
			writes := 0
			seam := input.MutateInstance
			input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
				writes++
				return seam(ctx, idx, mutate)
			}
			followParked(t, input, target, tc.pods, nil)
			row := rec.store[0]
			if row.Phase != tc.want {
				t.Fatalf("Phase = %s, want %s", row.Phase, tc.want)
			}
			if row.Operation == nil || row.Operation.ID != "update-0-1" || row.Operation.Waiting != "RetryBlock" {
				t.Fatalf("the parked attempt must stay as it was, got %+v", row.Operation)
			}
			if (writes > 0) != tc.wrote {
				t.Fatalf("writes = %d, want a write only when the phase moves (%v)", writes, tc.wrote)
			}
		})
	}
	t.Run("an attempt in flight is not followed", func(t *testing.T) {
		row := parkedRow(workloadtypes.InstancePhaseUpdating, "")
		row.Operation.Step = workloadtypes.UpdateStepDrain
		input, rec := escalationFixture([]workloadtypes.InstanceStatus{row})
		followParked(t, input, target, []*corev1.Pod{crashedPod(false)}, nil)
		if rec.store[0].Phase != workloadtypes.InstancePhaseUpdating {
			t.Fatalf("an attempt in flight keeps its phase, got %s", rec.store[0].Phase)
		}
	})
	t.Run("a scale-down victim is the wave's", func(t *testing.T) {
		input, rec := escalationFixture([]workloadtypes.InstanceStatus{parkedRow(workloadtypes.InstancePhaseFailed, "RetryBlock")})
		followParked(t, input, target, []*corev1.Pod{crashedPod(true)}, map[int32]struct{}{0: {}})
		if rec.store[0].Phase != workloadtypes.InstancePhaseFailed {
			t.Fatalf("an excluded row keeps its phase, got %s", rec.store[0].Phase)
		}
	})
}

// A parked set whose runner the kubelet restarts in place, and that is back
// in rotation inside the stuck-pod grace, is one reading across the whole
// cycle: no status write, however many times it restarts. The set unready
// past the grace is the one move to Failed, and serving again the one back.
func TestFollowParkedAttempts_RestartInsideTheGraceMovesNothing(t *testing.T) {
	now := time.Now()
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: parkedTarget}}
	input, rec := escalationFixture([]workloadtypes.InstanceStatus{parkedRow(workloadtypes.InstancePhaseUpdating, "RetryBlock")})
	input.Clock = clocktesting.NewFakeClock(now)
	input.StuckPodGrace = parkedGrace
	writes := 0
	seam := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		writes++
		return seam(ctx, idx, mutate)
	}
	pass := func(pods []*corev1.Pod) workloadtypes.InstancePhase {
		input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), rec.store...)
		followParked(t, input, target, pods, nil)
		return rec.store[0].Phase
	}
	for i, pods := range [][]*corev1.Pod{
		{restartingPod(now, 5*time.Second)},
		{crashedPod(true)},
		{restartingPod(now, parkedGrace/3)},
		{crashedPod(true)},
		{restartingPod(now, parkedGrace-time.Second)},
	} {
		if phase := pass(pods); phase != workloadtypes.InstancePhaseUpdating || writes != 0 {
			t.Fatalf("pass %d: Phase = %s with %d write(s), want Updating kept with none", i, phase, writes)
		}
	}
	if phase := pass([]*corev1.Pod{restartingPod(now, parkedGrace)}); phase != workloadtypes.InstancePhaseFailed || writes != 1 {
		t.Fatalf("unready past the grace: Phase = %s with %d write(s), want Failed in one write", phase, writes)
	}
	if phase := pass([]*corev1.Pod{restartingPod(now, parkedGrace+time.Minute)}); phase != workloadtypes.InstancePhaseFailed || writes != 1 {
		t.Fatalf("still unready: Phase = %s with %d write(s), want Failed kept with none", phase, writes)
	}
	if phase := pass([]*corev1.Pod{crashedPod(true)}); phase != workloadtypes.InstancePhaseUpdating || writes != 2 {
		t.Fatalf("serving again: Phase = %s with %d write(s), want Updating in one write", phase, writes)
	}
}

// The wait a parked attempt names follows the ladder of the revision its
// next attempt is at: the hold while that ladder holds, the ladder while
// it denies a start, and otherwise the wait the update pass last named.
func TestFollowParkedAttempts_WaitFollowsTheLadder(t *testing.T) {
	now := time.Now()
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: parkedTarget}}
	corrected := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "own-engine-fixedhash"}}
	due := metav1.NewTime(now.Add(time.Minute))
	for _, tc := range []struct {
		name   string
		was    string
		blocks []workloadtypes.RetryBlock
		target *appsv1.ControllerRevision
		want   string
	}{
		{name: "the ladder holds", was: "RetryBlock", target: target, want: "Held",
			blocks: []workloadtypes.RetryBlock{{TargetRevision: parkedTarget, State: workloadtypes.RetryBlockHeld}}},
		{name: "the backoff is not due", was: "Budget", target: target, want: "RetryBlock",
			blocks: []workloadtypes.RetryBlock{{TargetRevision: parkedTarget, State: workloadtypes.RetryBlockBackoff, NextRetryAt: &due}}},
		{name: "the ladder admits and the budget was named", was: "Budget", target: target, want: "Budget",
			blocks: []workloadtypes.RetryBlock{{TargetRevision: parkedTarget, State: workloadtypes.RetryBlockBackoff, NextRetryAt: &metav1.Time{Time: now.Add(-time.Second)}}}},
		{name: "a corrected revision has no ladder of its own", was: "Held", target: corrected, want: "Held",
			blocks: []workloadtypes.RetryBlock{{TargetRevision: parkedTarget, State: workloadtypes.RetryBlockHeld}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture([]workloadtypes.InstanceStatus{parkedRow(workloadtypes.InstancePhaseUpdating, tc.was)})
			input.Clock = clocktesting.NewFakeClock(now)
			input.ObservedState.RetryBlocks = tc.blocks
			followParked(t, input, tc.target, []*corev1.Pod{crashedPod(true)}, nil)
			if got := rec.store[0].Operation.Waiting; got != tc.want {
				t.Fatalf("Waiting = %q, want %q", got, tc.want)
			}
		})
	}
}
