package workload_test

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Fixtures shared with the escalation package's tests.

func intOrStringInt(v int) *intstr.IntOrString {
	x := intstr.FromInt(v)
	return &x
}

// singleInstancePlan returns a ComponentPlan whose desired pod count for
// instance idx is pods — the DesiredPodCountByInstance input the pass
// derives disposable-vs-gang routing from.
func singleInstancePlan(idx, pods int32) workloadtypes.ComponentPlan {
	return workloadtypes.ComponentPlan{
		Instances: []workloadtypes.InstancePlan{{Index: idx, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: pods}}}},
	}
}

func escalationFixture(insts []workloadtypes.InstanceStatus) (workloadtypes.ReconcileInput, *escalationRecorder) {
	rec := &escalationRecorder{store: append([]workloadtypes.InstanceStatus(nil), insts...)}
	input := workloadtypes.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range rec.store {
				if rec.store[i].Index == idx {
					mutate(&rec.store[i])
					return nil
				}
			}
			return nil
		},
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			b := workloadtypes.RetryBlock{TargetRevision: rev}
			mutate(&b)
			rec.blocks = append(rec.blocks, b)
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, reason string) {
			rec.warns = append(rec.warns, reason)
		},
	}
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
	return input, rec
}

// escalationFixture wires a ReconcileInput whose MutateInstance applies
// the callback to the local store and whose WarnInstanceFailed /
// MutateRetryBlock record their calls.
type escalationRecorder struct {
	store  []workloadtypes.InstanceStatus
	warns  []string
	blocks []workloadtypes.RetryBlock
}

const probeMessage = "containers with unready status: [main]"

// runningNotReadyPod builds the limbo shape: phase Running, every
// container started, ContainersReady=False with the kubelet's own reason
// and message.
func runningNotReadyPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.ContainersReady,
				Status:             corev1.ConditionFalse,
				Reason:             evidence.ReasonContainersNotReady,
				Message:            probeMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// servingPod builds a pod that is ContainersReady AND carries the
// ome.io/serving readiness gate — a pod in the load-balancer rotation.
func servingPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: "ome.io/serving", Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

const (
	gangFailedMessage      = "PodGroup svc-engine-0 reported phase Failed: 2 of 2 members failed"
	gangConflictMessage    = "PodGroup svc-engine-0 is controlled by StatefulSet/legacy, not by this owner"
	gangTerminatingMessage = "PodGroup svc-engine-0 is terminating; the gang is announced again once it is collected"
)

// gangObservations wires one Instance's PodGroup classification into the
// reconcile input, the way the PodGroup pass does before the dispatcher.
func gangObservations(idx int32, state workloadtypes.GangState, message string) *workloadtypes.GangObservations {
	g := workloadtypes.NewGangObservations()
	g.Record(idx, workloadtypes.GangObservation{Name: "svc-engine-0", State: state, Message: message})
	return g
}

const (
	gangPairSource = "llama-70b-engine-priorrev"
	gangPairTarget = "llama-70b-engine-targetrv"
)

// gangSurgePair is the source (index 0, Step=Surge, pinned to the marker)
// and the replacement gang's marker at markerIndex running markerStep.
func gangSurgePair(now time.Time, markerIndex int32, markerStep string) []workloadtypes.InstanceStatus {
	return []workloadtypes.InstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           workloadtypes.InstancePhaseUpdating,
			PodCount:        2,
			RunningRevision: gangPairSource,
			TargetRevision:  gangPairTarget,
			Operation: &workloadtypes.InstanceOperation{
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurge,
				SurgeIndex:     &markerIndex,
				TargetRevision: gangPairTarget,
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
		{
			Index:          markerIndex,
			Incarnation:    1,
			Phase:          workloadtypes.InstancePhaseCreating,
			PodCount:       2,
			TargetRevision: gangPairTarget,
			Operation: &workloadtypes.InstanceOperation{
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           markerStep,
				TargetRevision: gangPairTarget,
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
	}
}

// rollInFlight is one Instance with an update already stamped on it, at
// the step the caller names.
func rollInFlight(idx int32, step string, incarnation int64, deadline time.Time) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{
		Index:           idx,
		Incarnation:     incarnation,
		Phase:           workloadtypes.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "prior-rev",
		Operation: &workloadtypes.InstanceOperation{
			ID:             "update-" + step,
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: updateTarget().Name,
			Deadline:       metav1.NewTime(deadline),
		},
	}
}

// surgeSourceRow is the source half of a gang surge at step: Updating,
// pinned to the replacement gang under SurgeIndex, with the target
// revision the attempt is committed to.
func surgeSourceRow(now time.Time, step string, markerIndex int32) []workloadtypes.InstanceStatus {
	rows := gangSurgePair(now, markerIndex, workloadtypes.UpdateStepGangSurgeTarget)
	rows[0].Operation.Step = step
	return rows
}

// recordRetryBlockRemovals wires a recording MutateRetryBlock that
// captures the revision of every Remove disposition.
func recordRetryBlockRemovals(input *workloadtypes.ReconcileInput) *[]string {
	removed := &[]string{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		b := workloadtypes.RetryBlock{TargetRevision: rev}
		if mutate(&b) == workloadtypes.RetryBlockRemove {
			*removed = append(*removed, rev)
		}
		return nil
	}
	return removed
}

const schedulerMessage = "0/3 nodes are available: 3 Insufficient nvidia.com/gpu"

// unschedulablePod builds a Pending pod the scheduler has ruled out,
// carrying the scheduler's own message and the moment it did so.
func unschedulablePod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionFalse,
				Reason:             workloadtypes.WaitingReasonUnschedulable,
				Message:            schedulerMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
		},
	}
}
