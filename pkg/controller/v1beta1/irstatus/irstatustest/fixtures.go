// Package irstatustest holds the deterministic per-Instance row fixtures the
// irstatus codec tests and the real-API-server qualification suites share.
// It is test support only: no production package imports it.
package irstatustest

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	// Revision and OtherRevision are the two full revision names the ladder
	// fixtures use.
	Revision      = "example-engine-2f32f6fe"
	OtherRevision = "example-engine-9b1c0d2e"
	// Container is the serving container name on failure records.
	Container = "ome-container"
)

// Time is the fixed instant every fixture timestamp derives from.
var Time = metav1.NewTime(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))

// TimePlus returns Time shifted by d.
func TimePlus(d time.Duration) metav1.Time {
	return metav1.NewTime(Time.Add(d))
}

// UniformRows is the steady-state fixture: every row Ready on one revision at
// incarnation 1 with single-pod counts and admission.
func UniformRows(n int) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, n)
	for i := range rows {
		rows[i] = v1beta1.OMENativeInstanceStatus{
			Index:             int32(i),
			Incarnation:       1,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   Revision,
			PodCount:          1,
			ServingPodCount:   1,
			AvailablePodCount: 1,
			Admitted:          true,
		}
	}
	return rows
}

// The mass-failure fixture is a synthetic worst case: 4,000 single-pod rows
// split into 2,812 Ready, 814 Restarting with an in-flight Restart and 374
// Failed with a preserved Restart, a lastFailure on every row, and one row on
// a second revision. Phases are spread over the index space by a fixed
// permutation so the index sets fragment rather than form one range.
//
// The records are the controller's own. A recovered row keeps the Pod-level
// record its Pod left when it died with its node (name, PodFailed, time; no
// container detail survived); a parked row carries the deadline record stamped
// when its Restart's Drain step exceeded InstanceReadyTimeout. The service
// name is long enough that revisions and Pod names carry realistic weight;
// the resulting dense status exceeds etcd's default request limit.
const (
	MassFailureRowCount        = 4000
	MassFailureReadyCount      = 2812
	MassFailureRestartingCount = 814
	MassFailureFailedCount     = 374
	MassFailureOperationCount  = MassFailureRestartingCount + MassFailureFailedCount
	massFailureStride          = 1103

	MassFailureServiceName   = "example-chat-completions-70b-instruct"
	MassFailureRevision      = MassFailureServiceName + "-engine-2f32f6fe"
	MassFailureOtherRevision = MassFailureServiceName + "-engine-9b1c0d2e"

	// MassFailurePodReason is the Pod-level fallback reason on a recovered row.
	MassFailurePodReason = "PodFailed"
	// MassFailureDeadlineReason and MassFailureDeadlineMessage are the deadline
	// termination on a parked row.
	MassFailureDeadlineReason  = "DeadlineExceeded"
	MassFailureDeadlineMessage = "DeadlineExceeded: Restart/Drain exceeded InstanceReadyTimeout"
)

// MassFailurePodName is the Pod name a recovered mass-failure row records.
func MassFailurePodName(index int) string {
	return fmt.Sprintf("%s-engine-%d-0", MassFailureServiceName, index)
}

// MassFailureRows builds the mass-failure fixture.
func MassFailureRows() []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, MassFailureRowCount)
	for i := range rows {
		row := v1beta1.OMENativeInstanceStatus{
			Index:           int32(i),
			Incarnation:     1,
			RunningRevision: MassFailureRevision,
			LastFailure: &v1beta1.InstanceTermination{
				Reason:  MassFailureDeadlineReason,
				Message: MassFailureDeadlineMessage,
				Time:    Time,
			},
		}
		operation := &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("2f6b6c8e-0000-4000-8000-%012d", i),
			Type:           v1beta1.InstanceOperationRestart,
			StartedAt:      TimePlus(time.Minute),
			LastProgressAt: TimePlus(2 * time.Minute),
			Deadline:       TimePlus(30 * time.Minute),
			TargetRevision: MassFailureRevision,
			Reason:         "PodFailed",
		}
		switch rank := (i * massFailureStride) % MassFailureRowCount; {
		case rank < MassFailureReadyCount:
			row.Phase = v1beta1.OMENativeInstanceReady
			row.Incarnation = 2
			row.PodCount, row.ServingPodCount, row.AvailablePodCount = 1, 1, 1
			row.Admitted = true
			row.LastFailure = &v1beta1.InstanceTermination{PodName: MassFailurePodName(i), Reason: MassFailurePodReason, Time: Time}
		case rank < MassFailureReadyCount+MassFailureRestartingCount:
			row.Phase = v1beta1.OMENativeInstanceRestarting
			operation.Step = "WaitReady"
			row.Operation = operation
		default:
			row.Phase = v1beta1.OMENativeInstanceFailed
			operation.Step = "Recreate"
			operation.RetryCount = 3
			operation.Reason = "RestartBudgetExhausted"
			row.Operation = operation
		}
		rows[i] = row
	}
	rows[MassFailureRowCount-1].RunningRevision = MassFailureOtherRevision
	return rows
}

// RepresentativeRows mixes two revisions, three incarnations, a rollout in
// flight, sparse indices, partial admission, alternating ordinals, and
// thirteen sparse failures and operations.
func RepresentativeRows(n int) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, n)
	exitCode := int32(1)
	index := int32(0)
	for len(rows) < n {
		i := len(rows)
		if i%97 == 96 {
			index++ // leave a gap so the members set is sparse
		}
		row := v1beta1.OMENativeInstanceStatus{
			Index:             index,
			Incarnation:       int64(1 + (i*7)%3),
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   Revision,
			PodCount:          1,
			ServingPodCount:   1,
			AvailablePodCount: 1,
			Admitted:          i%53 != 0,
			ActiveOrdinal:     int32(i % 4 / 3),
		}
		if i%10 == 3 {
			row.RunningRevision = OtherRevision
		}
		if i%20 == 7 {
			row.Phase = v1beta1.OMENativeInstanceUpdating
			row.TargetRevision = OtherRevision
			row.ServingPodCount, row.AvailablePodCount = 0, 0
		}
		if i%(n/13+1) == 5 && i < 13*(n/13+1) {
			row.LastFailure = &v1beta1.InstanceTermination{
				PodName: fmt.Sprintf("example-engine-%d-0", index), ContainerName: Container,
				Reason: "Error", ExitCode: &exitCode, Time: Time,
			}
			row.Operation = &v1beta1.InstanceOperation{
				ID: fmt.Sprintf("2f6b6c8e-0000-4000-8000-%012d", index), Type: v1beta1.InstanceOperationRestart,
				Step: "DeletePods", StartedAt: Time, LastProgressAt: Time, Deadline: TimePlus(time.Hour),
				TargetRevision: Revision, Reason: "PodFailed",
			}
			row.Phase = v1beta1.OMENativeInstanceRestarting
		}
		rows = append(rows, row)
		index++
	}
	return rows
}

// FullFeatureRows touches every retained field: all phases, every operation
// type, conditions, timestamps, pointers, a negative incarnation, both
// ordinals, sparse indices, and a non-ascending row order.
func FullFeatureRows() []v1beta1.OMENativeInstanceStatus {
	exitCode := int32(143)
	surgeIndex := int32(21)
	readySince := TimePlus(-time.Hour)
	condition := func(kind string, status metav1.ConditionStatus, reason string) metav1.Condition {
		return metav1.Condition{Type: kind, Status: status, Reason: reason, Message: reason + " observed", LastTransitionTime: Time, ObservedGeneration: 3}
	}
	operation := func(id string, kind v1beta1.InstanceOperationType, step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{
			ID: id, Type: kind, Step: step, StartedAt: Time, LastProgressAt: TimePlus(time.Minute),
			Deadline: TimePlus(time.Hour), RetryCount: 1, TargetRevision: OtherRevision, Reason: "rollout",
		}
	}
	migrate := operation("op-migrate", v1beta1.InstanceOperationMigrate, "SurgeReady")
	migrate.SurgeIndex = &surgeIndex
	migrate.FromNode = "node-a"
	migrate.HintTargetNodes = []string{"node-c", "node-b"}
	migrate.RequestUUID = "9c0d1e2f-0000-4000-8000-000000000001"
	return []v1beta1.OMENativeInstanceStatus{
		{Index: 5, Incarnation: 3, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: Revision, PodCount: 8, ServingPodCount: 8, AvailablePodCount: 8, Admitted: true, ActiveOrdinal: 1, ReadySince: &readySince,
			Conditions: []metav1.Condition{condition("AllPodsReady", metav1.ConditionTrue, "Ready"), condition("AllPodsAvailable", metav1.ConditionTrue, "Available")}},
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstancePending, Admitted: false},
		{Index: 1, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: Revision, Operation: operation("op-create", v1beta1.InstanceOperationCreate, "WaitReady")},
		{Index: 2, Incarnation: -1, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: Revision, TargetRevision: OtherRevision, PodCount: 8, ServingPodCount: 4, Admitted: true, Operation: operation("op-update", v1beta1.InstanceOperationUpdate, "Drain")},
		{Index: 3, Incarnation: 2, Phase: v1beta1.OMENativeInstanceRestarting, RunningRevision: Revision, PodCount: 8, Admitted: true, Operation: operation("op-restart", v1beta1.InstanceOperationRestart, "DeletePods"),
			LastFailure: &v1beta1.InstanceTermination{PodName: "example-engine-3-2", ContainerName: Container, Reason: "Error", ExitCode: &exitCode, Message: "exit status 143", Time: Time}},
		{Index: 20, Incarnation: 1, Phase: v1beta1.OMENativeInstanceMigrating, RunningRevision: Revision, PodCount: 8, ServingPodCount: 8, AvailablePodCount: 8, Admitted: true, Operation: migrate},
		{Index: 7, Incarnation: 4, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: OtherRevision, Admitted: true, ActiveOrdinal: 1,
			LastFailure: &v1beta1.InstanceTermination{PodName: "example-engine-7-0", Reason: "PodFailed", Time: Time},
			Conditions:  []metav1.Condition{condition("Failed", metav1.ConditionTrue, "PodFailed")}},
		{Index: 2147483647, Incarnation: 9223372036854775807, Phase: v1beta1.OMENativeInstanceDeleting, RunningRevision: OtherRevision, PodCount: 2147483647, ServingPodCount: 1, AvailablePodCount: 2147483647, Operation: operation("op-delete", v1beta1.InstanceOperationDelete, "Drain")},
	}
}

// WireExampleRows is the documented wire-shape example: 500 Ready rows on
// one revision, two reincarnated rows, three rows on ordinal one, and one
// preserved failure.
func WireExampleRows() []v1beta1.OMENativeInstanceStatus {
	rows := UniformRows(500)
	rows[9].Incarnation = 2
	rows[115].Incarnation = 2
	for _, index := range []int{6, 43, 175} {
		rows[index].ActiveOrdinal = 1
	}
	exitCode := int32(143)
	rows[9].LastFailure = &v1beta1.InstanceTermination{PodName: "example-engine-9", ContainerName: Container, Reason: "Error", ExitCode: &exitCode, Time: Time}
	return rows
}

// LogicalStatus wraps rows in the DenseV1 logical status the codec consumes,
// with the aggregate fields a steady IR carries.
func LogicalStatus(rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplicaStatus {
	return &v1beta1.InferenceReplicaStatus{
		Replicas:         int32(len(rows)),
		CurrentRevision:  Revision,
		LabelSelector:    "ome.io/inferenceservice=llama-70b,ome.io/component=engine",
		InstanceStatuses: rows,
	}
}
