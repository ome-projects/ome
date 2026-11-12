package inferencereplica

import (
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// publishedStatusEqual reports whether two statuses of a replica are the
// same status under the rule the apiserver's semantic compare applies: a
// time equals another at the same instant whatever its location, and a nil
// slice or map equals an empty one. The status write asks it of the status
// it recomputed and the one it started from; equal means the live object
// needs no write.
//
// The rows are read field by field: the semantic compare walks them by
// reflection and copies every field of every row on the way, thousands of
// rows on every pass. The fields beside the rows, a few dozen whatever the
// row count, are left to the semantic compare.
func publishedStatusEqual(a, b *v1beta1.InferenceReplicaStatus) bool {
	if !instanceStatusesEqual(a.InstanceStatuses, b.InstanceStatuses) {
		return false
	}
	rest, other := *a, *b
	rest.InstanceStatuses, other.InstanceStatuses = nil, nil
	return equality.Semantic.DeepEqual(rest, other)
}

// instanceStatusesEqual reports whether two row slices hold the same rows
// in the same order; a nil slice and an empty one are the same.
func instanceStatusesEqual(a, b []v1beta1.OMENativeInstanceStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !instanceStatusEqual(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

// instanceStatusEqual reads every field of a row. A field added to the row
// type is read here too: the compare's contract test changes every leaf of
// the type one at a time and fails on a field this function does not read.
func instanceStatusEqual(a, b *v1beta1.OMENativeInstanceStatus) bool {
	return a.Index == b.Index &&
		a.Incarnation == b.Incarnation &&
		a.Phase == b.Phase &&
		a.RunningRevision == b.RunningRevision &&
		a.TargetRevision == b.TargetRevision &&
		a.PodCount == b.PodCount &&
		a.ReadyPodCount == b.ReadyPodCount &&
		a.ServingPodCount == b.ServingPodCount &&
		a.AvailablePodCount == b.AvailablePodCount &&
		a.ScheduledPodCount == b.ScheduledPodCount &&
		a.Admitted == b.Admitted &&
		slices.Equal(a.NodesOccupied, b.NodesOccupied) &&
		conditionsEqual(a.Conditions, b.Conditions) &&
		timePointersEqual(a.ReadySince, b.ReadySince) &&
		operationsEqual(a.Operation, b.Operation) &&
		a.ActiveOrdinal == b.ActiveOrdinal &&
		terminationsEqual(a.LastFailure, b.LastFailure) &&
		slices.Equal(a.Announced, b.Announced)
}

// conditionsEqual reads two condition lists in order; a nil list and an
// empty one are the same.
func conditionsEqual(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Status != b[i].Status || a[i].ObservedGeneration != b[i].ObservedGeneration ||
			!timesEqual(a[i].LastTransitionTime, b[i].LastTransitionTime) || a[i].Reason != b[i].Reason || a[i].Message != b[i].Message {
			return false
		}
	}
	return true
}

// operationsEqual reads two operation records; both absent is equal.
func operationsEqual(a, b *v1beta1.InstanceOperation) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ID == b.ID &&
		a.Type == b.Type &&
		a.Step == b.Step &&
		timesEqual(a.StartedAt, b.StartedAt) &&
		timesEqual(a.LastProgressAt, b.LastProgressAt) &&
		timesEqual(a.Deadline, b.Deadline) &&
		a.RetryCount == b.RetryCount &&
		a.TargetRevision == b.TargetRevision &&
		a.Reason == b.Reason &&
		a.Waiting == b.Waiting &&
		timePointersEqual(a.CapacityRefusedAt, b.CapacityRefusedAt) &&
		timePointersEqual(a.PlacementWaitSince, b.PlacementWaitSince) &&
		a.Strategy == b.Strategy &&
		int32PointersEqual(a.SurgeIndex, b.SurgeIndex) &&
		a.FromNode == b.FromNode &&
		slices.Equal(a.HintTargetNodes, b.HintTargetNodes) &&
		a.RequestUUID == b.RequestUUID
}

// terminationsEqual reads two failure records; both absent is equal.
func terminationsEqual(a, b *v1beta1.InstanceTermination) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.PodName == b.PodName &&
		a.ContainerName == b.ContainerName &&
		a.Reason == b.Reason &&
		int32PointersEqual(a.ExitCode, b.ExitCode) &&
		a.Message == b.Message &&
		timesEqual(a.Time, b.Time)
}

// timesEqual is the semantic compare's rule for a time: the same instant,
// whatever the location or the monotonic reading.
func timesEqual(a, b metav1.Time) bool { return a.UTC() == b.UTC() }

// timePointersEqual reads two optional times; both absent is equal.
func timePointersEqual(a, b *metav1.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return timesEqual(*a, *b)
}

// int32PointersEqual reads two optional counts; both absent is equal.
func int32PointersEqual(a, b *int32) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
