package placement

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// verifiedMemberAdmission limits reported admission and readiness to complete,
// owned Pod cohorts. A row's admission bit alone can describe a partial gang.
func verifiedMemberAdmission(ir *v1beta1.InferenceReplica, pods []corev1.Pod, gangSizes map[string]int32) (*v1beta1.InferenceReplicaStatus, error) {
	if ir == nil || ir.UID == "" {
		return nil, fmt.Errorf("admission requires an identified component")
	}
	if ir.Status.ReadyReplicas < 0 {
		return nil, fmt.Errorf("component ready count is negative")
	}
	cohorts := map[memberPodSlot][]*corev1.Pod{}
	for i := range pods {
		pod := &pods[i]
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.UID != ir.UID || owner.Kind != "InferenceReplica" || owner.APIVersion != v1beta1.SchemeGroupVersion.String() || pod.UID == "" {
			return nil, fmt.Errorf("pod %q has unverified component ownership", pod.Name)
		}
		slot, err := memberSlot(pod)
		if err != nil {
			return nil, err
		}
		if slot.Group != "" {
			slot.Ordinal = 0
		}
		cohorts[slot] = append(cohorts[slot], pod)
	}
	byIndex := map[int32][]memberPodSlot{}
	for slot := range cohorts {
		byIndex[slot.Index] = append(byIndex[slot.Index], slot)
	}
	status := ir.Status.DeepCopy()
	status.ReadyReplicas = 0
	seen := map[int32]bool{}
	for i := range status.InstanceStatuses {
		row := &status.InstanceStatuses[i]
		if row.Index < 0 || row.Incarnation < 0 || seen[row.Index] {
			return nil, fmt.Errorf("component has invalid or duplicate instance %d", row.Index)
		}
		seen[row.Index] = true
		if !row.Admitted {
			continue
		}
		row.Admitted = false
		revision := row.RunningRevision
		if revision == "" {
			revision = row.TargetRevision
			if revision == "" && row.Operation != nil {
				revision = row.Operation.TargetRevision
			}
		}
		ready := false
		for _, slot := range byIndex[row.Index] {
			if slot.Incarnation != row.Incarnation || slot.Revision != query.RevisionHashFromControllerRevisionName(revision) {
				continue
			}
			if slot.Group == "" && slot.Ordinal != row.ActiveOrdinal {
				continue
			}
			cohort := cohorts[slot]
			if !completeAdmittedCohort(cohort, slot.Group, gangSizes) {
				continue
			}
			row.Admitted = true
			serving := row.RunningRevision != "" && int64(row.ServingPodCount) >= int64(len(cohort))
			for _, pod := range cohort {
				serving = serving && livePodReady(pod)
			}
			ready = ready || serving
		}
		if ready {
			status.ReadyReplicas++
		}
	}
	status.ReadyReplicas = min(status.ReadyReplicas, ir.Status.ReadyReplicas)
	return status, nil
}

func completeAdmittedCohort(pods []*corev1.Pod, group string, gangSizes map[string]int32) bool {
	expected := int32(1)
	if group != "" {
		expected = gangSizes[group]
	}
	if expected <= 0 || int64(len(pods)) != int64(expected) {
		return false
	}
	type runnerSlot struct {
		runner  string
		ordinal int32
	}
	seen := map[runnerSlot]bool{}
	for _, pod := range pods {
		if !pod.DeletionTimestamp.IsZero() || workloadtypes.PodAdmissionGated(pod) || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			return false
		}
		if group != "" {
			slot, _ := memberSlot(pod)
			key := runnerSlot{runner: pod.Labels[query.LabelRunner], ordinal: slot.Ordinal}
			if key.runner == "" || seen[key] {
				return false
			}
			seen[key] = true
		}
	}
	return true
}
