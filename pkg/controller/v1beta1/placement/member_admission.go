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
	cohorts, err := memberPodCohorts(ir, pods)
	if err != nil {
		return nil, err
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
		admitted, ready := cohorts.rowAdmission(row, gangSizes)
		row.Admitted = admitted
		if ready {
			status.ReadyReplicas++
		}
	}
	status.ReadyReplicas = min(status.ReadyReplicas, ir.Status.ReadyReplicas)
	return status, nil
}

// podCohorts are a component's pods grouped by slot: a gang's pods share the
// slot of their group, a single pod keeps its ordinal.
type podCohorts struct {
	bySlot  map[memberPodSlot][]*corev1.Pod
	byIndex map[int32][]memberPodSlot
}

// memberPodCohorts groups pods by slot and refuses a pod the component does
// not control or whose identity labels do not parse.
func memberPodCohorts(ir *v1beta1.InferenceReplica, pods []corev1.Pod) (podCohorts, error) {
	cohorts := podCohorts{bySlot: map[memberPodSlot][]*corev1.Pod{}, byIndex: map[int32][]memberPodSlot{}}
	for i := range pods {
		pod := &pods[i]
		if !controlledByComponent(pod, ir) {
			return podCohorts{}, fmt.Errorf("pod %q has unverified component ownership", pod.Name)
		}
		slot, err := memberSlot(pod)
		if err != nil {
			return podCohorts{}, err
		}
		if slot.Group != "" {
			slot.Ordinal = 0
		}
		cohorts.bySlot[slot] = append(cohorts.bySlot[slot], pod)
	}
	for slot := range cohorts.bySlot {
		cohorts.byIndex[slot.Index] = append(cohorts.byIndex[slot.Index], slot)
	}
	return cohorts, nil
}

// controlledByComponent reports whether pod names the component as its
// controller and carries a UID of its own.
func controlledByComponent(pod *corev1.Pod, ir *v1beta1.InferenceReplica) bool {
	owner := metav1.GetControllerOf(pod)
	return owner != nil && owner.UID == ir.UID && owner.Kind == "InferenceReplica" &&
		owner.APIVersion == v1beta1.SchemeGroupVersion.String() && pod.UID != ""
}

// rowAdmission reports whether a row that claims admission has a complete
// cohort on its revision and attempt, and whether one of those cohorts serves.
func (c podCohorts) rowAdmission(row *v1beta1.OMENativeInstanceStatus, gangSizes map[string]int32) (admitted, ready bool) {
	revision := query.RevisionHashFromControllerRevisionName(rowRevision(row))
	for _, slot := range c.byIndex[row.Index] {
		if slot.Incarnation != row.Incarnation || slot.Revision != revision {
			continue
		}
		if slot.Group == "" && slot.Ordinal != row.ActiveOrdinal {
			continue
		}
		cohort := c.bySlot[slot]
		if !completeAdmittedCohort(cohort, slot.Group, gangSizes) {
			continue
		}
		admitted = true
		ready = ready || cohortServes(row, cohort)
	}
	return admitted, ready
}

// rowRevision is the revision a row's pods are expected to carry: the running
// one, else the target, else the operation's target.
func rowRevision(row *v1beta1.OMENativeInstanceStatus) string {
	switch {
	case row.RunningRevision != "":
		return row.RunningRevision
	case row.TargetRevision != "":
		return row.TargetRevision
	case row.Operation != nil:
		return row.Operation.TargetRevision
	}
	return ""
}

// cohortServes reports whether a running row counts its whole cohort as
// serving and every pod of the cohort is live and ready.
func cohortServes(row *v1beta1.OMENativeInstanceStatus, cohort []*corev1.Pod) bool {
	if row.RunningRevision == "" || int64(row.ServingPodCount) < int64(len(cohort)) {
		return false
	}
	for _, pod := range cohort {
		if !livePodReady(pod) {
			return false
		}
	}
	return true
}

// completeAdmittedCohort reports whether pods are the whole cohort of their
// slot: one pod, or a gang of the recorded size with every runner slot filled
// once, none of them leaving, gated or finished.
func completeAdmittedCohort(pods []*corev1.Pod, group string, gangSizes map[string]int32) bool {
	expected := int32(1)
	if group != "" {
		expected = gangSizes[group]
	}
	if expected <= 0 || int64(len(pods)) != int64(expected) {
		return false
	}
	for _, pod := range pods {
		if podLeftAdmission(pod) {
			return false
		}
	}
	return group == "" || distinctRunnerSlots(pods)
}

// podLeftAdmission reports a pod that is leaving, held by a scheduling gate or
// finished, so it cannot be part of an admitted cohort.
func podLeftAdmission(pod *corev1.Pod) bool {
	return !pod.DeletionTimestamp.IsZero() || workloadtypes.PodAdmissionGated(pod) ||
		pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// distinctRunnerSlots reports whether every pod of a gang names a runner and no
// two pods share a runner and ordinal.
func distinctRunnerSlots(pods []*corev1.Pod) bool {
	type runnerSlot struct {
		runner  string
		ordinal int32
	}
	seen := map[runnerSlot]bool{}
	for _, pod := range pods {
		slot, _ := memberSlot(pod)
		key := runnerSlot{runner: pod.Labels[query.LabelRunner], ordinal: slot.Ordinal}
		if key.runner == "" || seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}
