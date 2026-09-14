package engine

import (
	"reflect"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The observation used by policy selection can be older than the lossless
// capture. Neither BuildRequest nor a fresh worker result alone proves that
// it still describes the same source selected by that policy.
func predictionOwnersMatch(observed *snapshot.ClusterSnapshot, fresh *input.Snapshot, c policy.Candidate) bool {
	w := observed.Workloads[c.Workload]
	if w == nil || w.ISVC == nil || w.ISVC.UID == "" {
		return false
	}
	comp := w.Components[c.Component]
	if comp == nil || comp.IR == nil || comp.IR.UID == "" {
		return false
	}
	var isvc *v1beta1.InferenceService
	for i := range fresh.InferenceServices {
		x := &fresh.InferenceServices[i]
		if x.Namespace == c.Workload.Namespace && x.Name == c.Workload.Name {
			isvc = x
			break
		}
	}
	if isvc == nil || isvc.UID != w.ISVC.UID || isvc.Generation != w.ISVC.Generation {
		return false
	}
	oldStatus, oldOK := w.ISVC.Status.Components[c.Component]
	newStatus, newOK := isvc.Status.Components[c.Component]
	if !oldOK || !newOK || !reflect.DeepEqual(oldStatus.Lifecycle, newStatus.Lifecycle) || oldStatus.RolloutPhase != newStatus.RolloutPhase {
		return false
	}
	var ir *v1beta1.InferenceReplica
	for i := range fresh.InferenceReplicas {
		x := &fresh.InferenceReplicas[i]
		if x.Namespace == comp.IR.Namespace && x.Name == comp.IR.Name {
			ir = x
			break
		}
	}
	if ir == nil || ir.UID != comp.IR.UID || ir.Generation != comp.IR.Generation ||
		ir.Status.ObservedGeneration != comp.IR.Status.ObservedGeneration || ir.Status.CurrentRevision != comp.IR.Status.CurrentRevision || ir.Status.UpdateRevision != comp.IR.Status.UpdateRevision {
		return false
	}
	instance := predictionInstance(comp, c.Instance)
	if instance == nil {
		return false
	}
	for _, row := range ir.Status.InstanceStatuses {
		if row.Index == c.Instance {
			return row.Incarnation == instance.Incarnation && row.RunningRevision == instance.RunningRevision && row.TargetRevision == instance.TargetRevision
		}
	}
	return false
}

func predictionMembersMatch(observed *snapshot.ClusterSnapshot, c policy.Candidate, pods []corev1.Pod) bool {
	instance := predictionInstance(observed.Workloads[c.Workload].Components[c.Component], c.Instance)
	if instance == nil || len(pods) == 0 || len(instance.Pods) != len(pods) {
		return false
	}
	members := make(map[types.UID]snapshot.PodInfo, len(instance.Pods))
	for _, pod := range instance.Pods {
		if pod.UID == "" {
			return false
		}
		if _, exists := members[pod.UID]; exists {
			return false
		}
		members[pod.UID] = pod
	}
	for _, pod := range pods {
		old, ok := members[pod.UID]
		if !ok || old.Namespace != pod.Namespace || old.Name != pod.Name || old.Node != pod.Spec.NodeName ||
			old.GPUs != snapshot.PodGPURequest(&pod) ||
			string(old.Runner) != pod.Labels["ome.io/runner"] || strconv.FormatInt(int64(old.PodOrdinal), 10) != pod.Labels["ome.io/pod-ordinal"] ||
			strconv.FormatInt(old.Incarnation, 10) != pod.Labels["ome.io/instance-incarnation"] {
			return false
		}
		delete(members, pod.UID)
	}
	return len(members) == 0
}

func predictionInstance(comp *snapshot.Component, index int32) *snapshot.Instance {
	for _, instance := range comp.Instances {
		if instance.Index == index {
			return instance
		}
	}
	return nil
}
