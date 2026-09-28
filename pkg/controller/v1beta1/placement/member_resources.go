package placement

import (
	"context"
	"fmt"
	"math"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

type memberResourceCount struct {
	Occupied int32
	Reserved int32
	Ready    int32
}

type memberPodSlot struct {
	Index       int32
	Incarnation int64
	Ordinal     int32
	Revision    string
	Group       string
}

// countMemberResources counts whole physical instances and durable surge slots
// without treating a pending or terminating Pod as released capacity.
func countMemberResources(ir *v1beta1.InferenceReplica, pods []corev1.Pod, gangSizes map[string]int32) (memberResourceCount, error) {
	var out memberResourceCount
	if ir == nil || ir.UID == "" || len(ir.Spec.Runners) == 0 {
		return out, fmt.Errorf("physical accounting requires an identified component and runner shape")
	}
	for _, runner := range ir.Spec.Runners {
		if runner.Size <= 0 {
			return out, fmt.Errorf("component runner size is unresolved")
		}
	}
	if ir.Status.ReadyReplicas < 0 {
		return out, fmt.Errorf("component ready count is negative")
	}
	slots := map[memberPodSlot][]*corev1.Pod{}
	byIndex := map[int32][]*corev1.Pod{}
	for i := range pods {
		pod := &pods[i]
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.UID != ir.UID || owner.Kind != "InferenceReplica" || owner.APIVersion != v1beta1.SchemeGroupVersion.String() || pod.UID == "" {
			return out, fmt.Errorf("pod %q has unverified component ownership", pod.Name)
		}
		slot, err := memberSlot(pod)
		if err != nil {
			return out, err
		}
		byIndex[slot.Index] = append(byIndex[slot.Index], pod)
		// PodGroup identity survives changes to the component's desired shape.
		// An incomplete gang still occupies one whole replica.
		if slot.Group != "" {
			slot.Ordinal = 0
		}
		if slot.Group == "" || len(slots[slot]) == 0 {
			if out.Occupied == math.MaxInt32 {
				return out, fmt.Errorf("physical replica count exceeds int32")
			}
			out.Occupied++
		}
		slots[slot] = append(slots[slot], pod)
	}
	rows := map[int32]v1beta1.OMENativeInstanceStatus{}
	for _, row := range ir.Status.InstanceStatuses {
		if _, duplicate := rows[row.Index]; duplicate {
			return out, fmt.Errorf("component contains duplicate instance %d", row.Index)
		}
		rows[row.Index] = row
		// A steady row and an exact live Pod cohort provide readiness credit.
		// In-flight operations keep their occupancy without promising replacement
		// readiness from a cached aggregate or a partially materialized gang.
		live := byIndex[row.Index]
		if row.Phase != v1beta1.OMENativeInstanceReady || !row.Admitted || row.Operation != nil || row.PodCount <= 0 || int64(len(live)) != int64(row.PodCount) || row.ServingPodCount != row.PodCount {
			continue
		}
		ready := true
		for _, pod := range live {
			slot, _ := memberSlot(pod)
			expected := int32(1)
			if slot.Group != "" {
				expected = gangSizes[slot.Group]
			}
			ready = ready && expected > 0 && int64(len(live)) == int64(expected)
			ready = ready && slot.Incarnation == row.Incarnation && slot.Revision == query.RevisionHashFromControllerRevisionName(row.RunningRevision) && livePodReady(pod)
		}
		if ready {
			out.Ready++
		}
	}
	reservations := map[memberPodSlot]struct{}{}
	reserve := func(index int32, incarnation int64, ordinal int32, revision string, gang bool) error {
		hash := query.RevisionHashFromControllerRevisionName(revision)
		if index < 0 || incarnation < 0 || hash == "" {
			return fmt.Errorf("surge reservation has unresolved instance identity or revision")
		}
		slot := memberPodSlot{Index: index, Incarnation: incarnation, Ordinal: ordinal, Revision: hash}
		for physical := range slots {
			if physical.Index == index && physical.Incarnation == incarnation && physical.Revision == hash && (gang || physical.Ordinal == ordinal) {
				return nil
			}
		}
		reservations[slot] = struct{}{}
		return nil
	}
	for _, row := range rows {
		op := row.Operation
		if op == nil || op.Type != v1beta1.InstanceOperationUpdate {
			continue
		}
		if op.Step != workloadtypes.UpdateStepSurge && op.Step != workloadtypes.UpdateStepSurgeDrain && op.Step != workloadtypes.UpdateStepSurgeDrainSettle {
			continue
		}
		if op.SurgeIndex != nil {
			target, exists := rows[*op.SurgeIndex]
			if !exists {
				return out, fmt.Errorf("surge reservation has no target instance")
			}
			if err := reserve(*op.SurgeIndex, target.Incarnation, 0, op.TargetRevision, true); err != nil {
				return out, err
			}
		} else {
			if row.ActiveOrdinal < 0 || row.ActiveOrdinal > 1 {
				return out, fmt.Errorf("surge reservation has unresolved pod slot")
			}
			if err := reserve(row.Index, row.Incarnation, 1-row.ActiveOrdinal, op.TargetRevision, false); err != nil {
				return out, err
			}
		}
	}
	for _, migration := range ir.Status.Migrations {
		if migration.Phase.Terminal() {
			continue
		}
		if migration.SurgeInstance == nil || *migration.SurgeInstance < 0 {
			if migration.Phase != v1beta1.MigrationPhaseAccepted {
				return out, fmt.Errorf("active migration has no surge identity")
			}
			continue
		}
		target, exists := rows[*migration.SurgeInstance]
		if !exists {
			return out, fmt.Errorf("migration reservation has no target instance")
		}
		revision := target.RunningRevision
		if revision == "" {
			revision = rows[migration.SourceInstance].RunningRevision
		}
		if err := reserve(*migration.SurgeInstance, target.Incarnation, target.ActiveOrdinal, revision, true); err != nil {
			return out, err
		}
	}
	if len(reservations) > math.MaxInt32 {
		return out, fmt.Errorf("surge reservations exceed int32")
	}
	out.Reserved = int32(len(reservations))
	out.Ready = min(out.Ready, ir.Status.ReadyReplicas)
	return out, nil
}

// memberGangSizes reads the expected shape carried by each live gang, which
// can differ from the component's current runner templates during a rollout.
func memberGangSizes(ctx context.Context, reads client.Reader, ir *v1beta1.InferenceReplica, pods []corev1.Pod) (map[string]int32, error) {
	sizes := map[string]int32{}
	for _, pod := range pods {
		name := pod.Labels[query.LabelPodGroup]
		if name == "" || sizes[name] > 0 {
			continue
		}
		group := &unstructured.Unstructured{}
		group.SetGroupVersionKind(schedulingv1alpha1.SchemeGroupVersion.WithKind(constants.PodGroupKind))
		if err := reads.Get(ctx, client.ObjectKey{Namespace: ir.Namespace, Name: name}, group); err != nil {
			return nil, err
		}
		owner := metav1.GetControllerOf(group)
		if group.GetUID() == "" || !group.GetDeletionTimestamp().IsZero() || owner == nil || owner.UID != ir.UID || owner.Kind != "InferenceReplica" || owner.APIVersion != v1beta1.SchemeGroupVersion.String() {
			return nil, fmt.Errorf("gang %q has unverified component identity", name)
		}
		size, found, err := unstructured.NestedInt64(group.Object, "spec", "minMember")
		if err != nil || !found || size <= 1 || size > math.MaxInt32 {
			return nil, fmt.Errorf("gang %q has unresolved size", name)
		}
		sizes[name] = int32(size)
	}
	return sizes, nil
}

func memberSlot(pod *corev1.Pod) (memberPodSlot, error) {
	var out memberPodSlot
	index, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
	if err != nil || index < 0 {
		return out, fmt.Errorf("pod %q has no valid instance index", pod.Name)
	}
	out.Index = int32(index)
	// Omitted incarnation and ordinal are the protocol's original zero slots.
	if raw := pod.Labels[query.LabelInstanceIncarnation]; raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			return out, fmt.Errorf("pod %q has invalid incarnation", pod.Name)
		}
		out.Incarnation = value
	}
	if raw := pod.Labels[query.LabelPodOrdinal]; raw != "" {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || value < 0 {
			return out, fmt.Errorf("pod %q has invalid pod ordinal", pod.Name)
		}
		out.Ordinal = int32(value)
	}
	out.Revision = pod.Labels[query.LabelRevisionHash]
	out.Group = pod.Labels[query.LabelPodGroup]
	if out.Revision == "" {
		return out, fmt.Errorf("pod %q has no revision identity", pod.Name)
	}
	return out, nil
}

func livePodReady(pod *corev1.Pod) bool {
	if !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	ready, serving := false, false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			ready = condition.Status == corev1.ConditionTrue
		}
		if condition.Type == query.ServingConditionType {
			serving = condition.Status == corev1.ConditionTrue
		}
	}
	return ready && serving
}
