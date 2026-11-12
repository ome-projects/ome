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
	if err := checkMemberShape(ir); err != nil {
		return memberResourceCount{}, err
	}
	live, err := occupiedMemberSlots(ir, pods)
	if err != nil {
		return memberResourceCount{}, err
	}
	rows, err := memberRows(ir)
	if err != nil {
		return memberResourceCount{}, err
	}
	reserved, err := countMemberReservations(ir, rows, live.slots)
	if err != nil {
		return memberResourceCount{}, err
	}
	return memberResourceCount{
		Occupied: live.occupied,
		Reserved: reserved,
		Ready:    min(countReadyMemberRows(ir, live.byIndex, gangSizes), ir.Status.ReadyReplicas),
	}, nil
}

// checkMemberShape refuses a member the count cannot trust: one without an
// identity or a runner shape, a runner of unresolved size, or a negative
// ready count.
func checkMemberShape(ir *v1beta1.InferenceReplica) error {
	if ir == nil || ir.UID == "" || len(ir.Spec.Runners) == 0 {
		return fmt.Errorf("physical accounting requires an identified component and runner shape")
	}
	for _, runner := range ir.Spec.Runners {
		if runner.Size <= 0 {
			return fmt.Errorf("component runner size is unresolved")
		}
	}
	if ir.Status.ReadyReplicas < 0 {
		return fmt.Errorf("component ready count is negative")
	}
	return nil
}

// memberPods are the live pods of one member: by slot, where a gang's pods
// share one slot, and by instance index, with the whole replicas they occupy.
type memberPods struct {
	slots    map[memberPodSlot][]*corev1.Pod
	byIndex  map[int32][]*corev1.Pod
	occupied int32
}

// occupiedMemberSlots files every pod the member controls under its slot. A
// pod outside the member's control, or without an identity, is an error.
func occupiedMemberSlots(ir *v1beta1.InferenceReplica, pods []corev1.Pod) (memberPods, error) {
	live := memberPods{slots: map[memberPodSlot][]*corev1.Pod{}, byIndex: map[int32][]*corev1.Pod{}}
	for i := range pods {
		pod := &pods[i]
		if !controlledByMember(metav1.GetControllerOf(pod), ir) || pod.UID == "" {
			return live, fmt.Errorf("pod %q has unverified component ownership", pod.Name)
		}
		slot, err := memberSlot(pod)
		if err != nil {
			return live, err
		}
		if err := live.add(slot, pod); err != nil {
			return live, err
		}
	}
	return live, nil
}

// add files the pod under its slot and its index. A gang's pods share one
// slot, so PodGroup identity survives changes to the component's desired shape
// and an incomplete gang still occupies one whole replica.
func (m *memberPods) add(slot memberPodSlot, pod *corev1.Pod) error {
	m.byIndex[slot.Index] = append(m.byIndex[slot.Index], pod)
	if slot.Group != "" {
		slot.Ordinal = 0
	}
	if slot.Group == "" || len(m.slots[slot]) == 0 {
		if m.occupied == math.MaxInt32 {
			return fmt.Errorf("physical replica count exceeds int32")
		}
		m.occupied++
	}
	m.slots[slot] = append(m.slots[slot], pod)
	return nil
}

// controlledByMember reports whether owner is the member's own controller
// reference: its UID, its kind and its API version.
func controlledByMember(owner *metav1.OwnerReference, ir *v1beta1.InferenceReplica) bool {
	return owner != nil && owner.UID == ir.UID && owner.Kind == "InferenceReplica" && owner.APIVersion == v1beta1.SchemeGroupVersion.String()
}

// memberRows indexes the member's instance rows; an index that repeats is an
// error.
func memberRows(ir *v1beta1.InferenceReplica) (map[int32]v1beta1.OMENativeInstanceStatus, error) {
	rows := make(map[int32]v1beta1.OMENativeInstanceStatus, len(ir.Status.InstanceStatuses))
	for _, row := range ir.Status.InstanceStatuses {
		if _, duplicate := rows[row.Index]; duplicate {
			return nil, fmt.Errorf("component contains duplicate instance %d", row.Index)
		}
		rows[row.Index] = row
	}
	return rows, nil
}

// countReadyMemberRows counts the rows whose live pods prove readiness. An
// operation under way keeps its occupancy without promising replacement
// readiness from a cached aggregate or a partly built gang.
func countReadyMemberRows(ir *v1beta1.InferenceReplica, byIndex map[int32][]*corev1.Pod, gangSizes map[string]int32) int32 {
	var ready int32
	for _, row := range ir.Status.InstanceStatuses {
		if memberRowReady(row, byIndex[row.Index], gangSizes) {
			ready++
		}
	}
	return ready
}

// memberRowReady reports whether a row's live pods prove it ready: a steady,
// admitted row with no operation, whose live pods are exactly its cohort and
// a whole gang where it has one, each Ready and serving on the row's own
// attempt and revision.
func memberRowReady(row v1beta1.OMENativeInstanceStatus, live []*corev1.Pod, gangSizes map[string]int32) bool {
	if row.Phase != v1beta1.OMENativeInstanceReady || !row.Admitted || row.Operation != nil || row.PodCount <= 0 || int64(len(live)) != int64(row.PodCount) || row.ServingPodCount != row.PodCount {
		return false
	}
	revision := query.RevisionHashFromControllerRevisionName(row.RunningRevision)
	for _, pod := range live {
		slot, _ := memberSlot(pod)
		expected := int32(1)
		if slot.Group != "" {
			expected = gangSizes[slot.Group]
		}
		if expected <= 0 || int64(len(live)) != int64(expected) {
			return false
		}
		if slot.Incarnation != row.Incarnation || slot.Revision != revision || !livePodReady(pod) {
			return false
		}
	}
	return true
}

// memberReservations are the surge slots the member's update and migration
// rows hold that no live pod fills yet.
type memberReservations struct {
	rows map[int32]v1beta1.OMENativeInstanceStatus
	live map[memberPodSlot][]*corev1.Pod
	held map[memberPodSlot]struct{}
}

// countMemberReservations counts the surge slots the member's rows reserve:
// one per surging update and per migration under way, unless a live pod
// already fills the slot.
func countMemberReservations(ir *v1beta1.InferenceReplica, rows map[int32]v1beta1.OMENativeInstanceStatus, live map[memberPodSlot][]*corev1.Pod) (int32, error) {
	reservations := memberReservations{rows: rows, live: live, held: map[memberPodSlot]struct{}{}}
	if err := reservations.reserveUpdateSurges(); err != nil {
		return 0, err
	}
	if err := reservations.reserveMigrationSurges(ir.Status.Migrations); err != nil {
		return 0, err
	}
	if len(reservations.held) > math.MaxInt32 {
		return 0, fmt.Errorf("surge reservations exceed int32")
	}
	return int32(len(reservations.held)), nil
}

// reserve holds the slot unless a live pod of the same instance, attempt and
// revision (any ordinal for a gang) already fills it.
func (r *memberReservations) reserve(index int32, incarnation int64, ordinal int32, revision string, gang bool) error {
	hash := query.RevisionHashFromControllerRevisionName(revision)
	if index < 0 || incarnation < 0 || hash == "" {
		return fmt.Errorf("surge reservation has unresolved instance identity or revision")
	}
	slot := memberPodSlot{Index: index, Incarnation: incarnation, Ordinal: ordinal, Revision: hash}
	for physical := range r.live {
		if physical.Index == index && physical.Incarnation == incarnation && physical.Revision == hash && (gang || physical.Ordinal == ordinal) {
			return nil
		}
	}
	r.held[slot] = struct{}{}
	return nil
}

// reserveUpdateSurges reserves the slot of every row whose update is in a
// surge step: the gang target's slot when the update names one, else the
// row's other pod ordinal.
func (r *memberReservations) reserveUpdateSurges() error {
	for _, row := range r.rows {
		op := row.Operation
		if op == nil || op.Type != v1beta1.InstanceOperationUpdate || !surgeStep(op.Step) {
			continue
		}
		if op.SurgeIndex != nil {
			target, exists := r.rows[*op.SurgeIndex]
			if !exists {
				return fmt.Errorf("surge reservation has no target instance")
			}
			if err := r.reserve(*op.SurgeIndex, target.Incarnation, 0, op.TargetRevision, true); err != nil {
				return err
			}
			continue
		}
		if row.ActiveOrdinal < 0 || row.ActiveOrdinal > 1 {
			return fmt.Errorf("surge reservation has unresolved pod slot")
		}
		if err := r.reserve(row.Index, row.Incarnation, 1-row.ActiveOrdinal, op.TargetRevision, false); err != nil {
			return err
		}
	}
	return nil
}

// surgeStep reports whether an update step holds a surge slot.
func surgeStep(step string) bool {
	return step == workloadtypes.UpdateStepSurge || step == workloadtypes.UpdateStepSurgeDrain || step == workloadtypes.UpdateStepSurgeDrainSettle
}

// reserveMigrationSurges reserves the target slot of every migration under
// way. An accepted migration without a target holds nothing yet; any other
// active migration without one is an error.
func (r *memberReservations) reserveMigrationSurges(migrations []v1beta1.MigrationStatus) error {
	for _, migration := range migrations {
		if migration.Phase.Terminal() {
			continue
		}
		if migration.SurgeInstance == nil || *migration.SurgeInstance < 0 {
			if migration.Phase != v1beta1.MigrationPhaseAccepted {
				return fmt.Errorf("active migration has no surge identity")
			}
			continue
		}
		target, exists := r.rows[*migration.SurgeInstance]
		if !exists {
			return fmt.Errorf("migration reservation has no target instance")
		}
		revision := target.RunningRevision
		if revision == "" {
			revision = r.rows[migration.SourceInstance].RunningRevision
		}
		if err := r.reserve(*migration.SurgeInstance, target.Incarnation, target.ActiveOrdinal, revision, true); err != nil {
			return err
		}
	}
	return nil
}

// memberGangSizes reads the expected shape carried by each live gang, which
// can differ from the component's current runner templates during a rollout.
// One list covers the component so the read cost does not grow with replicas;
// a gang the list misses is still read by name.
func memberGangSizes(ctx context.Context, reads client.Reader, ir *v1beta1.InferenceReplica, pods []corev1.Pod) (map[string]int32, error) {
	sizes := map[string]int32{}
	var listed map[string]*unstructured.Unstructured
	for _, pod := range pods {
		name := pod.Labels[query.LabelPodGroup]
		if name == "" || sizes[name] > 0 {
			continue
		}
		if listed == nil {
			var err error
			if listed, err = listMemberGangs(ctx, reads, ir); err != nil {
				return nil, err
			}
		}
		group, ok := listed[name]
		if !ok {
			group = &unstructured.Unstructured{}
			group.SetGroupVersionKind(schedulingv1alpha1.SchemeGroupVersion.WithKind(constants.PodGroupKind))
			if err := reads.Get(ctx, client.ObjectKey{Namespace: ir.Namespace, Name: name}, group); err != nil {
				return nil, err
			}
		}
		if group.GetUID() == "" || !group.GetDeletionTimestamp().IsZero() || !controlledByMember(metav1.GetControllerOf(group), ir) {
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

// listMemberGangs indexes the component's gangs by name through the service
// and component labels their creator sets on every PodGroup.
func listMemberGangs(ctx context.Context, reads client.Reader, ir *v1beta1.InferenceReplica) (map[string]*unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schedulingv1alpha1.SchemeGroupVersion.WithKind(constants.PodGroupKind + "List"))
	if err := reads.List(ctx, list, client.InNamespace(ir.Namespace), client.MatchingLabels{
		constants.InferenceServicePodLabelKey: ir.NamePrefix(),
		constants.OMEComponentLabel:           string(ir.Spec.Component),
	}); err != nil {
		return nil, err
	}
	out := make(map[string]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		out[list.Items[i].GetName()] = &list.Items[i]
	}
	return out, nil
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
