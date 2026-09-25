// Package podgroup builds and reconciles scheduler-plugins
// `scheduling.x-k8s.io/v1alpha1` PodGroup objects for OMENative multi-pod
// Instances.
//
// Gang scheduling contract: every multi-pod Instance (leader + N workers)
// gets one PodGroup with MinMember == TotalPodsForInstance(inst). The
// scheduler-plugins coscheduler enforces that all minMember pods can be
// placed before any is bound to a node — without it, the leader can
// schedule alone, the workers stay Pending, and the LLM runtime hangs
// waiting for peer connections.
//
// Single-pod Instances do NOT get a PodGroup — there's nothing to gang.
//
// The CRD is optional: when `scheduling.x-k8s.io` is not installed on
// the cluster, the controller still creates pods but emits a
// `GangSchedulingUnavailable=True` Condition on the Component. The
// failure mode (potential partial gang placement) is documented; it is
// soft, not hard.
//
// SchedulerName contract — operator's responsibility. The PodGroup
// objects this package emits are only honored by a scheduler that
// reads `scheduling.x-k8s.io/v1alpha1` (today: scheduler-plugins'
// Coscheduling plugin, or a custom equivalent). The operator must set
// `runtime.spec.schedulerName` on the ServingRuntime (or
// `podSpec.schedulerName` on the InferenceService) to a gang-aware
// scheduler name — for example `ome-scheduler` when this repository's
// secondary scheduler is deployed. The controller
// does NOT enforce or default this field (the operator may have installed
// Coscheduling as a plugin inside the default scheduler, in which case
// the value `default-scheduler` IS gang-aware and a hard check would
// produce false positives). Instead, the workload gang reconciler emits a
// one-shot Warning event `MaybeNoGangScheduler` against the parent owner
// when a PodGroup is created for a pod whose effective SchedulerName is
// empty or `default-scheduler`; see
// `pkg/controller/v1beta1/workload/gang/gang.go`
// `maybeWarnNoGangScheduler` for the heuristic and dedup behavior.
package podgroup

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

var (
	// ErrPodGroupTerminating means the deterministic PodGroup name is still
	// occupied by an object whose deletion has begun. Callers must wait for an
	// authoritative NotFound before creating either a replacement PodGroup or
	// any member Pods that reference it.
	ErrPodGroupTerminating = errors.New("PodGroup is terminating")
	// ErrPodGroupOwnershipConflict means the deterministic PodGroup name is
	// occupied by an object controlled by another owner. OME must neither
	// update nor delete that object.
	ErrPodGroupOwnershipConflict = errors.New("PodGroup is controlled by another owner")
)

// IsMultiPodInstance reports whether the Instance has more than one pod
// across all Runners — i.e., it needs a PodGroup. Wraps
// InstancePlan.TotalPods with the multi-pod threshold (>=2) so callers
// don't repeat the constant.
func IsMultiPodInstance(inst workload.InstancePlan) bool {
	return inst.TotalPods() > 1
}

// BuildPodGroup produces the desired PodGroup for one multi-pod
// Instance. Caller MUST check IsMultiPodInstance(inst) first; passing
// a single-pod Instance returns an error so a stray call can't
// silently emit a 1-member PodGroup (functionally wrong: "schedule
// this one pod whenever, but block other members" with no other
// members).
//
// Spec.MinMember = TotalPods(inst) — every leader + worker pod must be
// schedulable for the gang to land. Spec.ScheduleTimeoutSeconds comes from
// InstanceReadyTimeout, bounded by plan.GangScheduleTimeout when the
// operator configured one. When plan.TopologyKey is set, the PodGroup
// advertises that same configured key through
// query.AnnotationTopologyKey.
//
// Pure compute, no I/O.
//
// ownerName is the workload owner name (Key.OwnerName) — the SAME base the pod
// renderer uses for each pod's pod-group label (render.go: isvcName =
// key.OwnerName). It is deliberately NOT owner.GetName(): on the IR-managed
// path the owner object is the InferenceReplica ("<isvc>-<component>"), whose
// name would double the component (e.g. PodGroup "svc-engine-engine-0") and
// never match the pods' "svc-engine-0" label → "PodGroup not found". owner is
// still used for the OwnerReference (GC) and namespace.
func BuildPodGroup(owner client.Object, ownerGVK schema.GroupVersionKind, ownerName string, plan workload.ComponentPlan, inst workload.InstancePlan) (*schedulingv1alpha1.PodGroup, error) {
	if owner == nil {
		return nil, fmt.Errorf("BuildPodGroup: nil owner")
	}
	if !IsMultiPodInstance(inst) {
		return nil, fmt.Errorf("BuildPodGroup: instance %d is single-pod (no gang)", inst.Index)
	}

	minMember := inst.TotalPods()
	timeout, bounded := scheduleTimeoutSeconds(plan)

	labels := podGroupLabels(ownerName, plan.Component, inst.Index)
	var annotations map[string]string
	if topologyKey := plan.TopologyKeyForInstance(inst.Index); topologyKey != "" {
		annotations = map[string]string{query.AnnotationTopologyKey: topologyKey}
	}

	spec := schedulingv1alpha1.PodGroupSpec{MinMember: minMember}
	if bounded {
		spec.ScheduleTimeoutSeconds = &timeout
	}

	return &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        query.PodGroupName(ownerName, plan.Component, inst.Index),
			Namespace:   owner.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(owner, ownerGVK),
			},
		},
		Spec: spec,
	}, nil
}

// scheduleTimeoutSeconds derives the gang's ScheduleTimeoutSeconds from
// the Component's InstanceReadyTimeout and bounds it with the operator's
// clamp. ok=false means the field stays unset, which leaves the timeout
// to the scheduler's own default: that is what an unconfigured clamp
// gives a Component whose InstanceReadyTimeout is unusable as a gang
// deadline, since there is no in-code window to fall back on.
func scheduleTimeoutSeconds(plan workload.ComponentPlan) (seconds int32, ok bool) {
	derived := int32(plan.InstanceReadyTimeout.Seconds())
	clamp := plan.GangScheduleTimeout
	if clamp == nil {
		return derived, derived > 0
	}
	minSeconds := int32(clamp.Min.Seconds())
	maxSeconds := int32(clamp.Max.Seconds())
	if derived <= 0 || derived < minSeconds {
		return minSeconds, true
	}
	if derived > maxSeconds {
		return maxSeconds, true
	}
	return derived, true
}

// podGroupLabels are the Component-scoped labels stamped on every
// PodGroup. Matches the headless Service selector trio so an operator
// looking at the parent Service can pivot to the gang via the same keys.
func podGroupLabels(isvcName string, component workload.ComponentType, instanceIdx int32) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelInstanceIdx:                fmt.Sprintf("%d", instanceIdx),
	}
}

// EnsurePodGroup creates (or no-op-reconciles) the PodGroup for one
// multi-pod Instance. Idempotent. No-op for single-pod Instances.
//
// The PodGroup MUST exist before any of its Instance's pods are
// created — otherwise the coscheduler sees pods referencing an
// unknown PodGroup and falls back to scheduling them individually
// (defeating the gang). Callers wire this before the per-Instance
// Create / Restart op.
func EnsurePodGroup(ctx context.Context, c client.Client, owner client.Object, ownerGVK schema.GroupVersionKind, ownerName string, plan workload.ComponentPlan, inst workload.InstancePlan) error {
	_, err := EnsurePodGroupForPods(ctx, c, owner, ownerGVK, ownerName, plan, inst, nil)
	return err
}

// EnsurePodGroupForPods is EnsurePodGroup with the currently-live pods that
// reference this PodGroup. Pod affinity is immutable, so while those pods are
// present the PodGroup topology annotation must describe their rendered
// topology rather than a newer Component spec. This prevents a topology-only
// edit or rollback from changing the scheduler contract underneath an
// in-flight gang. Once the group is empty, desired topology may advance before
// replacement pods are created.
func EnsurePodGroupForPods(ctx context.Context, c client.Client, owner client.Object, ownerGVK schema.GroupVersionKind, ownerName string, plan workload.ComponentPlan, inst workload.InstancePlan, pods []*corev1.Pod) (string, error) {
	return EnsurePodGroupForPodsWithReader(ctx, c, c, owner, ownerGVK, ownerName, plan, inst, pods)
}

// EnsurePodGroupForPodsWithReader is EnsurePodGroupForPods with a separate
// reader for topology-safety observations. Controllers should pass their live
// APIReader here: cached pod or PodGroup state must not advance the topology
// contract while immutable pods from the previous revision still exist.
func EnsurePodGroupForPodsWithReader(ctx context.Context, c client.Client, reader client.Reader, owner client.Object, ownerGVK schema.GroupVersionKind, ownerName string, plan workload.ComponentPlan, inst workload.InstancePlan, pods []*corev1.Pod) (string, error) {
	if c == nil {
		return "", fmt.Errorf("EnsurePodGroup: nil client")
	}
	if reader == nil {
		reader = c
	}
	if !IsMultiPodInstance(inst) {
		return "", nil
	}
	desired, err := BuildPodGroup(owner, ownerGVK, ownerName, plan, inst)
	if err != nil {
		return "", fmt.Errorf("EnsurePodGroup: build desired: %w", err)
	}

	// Steady-state fast path: the PodGroup spec is a pure function of
	// (owner, component, index, MinMember, topology contract) and changes only
	// when one of those desired fields changes. Read the live copy first and skip
	// the write path when the reconciled fields already match. At ~O(gangs) per
	// reconcile across thousands of pods this trims the per-gang work to a single
	// read on the common no-drift path. Cache miss or any drift falls through
	// to a direct create/update below using the live-read object and resource
	// version, so a stale cached Get cannot suppress a topology correction.
	existing := &schedulingv1alpha1.PodGroup{}
	getErr := reader.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		// Surface the read failure directly: falling through would run the
		// topology proof without the existing annotation and misreport a
		// transient Get error as an unprovable-topology error.
		return "", fmt.Errorf("EnsurePodGroup: get %s/%s: %w", desired.Namespace, desired.Name, getErr)
	}
	key, _, err := EnsurePodGroupForPodsFromObservation(ctx, c, owner, ownerGVK, ownerName,
		plan, inst, pods, existing, getErr == nil)
	return key, err
}

// EnsurePodGroupForPodsFromObservation reconciles one PodGroup from an
// authoritative inventory entry supplied by the caller. It performs no GET;
// this is the one-LIST path used when a Component has many gangs.
//
// A foreign same-named object is a collision, never an adoption candidate. A
// terminating owned object is also not success: its name cannot safely be
// reused until an authoritative inventory reports it absent. The reconciled
// object is returned so a caller can overlay same-pass writes on that inventory.
func EnsurePodGroupForPodsFromObservation(
	ctx context.Context,
	c client.Client,
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	ownerName string,
	plan workload.ComponentPlan,
	inst workload.InstancePlan,
	pods []*corev1.Pod,
	existing *schedulingv1alpha1.PodGroup,
	found bool,
) (string, *schedulingv1alpha1.PodGroup, error) {
	if c == nil {
		return "", nil, fmt.Errorf("EnsurePodGroup: nil client")
	}
	if !IsMultiPodInstance(inst) {
		return "", nil, nil
	}
	desired, err := BuildPodGroup(owner, ownerGVK, ownerName, plan, inst)
	if err != nil {
		return "", nil, fmt.Errorf("EnsurePodGroup: build desired: %w", err)
	}
	if found {
		if existing == nil {
			return "", nil, fmt.Errorf("EnsurePodGroup: observed %s/%s as present with a nil object", desired.Namespace, desired.Name)
		}
		if !ControlledByUID(existing, owner.GetUID()) {
			return "", nil, fmt.Errorf("%w: %s/%s", ErrPodGroupOwnershipConflict, existing.Namespace, existing.Name)
		}
		if existing.DeletionTimestamp != nil {
			return "", nil, fmt.Errorf("%w: %s/%s", ErrPodGroupTerminating, existing.Namespace, existing.Name)
		}
	}
	if len(pods) > 0 {
		if err := reconcileDesiredTopologyWithLivePods(desired, existing, found, ownerName, plan.Component, inst.Index, pods); err != nil {
			return "", nil, fmt.Errorf("EnsurePodGroup: reconcile live topology: %w", err)
		}
	}
	if found {
		if podGroupMatches(existing, desired) {
			return desired.Annotations[query.AnnotationTopologyKey], existing, nil
		}
		target := existing.DeepCopy()
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		reconcileTopologyKeyAnnotation(target, desired)
		target.Spec.MinMember = desired.Spec.MinMember
		target.Spec.ScheduleTimeoutSeconds = desired.Spec.ScheduleTimeoutSeconds
		if err := c.Update(ctx, target); err != nil {
			return "", nil, fmt.Errorf("EnsurePodGroup: update %s/%s: %w", target.Namespace, target.Name, err)
		}
		return desired.Annotations[query.AnnotationTopologyKey], target, nil
	}
	if err := c.Create(ctx, desired); err != nil {
		return "", nil, fmt.Errorf("EnsurePodGroup: create %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return desired.Annotations[query.AnnotationTopologyKey], desired, nil
}

// ControlledByUID reports whether obj's controller OwnerReference names the
// supplied UID. UIDs, rather than names, keep delete-and-recreate collisions
// from transferring ownership to a new object with the same name.
func ControlledByUID(obj metav1.Object, ownerUID types.UID) bool {
	if obj == nil || ownerUID == "" {
		return false
	}
	ref := metav1.GetControllerOfNoCopy(obj)
	return ref != nil && ref.UID == ownerUID
}

// EffectiveTopologyKeyForPods returns the topology contract that is safe for
// an Instance when no PodGroup exists to retain prior controller state. Empty
// pods use the current desired key. Once any immutable pod is live, only an
// exact controller-generated worker-to-leader term proves the active key. If
// desired topology is nonempty and no key can be proven, return an error so
// callers hold pod creation instead of silently rendering a split gang.
func EffectiveTopologyKeyForPods(desiredKey, ownerName string, component workload.ComponentType, instanceIdx int32, pods []*corev1.Pod) (string, error) {
	if len(pods) == 0 {
		return desiredKey, nil
	}
	key, ok, err := topologyKeyFromLivePods(ownerName, component, instanceIdx, pods)
	if err != nil {
		return "", err
	}
	if ok {
		return key, nil
	}
	if desiredKey != "" {
		return "", fmt.Errorf("cannot prove active topology for %s instance %d from %d live pod(s)", component, instanceIdx, len(pods))
	}
	return "", nil
}

// GeneratedTopologyKeyFromPods extracts the single topology key used by OME's
// generated worker-to-leader affinity across the supplied pods. Each accepted
// term must select the worker pod's own Instance leader. Hand-written affinity
// is deliberately ignored because it may describe an unrelated relationship.
func GeneratedTopologyKeyFromPods(ownerName string, component workload.ComponentType, pods []*corev1.Pod) (string, bool, error) {
	keys := make(map[string]struct{})
	for _, pod := range pods {
		if pod == nil || pod.Labels[query.LabelRunner] != workload.RunnerWorker ||
			pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
			continue
		}
		wantIndex := pod.Labels[query.LabelInstanceIdx]
		if wantIndex == "" {
			continue
		}
		for i := range pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			term := &pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i]
			if term.TopologyKey == "" || term.LabelSelector == nil {
				continue
			}
			labels := term.LabelSelector.MatchLabels
			if labels[constants.InferenceServicePodLabelKey] == ownerName &&
				labels[constants.OMEComponentLabel] == string(component) &&
				labels[query.LabelInstanceIdx] == wantIndex &&
				labels[query.LabelRunner] == workload.RunnerLeader {
				keys[term.TopologyKey] = struct{}{}
			}
		}
	}
	return uniqueTopologyKey(keys)
}

// reconcileDesiredTopologyWithLivePods aligns desired metadata with immutable
// affinity already carried by the group. Only an exact OME-generated
// worker-to-leader term or the existing controller-owned PodGroup annotation is
// trusted. Arbitrary user affinity may express a different relationship and
// must never be promoted into the gang scheduler's topology contract.
func reconcileDesiredTopologyWithLivePods(desired, existing *schedulingv1alpha1.PodGroup, existingFound bool, ownerName string, component workload.ComponentType, instanceIdx int32, pods []*corev1.Pod) error {
	key, ok, err := topologyKeyFromLivePods(ownerName, component, instanceIdx, pods)
	if err != nil {
		return err
	}
	if ok {
		setTopologyKeyAnnotation(desired, key, true)
		return nil
	}
	if existingFound {
		value, found := existing.Annotations[query.AnnotationTopologyKey]
		if found && value != "" {
			setTopologyKeyAnnotation(desired, value, true)
			return nil
		}
	}
	if desired.Annotations[query.AnnotationTopologyKey] != "" {
		return fmt.Errorf("cannot prove active topology for %s instance %d from %d live pod(s) and no PodGroup annotation", component, instanceIdx, len(pods))
	}
	// Topology was intentionally left unset. Preserve that non-topology behavior
	// even when live pods carry unrelated user affinity.
	setTopologyKeyAnnotation(desired, "", false)
	return nil
}

func setTopologyKeyAnnotation(pg *schedulingv1alpha1.PodGroup, value string, present bool) {
	if !present {
		delete(pg.Annotations, query.AnnotationTopologyKey)
		if len(pg.Annotations) == 0 {
			pg.Annotations = nil
		}
		return
	}
	if pg.Annotations == nil {
		pg.Annotations = make(map[string]string, 1)
	}
	pg.Annotations[query.AnnotationTopologyKey] = value
}

func topologyKeyFromLivePods(ownerName string, component workload.ComponentType, instanceIdx int32, pods []*corev1.Pod) (string, bool, error) {
	exact := make(map[string]struct{})
	wantIndex := fmt.Sprintf("%d", instanceIdx)
	for _, pod := range pods {
		if pod == nil || pod.Labels[query.LabelRunner] != workload.RunnerWorker ||
			pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
			continue
		}
		for i := range pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			term := &pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i]
			if term.TopologyKey == "" {
				continue
			}
			labels := map[string]string(nil)
			if term.LabelSelector != nil {
				labels = term.LabelSelector.MatchLabels
			}
			if labels[constants.InferenceServicePodLabelKey] == ownerName &&
				labels[constants.OMEComponentLabel] == string(component) &&
				labels[query.LabelInstanceIdx] == wantIndex &&
				labels[query.LabelRunner] == workload.RunnerLeader {
				exact[term.TopologyKey] = struct{}{}
			}
		}
	}
	return uniqueTopologyKey(exact)
}

func uniqueTopologyKey(keys map[string]struct{}) (string, bool, error) {
	switch len(keys) {
	case 0:
		return "", false, nil
	case 1:
		for key := range keys {
			return key, true, nil
		}
	default:
		return "", false, fmt.Errorf("conflicting live topology keys: %v", sortedTopologyKeys(keys))
	}
	panic("unreachable")
}

func sortedTopologyKeys(keys map[string]struct{}) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// podGroupMatches reports whether the cached PodGroup already carries
// the exact fields the EnsurePodGroup mutate closure reconciles —
// Labels, OwnerReferences, the controller-owned topology-key annotation,
// Spec.MinMember, and Spec.ScheduleTimeoutSeconds. Unrelated annotations are
// deliberately ignored and preserved.
// Returning true lets EnsurePodGroup skip the write on the
// no-drift steady-state path. Must stay in lockstep with the mutate
// closure: any field that closure writes must be compared here, or a
// drift on it would be silently dropped.
func podGroupMatches(existing, desired *schedulingv1alpha1.PodGroup) bool {
	if existing.Spec.MinMember != desired.Spec.MinMember {
		return false
	}
	if !equality.Semantic.DeepEqual(existing.Spec.ScheduleTimeoutSeconds, desired.Spec.ScheduleTimeoutSeconds) {
		return false
	}
	if !equality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return false
	}
	if !topologyKeyAnnotationMatches(existing, desired) {
		return false
	}
	return equality.Semantic.DeepEqual(existing.OwnerReferences, desired.OwnerReferences)
}

// reconcileTopologyKeyAnnotation owns only query.AnnotationTopologyKey. Other
// annotations may be written by users or admission and must survive updates.
func reconcileTopologyKeyAnnotation(target, desired *schedulingv1alpha1.PodGroup) {
	value, want := desired.Annotations[query.AnnotationTopologyKey]
	setTopologyKeyAnnotation(target, value, want)
}

func topologyKeyAnnotationMatches(existing, desired *schedulingv1alpha1.PodGroup) bool {
	existingValue, existingHas := existing.Annotations[query.AnnotationTopologyKey]
	desiredValue, desiredHas := desired.Annotations[query.AnnotationTopologyKey]
	return existingHas == desiredHas && existingValue == desiredValue
}

// FinalizeObservedPodGroup deletes an observed PodGroup with a UID precondition.
// Complete means an authoritative operation proved the object absent.
func FinalizeObservedPodGroup(ctx context.Context, c client.Client, ownerUID types.UID, pg *schedulingv1alpha1.PodGroup) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("DeletePodGroup: nil client")
	}
	if pg == nil {
		return true, nil
	}
	if !ControlledByUID(pg, ownerUID) {
		return false, fmt.Errorf("%w: %s/%s", ErrPodGroupOwnershipConflict, pg.Namespace, pg.Name)
	}
	if pg.DeletionTimestamp != nil {
		return false, nil
	}
	var opts []client.DeleteOption
	if pg.UID != "" {
		uid := pg.UID
		opts = append(opts, client.Preconditions{UID: &uid})
	}
	if err := c.Delete(ctx, pg.DeepCopy(), opts...); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
			return true, nil
		}
		return false, fmt.Errorf("DeletePodGroup: delete %s/%s: %w", pg.Namespace, pg.Name, err)
	}
	return false, nil
}

// DeleteObservedPodGroup requests deletion of an authoritative inventory
// object. Callers that own terminal status should use FinalizeObservedPodGroup
// and retain that status until complete is true.
func DeleteObservedPodGroup(ctx context.Context, c client.Client, ownerUID types.UID, pg *schedulingv1alpha1.PodGroup) error {
	_, err := FinalizeObservedPodGroup(ctx, c, ownerUID, pg)
	return err
}

// DeletePodGroup removes the PodGroup for an Instance that has been drained.
// Safe to call when the PodGroup doesn't exist. Inventory-backed callers
// should use DeleteObservedPodGroup to avoid this compatibility helper's GET.
//
// owner is abstract because the same primitive serves every OMENative owner
// shape. The controller UID is the authorization check; ownerName only derives
// the deterministic resource name.
func DeletePodGroup(ctx context.Context, c client.Client, owner client.Object, ownerName string, component workload.ComponentType, instanceIdx int32) error {
	if c == nil {
		return fmt.Errorf("DeletePodGroup: nil client")
	}
	if owner == nil {
		return fmt.Errorf("DeletePodGroup: nil owner")
	}
	key := client.ObjectKey{
		Name:      query.PodGroupName(ownerName, component, instanceIdx),
		Namespace: owner.GetNamespace(),
	}
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(ctx, key, pg); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
			return nil
		}
		return fmt.Errorf("DeletePodGroup: get %s: %w", key, err)
	}
	return DeleteObservedPodGroup(ctx, c, owner.GetUID(), pg)
}

// FinalizePodGroup requests deletion of an owned PodGroup and reports complete
// only after an authoritative GET or NotFound response proves it absent.
func FinalizePodGroup(ctx context.Context, c client.Client, reader client.Reader, owner client.Object, ownerName string, component workload.ComponentType, instanceIdx int32) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("DeletePodGroup: nil client")
	}
	if reader == nil {
		reader = c
	}
	if owner == nil {
		return false, fmt.Errorf("DeletePodGroup: nil owner")
	}
	key := client.ObjectKey{
		Name:      query.PodGroupName(ownerName, component, instanceIdx),
		Namespace: owner.GetNamespace(),
	}
	pg := &schedulingv1alpha1.PodGroup{}
	if err := reader.Get(ctx, key, pg); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
			return true, nil
		}
		return false, fmt.Errorf("DeletePodGroup: get %s: %w", key, err)
	}
	if !ControlledByUID(pg, owner.GetUID()) {
		return true, nil
	}
	return FinalizeObservedPodGroup(ctx, c, owner.GetUID(), pg)
}
