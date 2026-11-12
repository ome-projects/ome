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
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
// schedulable for the gang to land. Spec.ScheduleTimeoutSeconds is the
// placement grace (unschedulableGrace, the operator's
// lifecycle.unschedulableGracePeriod): the scheduler stops waiting for the
// gang in the same moment OME ends the attempt, so the two clocks never
// fire apart. plan.GangScheduleTimeout bounds it to the range the
// scheduler accepts when the operator configured one. When
// plan.TopologyKey is set, the PodGroup advertises that same configured
// key through query.AnnotationTopologyKey.
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
func BuildPodGroup(owner client.Object, ownerGVK schema.GroupVersionKind, ownerName string, plan workload.ComponentPlan, unschedulableGrace time.Duration, inst workload.InstancePlan) (*schedulingv1alpha1.PodGroup, error) {
	if owner == nil {
		return nil, fmt.Errorf("BuildPodGroup: nil owner")
	}
	if !IsMultiPodInstance(inst) {
		return nil, fmt.Errorf("BuildPodGroup: instance %d is single-pod (no gang)", inst.Index)
	}

	minMember := inst.TotalPods()
	timeout, bounded := scheduleTimeoutSeconds(unschedulableGrace, plan.GangScheduleTimeout)

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

// scheduleTimeoutSeconds is the gang's ScheduleTimeoutSeconds: the placement
// grace, raised to the minimum or lowered to the maximum of the operator's
// bounds when the grace falls outside what the scheduler accepts. ok=false
// means the field stays unset, which leaves the wait to the scheduler's own
// default: that is what a zero grace gives, since OME then has no clock on
// the placement either and there is no in-code window to fall back on.
func scheduleTimeoutSeconds(grace time.Duration, bounds *workload.GangScheduleTimeoutClamp) (seconds int32, ok bool) {
	if grace <= 0 {
		return 0, false
	}
	seconds = int32(grace.Seconds())
	if bounds == nil {
		return seconds, seconds > 0
	}
	minSeconds := int32(bounds.Min.Seconds())
	maxSeconds := int32(bounds.Max.Seconds())
	if seconds < minSeconds {
		return minSeconds, true
	}
	if seconds > maxSeconds {
		return maxSeconds, true
	}
	return seconds, true
}

// podGroupLabels are the Component-scoped labels written on every
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

// TopologyKeyOf is the topology key a group records; "" when it records
// none.
func TopologyKeyOf(group *schedulingv1alpha1.PodGroup) string {
	return group.Annotations[query.AnnotationTopologyKey]
}

// WriteNeeded reports whether WritePodGroup would send desired: its name is
// free, or the owned fields of the object at the name differ from it.
func WriteNeeded(desired, existing *schedulingv1alpha1.PodGroup, found bool) bool {
	return !found || drifted(desired, existing)
}

// GroupSettled reports whether the object at a gang's name is already the
// group DesiredPodGroup would build, read against the plan field by field
// without building it: the occupant is this owner's and not terminating,
// its owned fields carry the plan's values, and its topology key is the
// one its pods prove, or the one it records when they prove none. A
// settled group needs no write, and TopologyKeyOf reads the same key off
// it as off the built group. Any other object, one DesiredPodGroup refuses
// among them, reads as unsettled, and the build path then reports why. The
// pass asks it for every gang on every pass, so it allocates nothing.
func GroupSettled(
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	ownerName string,
	plan workload.ComponentPlan,
	unschedulableGrace time.Duration,
	inst workload.InstancePlan,
	pods []*corev1.Pod,
	existing *schedulingv1alpha1.PodGroup,
	found bool,
) bool {
	if owner == nil || !found || existing == nil || !IsMultiPodInstance(inst) ||
		!ControlledByUID(existing, owner.GetUID()) || existing.DeletionTimestamp != nil {
		return false
	}
	if existing.Spec.MinMember != inst.TotalPods() ||
		!timeoutSettled(existing.Spec.ScheduleTimeoutSeconds, unschedulableGrace, plan.GangScheduleTimeout) {
		return false
	}
	index := indexText(inst.Index)
	return labelsSettled(existing.Labels, ownerName, plan.Component, &index) &&
		controllerReferenceSettled(existing.OwnerReferences, owner, ownerGVK) &&
		topologySettled(existing, plan.TopologyKeyForInstance(inst.Index), ownerName, plan.Component, &index, pods)
}

// timeoutSettled reports whether a group's timeout is the one the grace
// and the bounds give: none when the grace is zero, else that value.
func timeoutSettled(got *int32, grace time.Duration, bounds *workload.GangScheduleTimeoutClamp) bool {
	want, bounded := scheduleTimeoutSeconds(grace, bounds)
	if !bounded || got == nil {
		return !bounded && got == nil
	}
	return *got == want
}

// instanceIndexText is the decimal text of an Instance index, as the
// labels of pods and groups spell it, held without allocating.
type instanceIndexText struct {
	buf [11]byte
	n   int
}

func indexText(index int32) instanceIndexText {
	var t instanceIndexText
	t.n = len(strconv.AppendInt(t.buf[:0], int64(index), 10))
	return t
}

// is reports whether s spells this index exactly: "01" is not "1".
func (t *instanceIndexText) is(s string) bool {
	return string(t.buf[:t.n]) == s
}

// podGroupLabelCount is how many labels podGroupLabels writes; a group
// carrying any other label is not settled.
var podGroupLabelCount = len(podGroupLabels("", "", 0))

// labelsSettled reports whether labels are exactly the ones podGroupLabels
// writes for the gang.
func labelsSettled(labels map[string]string, ownerName string, component workload.ComponentType, index *instanceIndexText) bool {
	if len(labels) != podGroupLabelCount {
		return false
	}
	got, ok := labels[query.LabelInstanceIdx]
	return ok && index.is(got) &&
		labelIs(labels, constants.InferenceServicePodLabelKey, ownerName) &&
		labelIs(labels, constants.OMEComponentLabel, string(component)) &&
		labelIs(labels, query.LabelManagedBy, query.ManagedByOMENative)
}

// labelIs reports whether labels carry key with exactly value.
func labelIs(labels map[string]string, key, value string) bool {
	got, ok := labels[key]
	return ok && got == value
}

// controllerReferenceSettled reports whether refs is the one controller
// reference BuildPodGroup writes, field by field.
func controllerReferenceSettled(refs []metav1.OwnerReference, owner client.Object, ownerGVK schema.GroupVersionKind) bool {
	if len(refs) != 1 {
		return false
	}
	ref := &refs[0]
	return apiVersionIs(ref.APIVersion, ownerGVK.GroupVersion()) &&
		ref.Kind == ownerGVK.Kind &&
		ref.Name == owner.GetName() &&
		ref.UID == owner.GetUID() &&
		flagIs(ref.Controller, true) &&
		flagIs(ref.BlockOwnerDeletion, true)
}

// apiVersionIs reports whether got spells gv, "group/version" or the bare
// version of the core group, without building the text.
func apiVersionIs(got string, gv schema.GroupVersion) bool {
	if gv.Group == "" {
		return got == gv.Version
	}
	group, version, slashed := strings.Cut(got, "/")
	return slashed && group == gv.Group && version == gv.Version
}

// flagIs reports whether an optional flag is present with value.
func flagIs(flag *bool, value bool) bool {
	return flag != nil && *flag == value
}

// topologySettled reports whether the key a group records is the one
// DesiredPodGroup would give it: the plan's when the gang has no pods;
// else the key the pods prove, the recorded key itself when they prove
// none and one is recorded, and no key when they prove none and the plan
// asks for none. Pods on two keys, and a plan key neither the pods nor
// the group can prove, settle nothing: the build path reports them.
func topologySettled(existing *schedulingv1alpha1.PodGroup, planKey, ownerName string, component workload.ComponentType, index *instanceIndexText, pods []*corev1.Pod) bool {
	recorded, set := existing.Annotations[query.AnnotationTopologyKey]
	if len(pods) == 0 {
		return set == (planKey != "") && recorded == planKey
	}
	keys := provenTopologyKey(ownerName, component, index, pods)
	switch {
	case keys.conflict():
		return false
	case keys.proven:
		return set && recorded == keys.key
	case set && recorded != "":
		return true
	case planKey != "":
		return false
	}
	return !set
}

// topologyKeys is what the generated worker-to-leader terms of some pods
// say about a gang's topology key: the one key they name, none, or two or
// more, which no PodGroup can describe. Naming keys that agree allocates
// nothing; the keys are collected, for the error that names every one of
// them, only once two disagree.
type topologyKeys struct {
	key    string
	proven bool
	// disputed is every distinct key named, kept from the first key that
	// differed from key on; nil while the terms agree.
	disputed []string
}

// name records one key a generated term carries.
func (k *topologyKeys) name(key string) {
	switch {
	case !k.proven:
		k.key, k.proven = key, true
	case k.disputed == nil && key == k.key:
	default:
		if k.disputed == nil {
			k.disputed = []string{k.key}
		}
		if !slices.Contains(k.disputed, key) {
			k.disputed = append(k.disputed, key)
		}
	}
}

// conflict reports whether the terms named two or more keys.
func (k *topologyKeys) conflict() bool {
	return k.disputed != nil
}

// result is the key the terms prove, none, or the error naming every key
// in dispute, sorted.
func (k *topologyKeys) result() (string, bool, error) {
	if k.conflict() {
		slices.Sort(k.disputed)
		return "", false, fmt.Errorf("conflicting live topology keys: %v", k.disputed)
	}
	return k.key, k.proven, nil
}

// generatedTerm reads one required affinity term of a worker as the term
// the renderer writes: the topology key it names and the index of the
// leader its selector names. ok is false for a term with no key, or one
// whose selector does not name a leader of the owner and the Component by
// equality, which a hand-written term never does.
func generatedTerm(term *corev1.PodAffinityTerm, ownerName string, component workload.ComponentType) (key, index string, ok bool) {
	if term.TopologyKey == "" {
		return "", "", false
	}
	index, ok = leaderIndexSelected(term.LabelSelector, ownerName, component)
	return term.TopologyKey, index, ok
}

// provenTopologyKey is what the pods of one Instance prove about its key:
// a term counts when it follows that Instance's leader, whatever index
// label the pod carries.
func provenTopologyKey(ownerName string, component workload.ComponentType, index *instanceIndexText, pods []*corev1.Pod) topologyKeys {
	var keys topologyKeys
	for _, pod := range pods {
		terms := workerAffinityTerms(pod)
		for i := range terms {
			if key, selected, ok := generatedTerm(&terms[i], ownerName, component); ok && index.is(selected) {
				keys.name(key)
			}
		}
	}
	return keys
}

// ownLeaderTopologyKey is what the pods of a whole Component prove: a term
// counts when it follows the leader of the Instance the pod's own index
// label names, and a pod with no index label counts for nothing.
func ownLeaderTopologyKey(ownerName string, component workload.ComponentType, pods []*corev1.Pod) topologyKeys {
	var keys topologyKeys
	for _, pod := range pods {
		terms := workerAffinityTerms(pod)
		if len(terms) == 0 {
			continue
		}
		own := pod.Labels[query.LabelInstanceIdx]
		if own == "" {
			continue
		}
		for i := range terms {
			if key, selected, ok := generatedTerm(&terms[i], ownerName, component); ok && selected == own {
				keys.name(key)
			}
		}
	}
	return keys
}

// DesiredPodGroup is the PodGroup one gang needs: the plan's group carrying
// the topology key its pods prove, or the key the existing group records
// when the pods prove none. An occupant this owner cannot write is refused
// before any topology is read. A caller that decides whether to write and
// then writes builds the group once and hands it to WriteNeeded and
// WritePodGroup.
func DesiredPodGroup(
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	ownerName string,
	plan workload.ComponentPlan,
	unschedulableGrace time.Duration,
	inst workload.InstancePlan,
	pods []*corev1.Pod,
	existing *schedulingv1alpha1.PodGroup,
	found bool,
) (*schedulingv1alpha1.PodGroup, error) {
	desired, err := BuildPodGroup(owner, ownerGVK, ownerName, plan, unschedulableGrace, inst)
	if err != nil {
		return nil, fmt.Errorf("EnsurePodGroup: build desired: %w", err)
	}
	if found {
		if err := writableOccupant(desired, existing, owner.GetUID()); err != nil {
			return nil, err
		}
	}
	if len(pods) > 0 {
		if err := reconcileDesiredTopologyWithLivePods(desired, existing, found, ownerName, plan.Component, inst.Index, pods); err != nil {
			return nil, fmt.Errorf("EnsurePodGroup: reconcile live topology: %w", err)
		}
	}
	return desired, nil
}

// writableOccupant checks the object found at the desired name: it must be
// this owner's and not on its way out. Another owner's object is a
// collision, never an adoption candidate; a terminating one keeps the name
// until an authoritative inventory reports it absent.
func writableOccupant(desired, existing *schedulingv1alpha1.PodGroup, ownerUID types.UID) error {
	if existing == nil {
		return fmt.Errorf("EnsurePodGroup: observed %s/%s as present with a nil object", desired.Namespace, desired.Name)
	}
	if !ControlledByUID(existing, ownerUID) {
		return fmt.Errorf("%w: %s/%s", ErrPodGroupOwnershipConflict, existing.Namespace, existing.Name)
	}
	if existing.DeletionTimestamp != nil {
		return fmt.Errorf("%w: %s/%s", ErrPodGroupTerminating, existing.Namespace, existing.Name)
	}
	return nil
}

// WritePodGroup creates desired when its name is free, else brings the
// owned fields of the object at the name to desired's values, writing
// only on drift. It returns the object as written, or the existing one
// when nothing drifted.
func WritePodGroup(ctx context.Context, c client.Client, desired, existing *schedulingv1alpha1.PodGroup, found bool) (*schedulingv1alpha1.PodGroup, error) {
	if !found {
		if err := c.Create(ctx, desired); err != nil {
			return nil, fmt.Errorf("EnsurePodGroup: create %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return desired, nil
	}
	if !drifted(desired, existing) {
		return existing, nil
	}
	target := existing.DeepCopy()
	ownedFieldsOf(desired).applyTo(target)
	if err := c.Update(ctx, target); err != nil {
		return nil, fmt.Errorf("EnsurePodGroup: update %s/%s: %w", target.Namespace, target.Name, err)
	}
	return target, nil
}

// drifted reports whether the owned fields of the object at the name differ
// from the desired group's.
func drifted(desired, existing *schedulingv1alpha1.PodGroup) bool {
	return !ownedFieldsOf(desired).equal(ownedFieldsOf(existing))
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

// IsNotFound reports whether err says there is no PodGroup to read: the
// object does not exist, or the cluster has no PodGroup API at all (the CRD
// is not installed, or the type is not in the scheme).
func IsNotFound(err error) bool {
	return apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err)
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
	key, ok, err := TopologyKeyProvenByPods(ownerName, component, instanceIdx, pods)
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

// GeneratedTopologyKeyFromPods is the one topology key the generated
// worker-to-leader affinity of the supplied pods follows: each term counted
// selects the leader of the worker's own Instance. Hand-written affinity is
// ignored, since it may describe an unrelated relationship. ok is false
// when no term counts; the error names the keys of a gang split across
// two or more.
func GeneratedTopologyKeyFromPods(ownerName string, component workload.ComponentType, pods []*corev1.Pod) (string, bool, error) {
	keys := ownLeaderTopologyKey(ownerName, component, pods)
	return keys.result()
}

// TopologyKeyProvenByPods is GeneratedTopologyKeyFromPods for the pods of one
// Instance: every term counted must select that Instance's leader, whatever
// index label the pod carries. ok is false when no worker among the pods
// carries the generated term; the error is two workers on different keys.
// The reading allocates nothing while the terms agree.
func TopologyKeyProvenByPods(ownerName string, component workload.ComponentType, instanceIdx int32, pods []*corev1.Pod) (string, bool, error) {
	index := indexText(instanceIdx)
	keys := provenTopologyKey(ownerName, component, &index, pods)
	return keys.result()
}

// workerAffinityTerms are the required pod-affinity terms of a worker pod.
// A leader, a pod without pod affinity and a nil pod have none: only a
// worker carries the generated term that names its leader.
func workerAffinityTerms(pod *corev1.Pod) []corev1.PodAffinityTerm {
	if pod == nil || pod.Labels[query.LabelRunner] != workload.RunnerWorker ||
		pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
		return nil
	}
	return pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

// leaderIndexSelected is the Instance index a term's selector names, "" for
// none, and whether the term otherwise selects a leader pod the way the
// renderer writes it: the owner, the Component and the leader runner, each
// by equality.
func leaderIndexSelected(selector *metav1.LabelSelector, ownerName string, component workload.ComponentType) (index string, ok bool) {
	if selector == nil {
		return "", false
	}
	labels := selector.MatchLabels
	return labels[query.LabelInstanceIdx], labels[constants.InferenceServicePodLabelKey] == ownerName &&
		labels[constants.OMEComponentLabel] == string(component) &&
		labels[query.LabelRunner] == workload.RunnerLeader
}

// reconcileDesiredTopologyWithLivePods aligns desired metadata with immutable
// affinity already carried by the group. Only an exact OME-generated
// worker-to-leader term or the existing controller-owned PodGroup annotation is
// trusted. Arbitrary user affinity may express a different relationship and
// must never be promoted into the gang scheduler's topology contract.
func reconcileDesiredTopologyWithLivePods(desired, existing *schedulingv1alpha1.PodGroup, existingFound bool, ownerName string, component workload.ComponentType, instanceIdx int32, pods []*corev1.Pod) error {
	key, ok, err := TopologyKeyProvenByPods(ownerName, component, instanceIdx, pods)
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

// ownedFields are the PodGroup fields this controller writes. Everything
// else on the object, other annotations above all, belongs to whoever put
// it there and survives an update.
type ownedFields struct {
	Labels                 map[string]string
	OwnerReferences        []metav1.OwnerReference
	TopologyKey            string
	TopologyKeySet         bool
	MinMember              int32
	ScheduleTimeoutSeconds *int32
}

// ownedFieldsOf reads the owned fields off a built or an observed PodGroup.
func ownedFieldsOf(pg *schedulingv1alpha1.PodGroup) ownedFields {
	key, set := pg.Annotations[query.AnnotationTopologyKey]
	return ownedFields{
		Labels:                 pg.Labels,
		OwnerReferences:        pg.OwnerReferences,
		TopologyKey:            key,
		TopologyKeySet:         set,
		MinMember:              pg.Spec.MinMember,
		ScheduleTimeoutSeconds: pg.Spec.ScheduleTimeoutSeconds,
	}
}

// equal reads the six owned fields one by one under the rule the
// apiserver's semantic compare applies: a nil label map or owner list
// equals an empty one, and a timeout or a flag set on one side alone is a
// difference. The owned fields hold no time and no quantity. The pass asks
// it for every gang on every pass, so it reads the fields directly rather
// than through a reflection walk that copies every leaf on the way.
func (f ownedFields) equal(other ownedFields) bool {
	return maps.Equal(f.Labels, other.Labels) &&
		ownerReferencesEqual(f.OwnerReferences, other.OwnerReferences) &&
		f.TopologyKey == other.TopologyKey &&
		f.TopologyKeySet == other.TopologyKeySet &&
		f.MinMember == other.MinMember &&
		int32PointersEqual(f.ScheduleTimeoutSeconds, other.ScheduleTimeoutSeconds)
}

// ownerReferencesEqual reads two owner reference lists in order; a nil
// list and an empty one are the same.
func ownerReferencesEqual(a, b []metav1.OwnerReference) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !ownerReferenceEqual(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

// ownerReferenceEqual reads every field of an owner reference. A field
// added to the reference type is read here too: the compare's contract
// test changes every leaf of the owned fields one at a time and fails on a
// field this function does not read.
func ownerReferenceEqual(a, b *metav1.OwnerReference) bool {
	return a.APIVersion == b.APIVersion &&
		a.Kind == b.Kind &&
		a.Name == b.Name &&
		a.UID == b.UID &&
		boolPointersEqual(a.Controller, b.Controller) &&
		boolPointersEqual(a.BlockOwnerDeletion, b.BlockOwnerDeletion)
}

// boolPointersEqual reads two optional flags; both absent is equal.
func boolPointersEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// int32PointersEqual reads two optional counts; both absent is equal.
func int32PointersEqual(a, b *int32) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// applyTo writes the owned fields onto pg and leaves the rest of the object
// as it is.
func (f ownedFields) applyTo(pg *schedulingv1alpha1.PodGroup) {
	pg.Labels = f.Labels
	pg.OwnerReferences = f.OwnerReferences
	setTopologyKeyAnnotation(pg, f.TopologyKey, f.TopologyKeySet)
	pg.Spec.MinMember = f.MinMember
	pg.Spec.ScheduleTimeoutSeconds = f.ScheduleTimeoutSeconds
}

// FinalizeObservedPodGroup deletes an observed PodGroup with a UID precondition.
// Complete means an authoritative operation proved the object absent.
func FinalizeObservedPodGroup(ctx context.Context, c client.Client, ownerUID types.UID, pg *schedulingv1alpha1.PodGroup) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("FinalizeObservedPodGroup: nil client")
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
		if IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("FinalizeObservedPodGroup: delete %s/%s: %w", pg.Namespace, pg.Name, err)
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

// FinalizePodGroup requests deletion of an owned PodGroup and reports complete
// only after an authoritative GET or NotFound response proves it absent.
func FinalizePodGroup(ctx context.Context, c client.Client, reader client.Reader, owner client.Object, ownerName string, component workload.ComponentType, instanceIdx int32) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("FinalizePodGroup: nil client")
	}
	if reader == nil {
		reader = c
	}
	if owner == nil {
		return false, fmt.Errorf("FinalizePodGroup: nil owner")
	}
	key := client.ObjectKey{
		Name:      query.PodGroupName(ownerName, component, instanceIdx),
		Namespace: owner.GetNamespace(),
	}
	pg := &schedulingv1alpha1.PodGroup{}
	if err := reader.Get(ctx, key, pg); err != nil {
		if IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("FinalizePodGroup: get %s: %w", key, err)
	}
	if !ControlledByUID(pg, owner.GetUID()) {
		return true, nil
	}
	return FinalizeObservedPodGroup(ctx, c, owner.GetUID(), pg)
}
