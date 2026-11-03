// Package gang owns the per-Instance PodGroup lifecycle for OMENative
// multi-pod Components. Sits one layer above the `workload/podgroup`
// primitives and orchestrates them from the workload reconciler's
// input model.
//
// Lives in its own subpackage (rather than workload root) so the
// import edges are gang → workload + gang → podgroup → workload,
// with no back-edge — would otherwise close a cycle through podgroup.
package gang

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ErrPodGroupTerminating is returned when a planned gang's deterministic
// PodGroup name is still occupied by an owned object being deleted. Callers
// should requeue without running Pod creation.
var ErrPodGroupTerminating = podgroup.ErrPodGroupTerminating

// PodGroupReconcileState carries the one authoritative inventory and the
// persisted lifecycle owners that suppress prerequisite creation. Callers may
// preload it once and share the same Inventory with finalization.
type PodGroupReconcileState struct {
	Inventory     *PodGroupInventory
	TerminalOwned map[int32]struct{}
}

// EnsureSurgePodGroupWithState builds the Deps.EnsureGangPodGroup callback
// the gang-surge op invokes to write a surge Instance's PodGroup just before
// its pods, whether or not the plan carries the surge index yet. It lives
// here so workload/ops needs no workload/podgroup import. When the input
// brings the Component's pods and state brings the owner's PodGroups, the
// callback performs no extra Pod LIST or PodGroup GET. A single-pod Instance
// or a cluster without the PodGroup API gets no PodGroup, only the topology
// key its live pods already carry. Idempotent.
func EnsureSurgePodGroupWithState(deps workload.Deps, state PodGroupReconcileState) workload.EnsureGangPodGroupFn {
	return func(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) (string, error) {
		if deps.Client == nil || !podgroup.IsMultiPodInstance(inst) {
			return plan.TopologyKeyForInstance(inst.Index), nil
		}
		reader := deps.Reader()
		name := query.PodGroupName(input.Key.OwnerName, plan.Component, inst.Index)
		var pods []*corev1.Pod
		if input.AuthoritativePods != nil {
			for _, pod := range input.AuthoritativePods.Pods {
				if pod != nil && pod.Labels[query.LabelPodGroup] == name {
					pods = append(pods, pod)
				}
			}
		} else {
			var err error
			pods, err = podsForPodGroup(ctx, reader, input.Key.Namespace, name)
			if err != nil {
				return "", fmt.Errorf("list surge PodGroup pods: %w", err)
			}
		}

		gangSchedulingAvailable := input.DesiredSpec.GangSchedulingAvailable
		inv := state.Inventory
		if gangSchedulingAvailable && inv != nil {
			if input.OwnerObject == nil || inv.OwnerUID() != input.OwnerObject.GetUID() {
				return "", fmt.Errorf("ensure surge PodGroup: inventory owner does not match input owner")
			}
			gangSchedulingAvailable = inv.Available()
		}
		if !gangSchedulingAvailable {
			return podgroup.EffectiveTopologyKeyForPods(
				plan.TopologyKeyForInstance(inst.Index), input.Key.OwnerName,
				plan.Component, inst.Index, pods)
		}
		if inv != nil {
			existing, found := inv.ByName(name)
			// Classify before writing, for the same reason the top-level
			// pass does: the surge index may not be pinned in the plan yet,
			// so this is the first place the row's PodGroup is read at all
			// and the only chance to record why its members are withheld.
			input.Gangs.Record(inst.Index,
				podgroup.ObserveGang(name, existing, found, input.OwnerObject.GetUID()))
			key, reconciled, err := podgroup.EnsurePodGroupForPodsFromObservation(ctx, deps.Client,
				input.OwnerObject, input.OwnerGVK, input.Key.OwnerName, plan, inst,
				pods, existing, found)
			if err == nil {
				inv.recordReconciled(reconciled)
				return key, nil
			}
			return "", unusableNameError(err)
		}
		return podgroup.EnsurePodGroupForPods(ctx, deps.Client, reader, input.OwnerObject, input.OwnerGVK, input.Key.OwnerName, plan, inst, pods)
	}
}

// EnsurePodGroupsWithState drives the per-Instance PodGroup lifecycle for
// one Component reconcile and returns the effective topology key of every
// planned gang. Adapters call it before the per-Instance create pass, so the
// gang is announced to the scheduler before its first pod exists; a gang
// announced later would be placed pod by pod. A single-pod Instance gets no
// PodGroup. When the plan has a gang and the cluster lacks the PodGroup API,
// nothing is written and the GangSchedulingUnavailable condition is True:
// pods are still created, and the condition is the operator's signal that
// gang placement may race. An empty topology key is valid only when topology
// was left unset; a configured key that cannot be proven from a gang's live
// pods or its PodGroup fails the pass before any missing member is created.
// It performs no eager deletion: terminal lifecycle owners delete their own
// PodGroups after proving their Pod bucket empty.
//
// The pass runs in one fixed order: read the inventory, write the
// availability condition, group the live pods by PodGroup, then for every
// planned gang record what its PodGroup says and ensure or reset it, and
// last the scheduler-name warning. The condition is written before any pod
// read or PodGroup write, so a later failure cannot hide whether gang
// scheduling is usable; the warning runs after the writes, so a PodGroup
// write failure takes precedence over it.
func EnsurePodGroupsWithState(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, state PodGroupReconcileState) (map[int32]string, error) {
	if deps.Client == nil {
		return nil, fmt.Errorf("EnsurePodGroups: nil client")
	}
	if input.OwnerObject == nil {
		return nil, fmt.Errorf("EnsurePodGroups: nil owner")
	}
	pass := newPodGroupPass(deps, &input, plan, state.Inventory)
	if len(pass.gangs) == 0 {
		// A single-pod Instance needs no PodGroup, whatever the cluster offers.
		if err := pass.writeAvailability(ctx, input.DesiredSpec.GangSchedulingAvailable); err != nil {
			return nil, err
		}
		return pass.topology, nil
	}
	available, err := pass.readInventory(ctx)
	if err != nil {
		return nil, err
	}
	if err := pass.writeAvailability(ctx, available); err != nil {
		return nil, err
	}
	if err := pass.groupLivePods(ctx); err != nil {
		return nil, err
	}
	if !available {
		return pass.topologyFromLivePods()
	}
	pass.terminalOwned = terminalOwnedIndices(input.ObservedState.InstanceStatuses, state.TerminalOwned)
	for _, inst := range pass.gangs {
		if err := pass.ensureGang(ctx, inst); err != nil {
			return nil, err
		}
	}
	if err := pass.warnWhenDefaultScheduler(ctx); err != nil {
		return nil, err
	}
	return pass.topology, nil
}

// podGroupPass is one Component's PodGroup pass: the gangs the plan asks
// for, the owner's PodGroups read once, the Component's live pods keyed by
// PodGroup name, and what the pass records per Instance.
type podGroupPass struct {
	deps  workload.Deps
	input *workload.ReconcileInput
	plan  workload.ComponentPlan
	// gangs are the plan's multi-pod Instances, in plan order.
	gangs []workload.InstancePlan
	// inventory is the owner's PodGroups: the caller's when it brought one,
	// else read once by readInventory.
	inventory *PodGroupInventory
	// podsByGroup holds the Component's live pods under the PodGroup name
	// each pod carries.
	podsByGroup map[string][]*corev1.Pod
	// terminalOwned lists the Instances whose teardown owns the PodGroup.
	terminalOwned map[int32]struct{}
	// topology is the effective topology key of every gang the pass settled.
	topology map[int32]string
	// warningRow is the first ensured Instance that has a row. The
	// scheduler-name warning is recorded on it, because the record lives on
	// a row and the create pass may not have opened one for every index.
	warningRow      int32
	warningRowFound bool
}

func newPodGroupPass(deps workload.Deps, input *workload.ReconcileInput, plan workload.ComponentPlan, inventory *PodGroupInventory) *podGroupPass {
	return &podGroupPass{
		deps:      deps,
		input:     input,
		plan:      plan,
		gangs:     plannedGangs(plan),
		inventory: inventory,
		topology:  make(map[int32]string),
	}
}

// plannedGangs lists the plan's multi-pod Instances in plan order. A
// single-pod Instance has nothing to gang.
func plannedGangs(plan workload.ComponentPlan) []workload.InstancePlan {
	var gangs []workload.InstancePlan
	for _, inst := range plan.Instances {
		if podgroup.IsMultiPodInstance(inst) {
			gangs = append(gangs, inst)
		}
	}
	return gangs
}

// groupName is the deterministic PodGroup name of one Instance.
func (p *podGroupPass) groupName(idx int32) string {
	return query.PodGroupName(p.input.Key.OwnerName, p.plan.Component, idx)
}

// readInventory settles whether gang scheduling is usable this pass: the
// spec must advertise the PodGroup API, and the owner's inventory, read
// here when the caller brought none, must have found it. An inventory
// built for another owner is a wiring error, not a reading.
func (p *podGroupPass) readInventory(ctx context.Context) (bool, error) {
	if !p.input.DesiredSpec.GangSchedulingAvailable {
		return false, nil
	}
	if p.inventory == nil {
		inventory, err := ObservePodGroups(ctx, p.deps.Reader(), p.input.OwnerObject)
		if err != nil {
			return false, fmt.Errorf("EnsurePodGroups: observe PodGroups: %w", err)
		}
		p.inventory = inventory
	}
	if p.inventory.OwnerUID() != p.input.OwnerObject.GetUID() {
		return false, fmt.Errorf("EnsurePodGroups: inventory owner UID %q does not match owner UID %q", p.inventory.OwnerUID(), p.input.OwnerObject.GetUID())
	}
	return p.inventory.Available(), nil
}

// writeAvailability writes the GangSchedulingUnavailable condition: True
// only when the plan has a gang and the cluster cannot enforce it.
func (p *podGroupPass) writeAvailability(ctx context.Context, available bool) error {
	if err := patchGangSchedulingCondition(ctx, *p.input, available, len(p.gangs) > 0); err != nil {
		return fmt.Errorf("EnsurePodGroups: patch GangSchedulingUnavailable condition: %w", err)
	}
	return nil
}

// groupLivePods reads the Component's pods, the adapter's live list when
// it brought one, and keys them by the PodGroup name each pod carries.
func (p *podGroupPass) groupLivePods(ctx context.Context) error {
	var pods []*corev1.Pod
	if p.input.AuthoritativePods != nil {
		pods = p.input.AuthoritativePods.Pods
	} else {
		var err error
		pods, err = query.ListOMENativePodsByName(ctx, p.deps.Reader(), p.input.Key.Namespace,
			p.input.Key.OwnerName, p.plan.Component, false)
		if err != nil {
			return fmt.Errorf("EnsurePodGroups: list component pods: %w", err)
		}
	}
	p.podsByGroup = make(map[string][]*corev1.Pod)
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		name := pod.Labels[query.LabelPodGroup]
		if name == "" {
			continue
		}
		p.podsByGroup[name] = append(p.podsByGroup[name], pod)
	}
	return nil
}

// topologyFromLivePods is the pass when the cluster cannot enforce gangs:
// no PodGroup is written, and each gang keeps the topology its live pods
// already carry, so a missing member is not rebuilt under a newer key only
// because the optional PodGroup API is absent.
func (p *podGroupPass) topologyFromLivePods() (map[int32]string, error) {
	for _, inst := range p.gangs {
		key, err := podgroup.EffectiveTopologyKeyForPods(
			p.plan.TopologyKeyForInstance(inst.Index), p.input.Key.OwnerName, p.plan.Component,
			inst.Index, p.podsByGroup[p.groupName(inst.Index)])
		if err != nil {
			return nil, fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
		}
		p.topology[inst.Index] = key
	}
	return p.topology, nil
}

// ensureGang records what one planned gang's PodGroup says, then resets a
// group the scheduler failed or writes the group the plan asks for. A row
// the teardown owns keeps its group. A name this owner cannot write,
// because the object at it is terminating or another controller holds it,
// is a fact about one Instance, not about the Component: the pass goes on
// to the other gangs, the row's members stay withheld, the escalation pass
// reads the recorded state, and no topology is published for the index (an
// absent entry reads as the Component's desired key, which a fresh gang at
// that name would get).
func (p *podGroupPass) ensureGang(ctx context.Context, inst workload.InstancePlan) error {
	if _, terminal := p.terminalOwned[inst.Index]; terminal {
		return nil
	}
	name := p.groupName(inst.Index)
	existing, found := p.inventory.ByName(name)
	obs := podgroup.ObserveGang(name, existing, found, p.input.OwnerObject.GetUID())
	p.input.Gangs.Record(inst.Index, obs)
	if obs.State == workload.GangStateFailed && resettableIndex(p.input.ObservedState, inst.Index) {
		return p.resetFailedGroup(ctx, inst.Index, obs)
	}
	key, reconciled, err := podgroup.EnsurePodGroupForPodsFromObservation(ctx, p.deps.Client,
		p.input.OwnerObject, p.input.OwnerGVK, p.input.Key.OwnerName, p.plan, inst,
		p.podsByGroup[name], existing, found)
	if err != nil {
		if errors.Is(err, podgroup.ErrPodGroupTerminating) ||
			errors.Is(err, podgroup.ErrPodGroupOwnershipConflict) {
			return nil
		}
		return fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
	}
	p.recordEnsured(inst.Index, reconciled, key)
	return nil
}

// resetFailedGroup deletes a group the gang scheduler failed, so the next
// pass builds a fresh one. The scheduler's Failed phase is final for the
// object: its controller stops reconciling it, and this pass reconciles
// only labels, ownership and size, so the group can never be admitted
// again under that name. The row is untouched: a failure that resets
// itself must not end an attempt, or the reset and the rebuild would chase
// each other through Failed forever. A member the kubelet has finished
// with holds the reset back: the group's phase comes from its members, so
// a group rebuilt around that pod would fail the same way; the create pass
// replaces the pod first and the reset lands the pass after.
func (p *podGroupPass) resetFailedGroup(ctx context.Context, idx int32, obs workload.GangObservation) error {
	if anyTerminalMember(p.podsByGroup[obs.Name]) {
		return nil
	}
	if err := p.inventory.DeleteOwnedName(ctx, p.deps.Client, obs.Name); err != nil {
		return fmt.Errorf("EnsurePodGroups: reset instance %d: %w", idx, err)
	}
	return warnPodGroupReset(ctx, p.deps, *p.input, p.plan.Component, idx, obs)
}

// recordEnsured overlays the written group on the inventory, so the inline
// surge callback sees it without a second read, publishes the gang's
// topology key, and keeps the first ensured Instance that has a row as the
// place for the scheduler-name warning.
func (p *podGroupPass) recordEnsured(idx int32, reconciled *schedulingv1alpha1.PodGroup, key string) {
	p.inventory.recordReconciled(reconciled)
	p.topology[idx] = key
	if !p.warningRowFound && p.input.ObservedState.Instance(idx) != nil {
		p.warningRow = idx
		p.warningRowFound = true
	}
}

// warnWhenDefaultScheduler runs the scheduler-name heuristic once the
// groups are written, so a PodGroup write failure takes precedence, and
// only when an ensured Instance has a row to record the warning on.
func (p *podGroupPass) warnWhenDefaultScheduler(ctx context.Context) error {
	if !p.warningRowFound {
		return nil
	}
	return maybeWarnNoGangScheduler(ctx, p.deps, *p.input, p.plan.Component, p.warningRow,
		p.input.DesiredSpec.PodSpec, p.input.DesiredSpec.WorkerPodSpec)
}

// unusableNameError re-spells the two reasons a deterministic name
// cannot be written as the workload-level sentinel, so the op that
// invoked this callback can defer to the escalation pass without
// importing the optional gang-scheduling API. Anything else is a real
// failure and travels unchanged.
func unusableNameError(err error) error {
	if errors.Is(err, podgroup.ErrPodGroupTerminating) ||
		errors.Is(err, podgroup.ErrPodGroupOwnershipConflict) {
		return fmt.Errorf("%w: %w", workload.ErrGangNameUnusable, err)
	}
	return err
}

// warnPodGroupReset announces the one rebuild an operator would
// otherwise have no trace of: the Instance is never failed for a failed
// group, so this event is where the gang scheduler's own explanation
// survives. Announced once per episode on the Instance's own row.
func warnPodGroupReset(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, component workload.ComponentType, idx int32, obs workload.GangObservation) error {
	target := workload.EventTarget(input)
	if deps.Recorder == nil || target == nil {
		return nil
	}
	announced, err := status.Announce(ctx, input, idx, workload.EventReasonPodGroupReset)
	if err != nil || !announced {
		return err
	}
	deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(workload.EventReasonPodGroupReset),
		"OMENative component=%s: deleting PodGroup %s so the gang can be admitted again (%s)",
		component, obs.Name, obs.Message)
	return nil
}

// resettableIndex reports whether this owner may rebuild the group at
// idx. A teardown belongs to DeleteBatch and a migration to its record;
// deleting the group under either would pull the gang contract out from
// under pods the other machine is still driving.
func resettableIndex(observed workload.WorkloadObservedState, idx int32) bool {
	row := observed.Instance(idx)
	if row == nil {
		return true
	}
	switch workload.OwnerOfOperation(row.Operation) {
	case workload.OwnerDelete, workload.OwnerMigrate:
		return false
	}
	return true
}

// anyTerminalMember reports whether the group still holds a member the
// kubelet has finished with. Such a pod occupies its stable name until
// the create pass recycles it, and the gang controller derives the
// group's phase from exactly these.
func anyTerminalMember(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod != nil && pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodFailed {
			return true
		}
	}
	return false
}

func terminalOwnedIndices(observed []workload.InstanceStatus, explicit map[int32]struct{}) map[int32]struct{} {
	out := make(map[int32]struct{}, len(explicit))
	for idx := range explicit {
		out[idx] = struct{}{}
	}
	for i := range observed {
		s := &observed[i]
		// A row DeleteBatch owns keeps its group until the teardown
		// removes it; re-ensuring one here would recreate a contract the
		// teardown is dismantling.
		if workload.Owner(s) == workload.OwnerDelete {
			out[s.Index] = struct{}{}
		}
	}
	return out
}

func podsForPodGroup(ctx context.Context, c client.Reader, namespace, name string) ([]*corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := c.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{query.LabelPodGroup: name}); err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	return pods, nil
}

// patchGangSchedulingCondition writes the GangSchedulingUnavailable
// Component-level condition through input.WriteAggregateCondition.
// The closure wraps apimeta.SetStatusCondition + apiserver round-
// trip under retry.RetryOnConflict.
func patchGangSchedulingCondition(ctx context.Context, input workload.ReconcileInput, crdPresent, anyMultiPod bool) error {
	var generation int64
	if input.OwnerObject != nil {
		generation = input.OwnerObject.GetGeneration()
	}
	desired := gangSchedulingUnavailableCondition(crdPresent, anyMultiPod, generation)
	return input.WriteAggregateCondition(ctx, desired)
}

// gangSchedulingUnavailableCondition builds the
// GangSchedulingUnavailable condition. True when the Component has
// at least one multi-pod Instance but the cluster lacks the
// scheduler-plugins PodGroup CRD; False otherwise.
func gangSchedulingUnavailableCondition(crdPresent, anyMultiPodInstance bool, generation int64) metav1.Condition {
	now := metav1.Now()
	if !anyMultiPodInstance || crdPresent {
		// Single-pod-only Component OR the gang scheduler is
		// available — either way, no degradation to flag.
		return metav1.Condition{
			Type:               string(workload.ConditionGangSchedulingUnavailable),
			Status:             metav1.ConditionFalse,
			Reason:             string(workload.ReasonGangSchedulingAvailable),
			Message:            "Gang scheduling is available or not required",
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	}
	return metav1.Condition{
		Type:               string(workload.ConditionGangSchedulingUnavailable),
		Status:             metav1.ConditionTrue,
		Reason:             string(workload.ReasonPodGroupCRDNotInstalled),
		Message:            "scheduler-plugins scheduling.x-k8s.io/v1alpha1 PodGroup CRD is not installed; multi-pod Instances may schedule partially",
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

// maybeWarnNoGangScheduler emits a one-shot Warning when a PodGroup
// was created but the pod template's effective spec.schedulerName is
// empty or "default-scheduler". The stock kube-scheduler doesn't
// read scheduler-plugins PodGroup objects, so the gang contract is
// silently broken in that configuration.
//
// "Effective" schedulerName: leaderSpec wins, then workerSpec, then
// treated as unset. The warning is a heuristic — we can't detect
// scheduler-plugins installed AS the default scheduler from inside
// the controller, so accept the false-positive cost there.
//
// The warning is about the Component's template, so one delivery per
// episode is what an operator needs; idx is the first Instance whose
// group this pass ensured, and its row carries the record.
//
// nil-safe: nil recorder, nil owner, nil pod template are no-ops.
func maybeWarnNoGangScheduler(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, component workload.ComponentType, idx int32, leaderSpec, workerSpec *corev1.PodSpec) error {
	target := workload.EventTarget(input)
	if target == nil || deps.Recorder == nil {
		return nil
	}
	name := effectiveSchedulerName(leaderSpec, workerSpec)
	if name != "" && name != corev1.DefaultSchedulerName {
		// Operator opted in to a non-default scheduler — trust them.
		return nil
	}
	announced, err := status.Announce(ctx, input, idx, workload.EventReasonMaybeNoGangScheduler)
	if err != nil || !announced {
		return err
	}
	deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(workload.EventReasonMaybeNoGangScheduler),
		"OMENative component=%s created scheduler-plugins PodGroup objects but pod template's spec.schedulerName is %q (default kube-scheduler does not enforce PodGroup gang); set runtime.spec.schedulerName to a gang-aware scheduler (e.g. scheduler-plugins-scheduler) or install scheduler-plugins as a plugin in the default scheduler",
		component, name)
	return nil
}

// effectiveSchedulerName returns the leader's spec.schedulerName when
// set, otherwise the worker's, otherwise empty. Mirrors the
// "leader-wins" precedence the runtime templating uses elsewhere; a
// nil template counts as "unset" for that side.
func effectiveSchedulerName(leaderSpec, workerSpec *corev1.PodSpec) string {
	if leaderSpec != nil && leaderSpec.SchedulerName != "" {
		return leaderSpec.SchedulerName
	}
	if workerSpec != nil && workerSpec.SchedulerName != "" {
		return workerSpec.SchedulerName
	}
	return ""
}
