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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

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
// brings the Component's pods and state brings the owner's PodGroups as the
// apiserver lists them, the callback performs no extra Pod LIST or PodGroup
// GET; an inventory the cache served is read live first, since the callback
// writes; without an inventory it reads the name once. A single-pod
// Instance or a cluster without the PodGroup API gets no PodGroup, only the
// topology key its live pods already carry; a live read that finds the API
// gone says so on the Component and takes that path. Idempotent.
func EnsureSurgePodGroupWithState(deps workload.Deps, state PodGroupReconcileState) workload.EnsureGangPodGroupFn {
	return func(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) (string, error) {
		if deps.Client == nil || !podgroup.IsMultiPodInstance(inst) {
			return plan.TopologyKeyForInstance(inst.Index), nil
		}
		name := query.PodGroupName(input.Key.OwnerName, plan.Component, inst.Index)
		pods, err := surgeGangPods(ctx, deps.Reader(), &input, name)
		if err != nil {
			return "", err
		}
		available, err := surgeSchedulingAvailable(&input, state.Inventory)
		if err != nil {
			return "", err
		}
		surge := &surgeGangWrite{shared: gangPassShared{deps: deps, input: &input, plan: plan, inventory: state.Inventory}, inst: inst}
		surge.write = gangWrite{gangPassShared: &surge.shared, inst: &surge.inst, name: name, pods: pods}
		write := &surge.write
		if !available {
			return write.topologyFromPods()
		}
		if input.OwnerObject == nil {
			return "", fmt.Errorf("ensure surge PodGroup: nil owner")
		}
		if err := state.Inventory.ReadLive(ctx, deps.Reader()); err != nil {
			return "", fmt.Errorf("ensure surge PodGroup: %w", err)
		}
		apiGone, err := liveReadFoundNoAPI(ctx, &input, state.Inventory)
		if err != nil {
			return "", fmt.Errorf("ensure surge PodGroup: %w", err)
		}
		if apiGone {
			return write.topologyFromPods()
		}
		if err := write.readName(ctx); err != nil {
			return "", fmt.Errorf("ensure surge PodGroup: %w", err)
		}
		// The surge index may not be in the plan yet, so this is the first
		// place the row's PodGroup is read at all and the only chance to
		// record why its members are withheld.
		write.observe()
		key, err := write.write(ctx)
		if err != nil {
			return "", unusableNameError(err)
		}
		return key, nil
	}
}

// surgeGangPods are the pods that carry one gang's PodGroup name: taken from
// the adapter's live list when it brought one, else listed by that label.
func surgeGangPods(ctx context.Context, reader client.Reader, input *workload.ReconcileInput, name string) ([]*corev1.Pod, error) {
	if input.AuthoritativePods == nil {
		pods, err := podsForPodGroup(ctx, reader, input.Key.Namespace, name)
		if err != nil {
			return nil, fmt.Errorf("list surge PodGroup pods: %w", err)
		}
		return pods, nil
	}
	var pods []*corev1.Pod
	for _, pod := range input.AuthoritativePods.Pods {
		if pod != nil && pod.Labels[query.LabelPodGroup] == name {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

// surgeSchedulingAvailable settles whether the surge may write a PodGroup:
// the spec must advertise the PodGroup API and, when the caller brought an
// inventory, that inventory must have found it. An inventory built for
// another owner is a wiring error, not a reading.
func surgeSchedulingAvailable(input *workload.ReconcileInput, inventory *PodGroupInventory) (bool, error) {
	available := input.DesiredSpec.GangSchedulingAvailable
	if !available || inventory == nil {
		return available, nil
	}
	if input.OwnerObject == nil || inventory.OwnerUID() != input.OwnerObject.GetUID() {
		return false, fmt.Errorf("ensure surge PodGroup: inventory owner does not match input owner")
	}
	return inventory.Available(), nil
}

// liveReadFoundNoAPI reports whether the apiserver's read of an inventory
// the cache had shown available answered that there is no PodGroup API,
// and writes the GangSchedulingUnavailable condition True when it did. The
// manager discovered the API at its start and the cache keeps answering
// after the API is removed, so this read is where a pass learns of the
// removal; from here it runs as on a cluster without the API, and the
// condition is the record the next pass reads the apiserver on.
func liveReadFoundNoAPI(ctx context.Context, input *workload.ReconcileInput, inventory *PodGroupInventory) (bool, error) {
	if inventory == nil || inventory.Cached() || inventory.Available() {
		return false, nil
	}
	if err := patchGangSchedulingCondition(ctx, *input, false, true); err != nil {
		return false, fmt.Errorf("patch GangSchedulingUnavailable condition: %w", err)
	}
	return true, nil
}

// gangPassShared is what one pass holds once and every gang's write reads:
// the clients, the reconcile input, the plan and the owner's PodGroups. A
// record points at it, so a record is the gang's own fields alone.
type gangPassShared struct {
	deps  workload.Deps
	input *workload.ReconcileInput
	plan  workload.ComponentPlan
	// inventory is the owner's PodGroups when the caller brought them; nil
	// means the name is read on its own.
	inventory *PodGroupInventory
}

// surgeGangWrite is the surge callback's write record together with the
// readings the pass shares with every gang and the callback shares with
// nobody: one allocation per surge holds both.
type surgeGangWrite struct {
	shared gangPassShared
	inst   workload.InstancePlan
	write  gangWrite
}

// gangWrite is the PodGroup write of one planned gang: the Instance, its
// deterministic name, the pods that carry that name, what the read found
// at the name, and the group the plan and the pods ask for, over the
// readings the pass shares with every gang. The pass builds one per
// planned gang from one allocation per pod list; the surge callback builds
// one for the Instance it is about to create pods for. Both run the same
// steps: readName, observe, write; the pass asks needed first when it
// decides on the cached pods.
type gangWrite struct {
	*gangPassShared
	// inst is the plan's entry for the gang.
	inst *workload.InstancePlan
	name string
	pods []*corev1.Pod
	// existing is the object readName found at the name; found is false
	// when nothing holds it.
	existing *schedulingv1alpha1.PodGroup
	// desired is the group the plan and the pods ask for, built once by
	// needed or write, whichever runs first, and used by both; a settled
	// group is never built.
	desired *schedulingv1alpha1.PodGroup
	found   bool
	// settled is the reading of the object at the name against the plan,
	// taken once by settledGroup: true when it is already the group the
	// plan and the pods ask for.
	settled, settledKnown bool
}

// readName settles what holds the gang's name: the inventory's entry when
// there is an inventory, else one read of the name through the live
// reader. A name nothing holds reads as not found.
func (w *gangWrite) readName(ctx context.Context) error {
	if w.inventory != nil {
		w.existing, w.found = w.inventory.ByName(w.name)
		return nil
	}
	existing := &schedulingv1alpha1.PodGroup{}
	err := w.deps.Reader().Get(ctx, client.ObjectKey{Namespace: w.input.Key.Namespace, Name: w.name}, existing)
	switch {
	case err == nil:
		w.existing, w.found = existing, true
	case apierrors.IsNotFound(err):
		w.existing, w.found = nil, false
	default:
		return fmt.Errorf("get %s/%s: %w", w.input.Key.Namespace, w.name, err)
	}
	return nil
}

// observe records what readName found as the Instance's gang reading and
// returns it.
func (w *gangWrite) observe() workload.GangObservation {
	obs := podgroup.ObserveGang(w.name, w.existing, w.found, w.input.OwnerObject.GetUID())
	w.input.Gangs.Record(w.inst.Index, obs)
	return obs
}

// settledGroup reports whether the object at the name is already the
// group the plan and the pods ask for, read field by field without
// building it, so a gang at rest costs the pass nothing. The reading is
// taken once; a group it does not settle is decided on the built group,
// which also carries the error the build reports.
func (w *gangWrite) settledGroup() bool {
	if !w.settledKnown {
		w.settled = podgroup.GroupSettled(w.input.OwnerObject, w.input.OwnerGVK, w.input.Key.OwnerName,
			w.plan, w.input.UnschedulableGrace, *w.inst, w.pods, w.existing, w.found)
		w.settledKnown = true
	}
	return w.settled
}

// desiredGroup is the group the plan and the pods ask for, built on the
// first call: the plan's group carrying the key the pods prove, or the key
// the object at the name records when they prove none.
func (w *gangWrite) desiredGroup() (*schedulingv1alpha1.PodGroup, error) {
	if w.desired != nil {
		return w.desired, nil
	}
	desired, err := podgroup.DesiredPodGroup(w.input.OwnerObject, w.input.OwnerGVK, w.input.Key.OwnerName,
		w.plan, w.input.UnschedulableGrace, *w.inst, w.pods, w.existing, w.found)
	if err != nil {
		return nil, err
	}
	w.desired = desired
	return desired, nil
}

// needed reports whether write would send anything, the name being free
// or the object at it differing from the desired group, and the topology
// key the group records either way. A settled group is read, not built. A
// name this owner cannot write and a topology the pods cannot prove are
// reported as write reports them.
func (w *gangWrite) needed() (bool, string, error) {
	if w.settledGroup() {
		return false, podgroup.TopologyKeyOf(w.existing), nil
	}
	desired, err := w.desiredGroup()
	if err != nil {
		return false, "", err
	}
	return podgroup.WriteNeeded(desired, w.existing, w.found), podgroup.TopologyKeyOf(desired), nil
}

// write brings the object at the name to what the plan asks for, overlays
// the result on the inventory when there is one, so a later write in the
// same pass sees it without a second read, and returns the gang's
// effective topology key. A settled group is left as the inventory holds
// it. A write is decided on the apiserver's inventory: one the cache
// served refuses a write it would call for.
func (w *gangWrite) write(ctx context.Context) (string, error) {
	if w.settledGroup() {
		return podgroup.TopologyKeyOf(w.existing), nil
	}
	desired, err := w.desiredGroup()
	if err != nil {
		return "", err
	}
	if w.inventory.Cached() && podgroup.WriteNeeded(desired, w.existing, w.found) {
		return "", fmt.Errorf("write %s/%s: %w", w.input.Key.Namespace, w.name, ErrInventoryCached)
	}
	reconciled, err := podgroup.WritePodGroup(ctx, w.deps.Client, desired, w.existing, w.found)
	if err != nil {
		return "", err
	}
	w.inventory.recordReconciled(reconciled)
	return podgroup.TopologyKeyOf(desired), nil
}

// topologyFromPods is the key the gang keeps when no PodGroup is written:
// the one its live pods already carry, or the plan's when it has none.
func (w *gangWrite) topologyFromPods() (string, error) {
	return podgroup.EffectiveTopologyKeyForPods(w.plan.TopologyKeyForInstance(w.inst.Index), w.input.Key.OwnerName,
		w.plan.Component, w.inst.Index, w.pods)
}

// EnsurePodGroupsWithState drives the per-Instance PodGroup lifecycle for
// one Component reconcile and returns the effective topology key of every
// planned gang. Adapters call it before the per-Instance create pass, so the
// gang is announced to the scheduler before its first pod exists; a gang
// announced later would be placed pod by pod. A single-pod Instance gets no
// PodGroup. When the plan has a gang and the cluster lacks the PodGroup API,
// whether the manager found none at its start or the apiserver's read of
// this pass finds it gone, nothing is written and the
// GangSchedulingUnavailable condition is True: pods are still created, and
// the condition is the operator's signal that gang placement may race. An
// empty topology key is valid only when topology
// was left unset; a configured key that cannot be proven from a gang's live
// pods or its PodGroup fails the pass before any missing member is created.
// It performs no eager deletion: terminal lifecycle owners delete their own
// PodGroups after proving their Pod bucket empty.
//
// The pass runs in one fixed order: read the inventory, write the
// availability condition, read the pods and group them by PodGroup, then
// for every planned gang record what its PodGroup says and ensure or reset
// it, and last the scheduler-name warning. The condition is written before
// any pod read or PodGroup write, so a later failure cannot hide whether
// gang scheduling is usable, and written again, True, when the apiserver's
// read before a write finds the API gone; the warning runs after the
// writes, so a PodGroup write failure takes precedence over it. The
// inventory and the pods are the cache's readings when they settle every
// gang with nothing to write, and the apiserver's otherwise: every write
// and reset is decided on the live groups and the live pods.
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
	pass.terminalOwned = terminalOwnedIndices(input.ObservedState.InstanceStatuses, state.TerminalOwned)
	available, err = pass.readPods(ctx, available)
	if err != nil {
		return nil, err
	}
	if !available {
		return pass.topologyWithoutGroups()
	}
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
// for, the owner's PodGroups read once, the Component's pods keyed by
// PodGroup name, the write of each gang over those pods, and what the pass
// records per Instance.
type podGroupPass struct {
	// gangPassShared is what every gang's record reads; the inventory in
	// it is the caller's when it brought one, else the cache's, read once
	// by readInventory; a pass that may write a group reads the
	// apiserver's into it first.
	gangPassShared
	// gangs are the plan's multi-pod Instances in plan order, each the
	// plan's own entry; a planned gang's record points at it.
	gangs []*workload.InstancePlan
	// positions finds a planned gang's record by its Instance index.
	positions gangPositions
	// records are the gangs' PodGroup writes over the Component's pods, one
	// per planned gang in plan order, each carrying the gang's name and the
	// pods under it: the adapter's live list, the pass's cached list, or the
	// apiserver's, as readPods settled. groupPods builds them all from one
	// pod list, so the decision to read the apiserver and the write that
	// follows share one record.
	records []gangWrite
	// strays holds, under the PodGroup name each carries, the listed pods
	// whose name is no planned gang's plain name; a gang whose name the
	// label limit hashed finds its pods here. Built only when such a pod is
	// listed.
	strays map[string][]*corev1.Pod
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
	gangs := plannedGangs(plan)
	return &podGroupPass{
		gangPassShared: gangPassShared{deps: deps, input: input, plan: plan, inventory: inventory},
		gangs:          gangs,
		positions:      newGangPositions(gangs, len(plan.Instances)),
		topology:       make(map[int32]string, len(gangs)),
	}
}

// plannedGangs lists the plan's multi-pod Instances in plan order, each
// the plan's own entry, in one allocation of pointers: the pass reads the
// entries and changes none, so it keeps no copy of them. A single-pod
// Instance has nothing to gang.
func plannedGangs(plan workload.ComponentPlan) []*workload.InstancePlan {
	count := 0
	for i := range plan.Instances {
		if podgroup.IsMultiPodInstance(plan.Instances[i]) {
			count++
		}
	}
	if count == 0 {
		return nil
	}
	gangs := make([]*workload.InstancePlan, 0, count)
	for i := range plan.Instances {
		if podgroup.IsMultiPodInstance(plan.Instances[i]) {
			gangs = append(gangs, &plan.Instances[i])
		}
	}
	return gangs
}

// gangPositions finds a planned gang's position among the pass's gangs by
// its Instance index. An index below the plan's count sits in the slice,
// where a plan at rest has every gang, since a new Instance takes the
// lowest free index; an index at or above the count, which a gap below it
// leaves after a surge or a scale-down, sits in the map, built only for
// those. An index the plan lists twice keeps its first position.
type gangPositions struct {
	// dense holds, at an index below the plan's count, the gang's
	// position plus one; zero is an index no planned gang has.
	dense []int32
	// sparse holds the positions of the gangs whose index is at or above
	// the plan's count; nil when there is none.
	sparse map[int32]int
}

// newGangPositions indexes gangs, the plan's gangs in plan order, over a
// plan of planCount entries.
func newGangPositions(gangs []*workload.InstancePlan, planCount int) gangPositions {
	if len(gangs) == 0 {
		return gangPositions{}
	}
	positions := gangPositions{dense: make([]int32, planCount)}
	for pos, inst := range gangs {
		if _, seen := positions.of(inst.Index); seen {
			continue
		}
		if inst.Index >= 0 && int(inst.Index) < planCount {
			positions.dense[inst.Index] = int32(pos) + 1
			continue
		}
		if positions.sparse == nil {
			positions.sparse = map[int32]int{}
		}
		positions.sparse[inst.Index] = pos
	}
	return positions
}

// of is the position of the planned gang whose Instance index is index,
// and whether the plan has one.
func (g gangPositions) of(index int32) (int, bool) {
	if index >= 0 && int(index) < len(g.dense) {
		pos := g.dense[index]
		return int(pos) - 1, pos > 0
	}
	pos, ok := g.sparse[index]
	return pos, ok
}

// groupName is the deterministic PodGroup name of one Instance.
func (p *podGroupPass) groupName(idx int32) string {
	return query.PodGroupName(p.input.Key.OwnerName, p.plan.Component, idx)
}

// readInventory settles whether gang scheduling is usable this pass: the
// spec must advertise the PodGroup API, and the owner's inventory, read
// from the cache here when the caller brought none, must have found it.
// An inventory built for another owner is a wiring error, not a reading.
func (p *podGroupPass) readInventory(ctx context.Context) (bool, error) {
	if !p.input.DesiredSpec.GangSchedulingAvailable {
		return false, nil
	}
	if p.inventory == nil {
		inventory, err := ObserveCachedPodGroups(ctx, p.deps.Client, p.input.OwnerObject)
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

// readPods settles the pods the gangs are decided on, and reports whether
// gang scheduling is still usable once they are read. The adapter's live
// list stands when it brought one. Otherwise the pass takes the pass's one
// cached list, the list the engine decides on after it, and that reading
// stands when it settles every planned gang with nothing to write; when a
// gang's cached pods cannot settle it, or its group would be written or
// reset, the pass reads the apiserver once, the owner's groups and the
// Component's pods, and decides on that, so every write is decided on the
// live groups and the live pods. Sharing the list with the engine keeps
// the two from reading the cache at different moments of one pass: the key
// this pass publishes for a gang and the pods the engine renders on it come
// from one picture.
func (p *podGroupPass) readPods(ctx context.Context, available bool) (bool, error) {
	if p.input.AuthoritativePods != nil {
		p.groupPods(p.input.AuthoritativePods.Pods)
		return p.readInventoryLive(ctx, available)
	}
	cached, err := p.input.CachedPods.Pods(ctx, p.listPodsCached)
	if err != nil {
		return false, fmt.Errorf("EnsurePodGroups: list component pods: %w", err)
	}
	p.groupPods(cached)
	if p.decidedOnCachedPods(ctx, available) {
		return available, nil
	}
	return p.readLive(ctx, available)
}

// readLive takes the apiserver's picture for a pass that may write: the
// owner's groups, when the inventory is the cache's, then the Component's
// pods, and starts the gangs' writes over from them. It reports whether
// gang scheduling is still usable on that picture.
func (p *podGroupPass) readLive(ctx context.Context, available bool) (bool, error) {
	available, err := p.readInventoryLive(ctx, available)
	if err != nil {
		return false, err
	}
	live, err := query.ListOMENativePodsByName(ctx, p.deps.Reader(), p.input.Key.Namespace,
		p.input.Key.OwnerName, p.plan.Component, false)
	if err != nil {
		return false, fmt.Errorf("EnsurePodGroups: list component pods: %w", err)
	}
	p.groupPods(live)
	return available, nil
}

// readInventoryLive brings an inventory the cache served to the
// apiserver's picture, since the pass may write a group, and reports
// whether gang scheduling is still usable: the apiserver may answer that
// the PodGroup API is gone, which the cache's picture does not show, and
// the pass then goes on without groups. Without the PodGroup API no group
// is written, so the cache's reading stands.
func (p *podGroupPass) readInventoryLive(ctx context.Context, available bool) (bool, error) {
	if !available {
		return false, nil
	}
	if err := p.inventory.ReadLive(ctx, p.deps.Reader()); err != nil {
		return false, fmt.Errorf("EnsurePodGroups: %w", err)
	}
	apiGone, err := liveReadFoundNoAPI(ctx, p.input, p.inventory)
	if err != nil {
		return false, fmt.Errorf("EnsurePodGroups: %w", err)
	}
	return !apiGone, nil
}

// listPodsCached lists the Component's pods through the cache.
func (p *podGroupPass) listPodsCached(ctx context.Context) ([]*corev1.Pod, error) {
	return query.ListOMENativePodsByName(ctx, p.deps.Client, p.input.Key.Namespace,
		p.input.Key.OwnerName, p.plan.Component, true)
}

// decidedOnCachedPods reports whether the cached pods settle every planned
// gang with nothing to write, so the apiserver's list could change no
// decision of this pass.
func (p *podGroupPass) decidedOnCachedPods(ctx context.Context, available bool) bool {
	for _, inst := range p.gangs {
		if !p.gangDecidedOnCache(ctx, inst, available) {
			return false
		}
	}
	return true
}

// gangDecidedOnCache reports whether one gang is settled by its cached
// pods and, when the inventory is the cache's, its cached group: the key
// the pass would publish from them is the key the plan asks for, and
// nothing would be written. A gang whose key is changing is decided on
// the apiserver's pods, so its group advances on the pass that finds the
// gang empty there. Without the PodGroup API the gang keeps the key its
// pods prove, so the cached pods settle it when they prove the plan's key,
// or when the plan asks for none and every pod it asks for is among them,
// since the apiserver's list could then prove no other. With the API the
// gang is settled when its group needs no write and no reset: a group
// that matches the plan records the key its pods were created under, so a
// pod the cache has not shown yet carries that key; a group with no key
// recorded leans on the pods alone, so a gang the cache shows in part is
// decided on the apiserver's pods. A name the cache shows free, or a group
// it shows drifted or failed, is read from the apiserver before anything
// is written or reset.
func (p *podGroupPass) gangDecidedOnCache(ctx context.Context, inst *workload.InstancePlan, available bool) bool {
	write := p.writeOf(inst)
	complete := len(write.pods) >= int(inst.TotalPods())
	planKey := p.plan.TopologyKeyForInstance(inst.Index)
	if !available {
		key, proven, err := podgroup.TopologyKeyProvenByPods(p.input.Key.OwnerName, p.plan.Component, inst.Index, write.pods)
		if err != nil {
			return false
		}
		if proven {
			return key == planKey
		}
		return complete && planKey == ""
	}
	if _, terminal := p.terminalOwned[inst.Index]; terminal {
		return true
	}
	if err := write.readName(ctx); err != nil {
		return false
	}
	obs := write.observe()
	if obs.State == workload.GangStateFailed && resettableIndex(p.input.ObservedState, inst.Index) {
		return false
	}
	needed, key, err := write.needed()
	if err != nil {
		return nameUnusable(err)
	}
	if needed || key != planKey {
		return false
	}
	return complete || key != ""
}

// groupPods keys the Component's pods by the PodGroup name each carries
// and starts the gangs' writes over from that list: one record per planned
// gang, carrying the gang's name and its pods in list order over the
// readings the pass shares with every gang. A pod is a
// gang's when it carries the gang's name, whatever its other labels say.
// A planned gang's plain name spells its index, so such a pod is placed by
// the index its name spells, the name the record carries is the label the
// pod already carries, and every gang's pods are cut from one array, each
// bucket at its own length so a later append reaches no other gang's. A
// pod carrying any other name is kept by name, where a gang whose name the
// label limit hashed finds its pods; a gang with no pod listed formats its
// name.
func (p *podGroupPass) groupPods(pods []*corev1.Pod) {
	p.records = make([]gangWrite, len(p.gangs))
	p.strays = nil
	counts := make([]int, len(p.gangs))
	total := 0
	for _, pod := range pods {
		name, pos, planned := p.gangOf(pod)
		switch {
		case name == "":
		case planned:
			if counts[pos] == 0 {
				p.records[pos].name = name
			}
			counts[pos]++
			total++
		default:
			if p.strays == nil {
				p.strays = make(map[string][]*corev1.Pod)
			}
			p.strays[name] = append(p.strays[name], pod)
		}
	}
	backing := make([]*corev1.Pod, total)
	next := 0
	for pos := range p.records {
		record := &p.records[pos]
		inst := p.gangs[pos]
		record.gangPassShared, record.inst = &p.gangPassShared, inst
		if counts[pos] == 0 {
			record.name = p.groupName(inst.Index)
			record.pods = p.strays[record.name]
			continue
		}
		record.pods = backing[next : next : next+counts[pos]]
		next += counts[pos]
	}
	for _, pod := range pods {
		if _, pos, planned := p.gangOf(pod); planned {
			p.records[pos].pods = append(p.records[pos].pods, pod)
		}
	}
}

// gangOf reads the PodGroup name a listed pod carries, empty for a nil
// pod or one with no name, and the position of the planned gang whose
// plain name it is, when it is one.
func (p *podGroupPass) gangOf(pod *corev1.Pod) (name string, pos int, planned bool) {
	if pod == nil {
		return "", 0, false
	}
	name = pod.Labels[query.LabelPodGroup]
	if name == "" {
		return "", 0, false
	}
	index, ok := query.PodGroupIndex(name, p.input.Key.OwnerName, p.plan.Component)
	if !ok {
		return name, 0, false
	}
	pos, planned = p.positions.of(index)
	return name, pos, planned
}

// topologyWithoutGroups is the pass when the cluster cannot enforce gangs:
// no PodGroup is written, and each gang keeps the topology its pods
// already carry, so a missing member is not rebuilt under a newer key only
// because the optional PodGroup API is absent.
func (p *podGroupPass) topologyWithoutGroups() (map[int32]string, error) {
	for _, inst := range p.gangs {
		key, err := p.writeOf(inst).topologyFromPods()
		if err != nil {
			return nil, fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
		}
		p.topology[inst.Index] = key
	}
	return p.topology, nil
}

// writeOf is the PodGroup write of one planned gang, over the pass's
// inventory and the pods grouped under the gang's name: one per gang and
// pod list, so the pass's decision and its write share the read of the
// name and the desired group. An Instance the plan does not list, or one
// asked for before the pods are grouped, gets a record of its own over
// the same readings.
func (p *podGroupPass) writeOf(inst *workload.InstancePlan) *gangWrite {
	if pos, ok := p.positions.of(inst.Index); ok && p.records != nil {
		return &p.records[pos]
	}
	name := p.groupName(inst.Index)
	own := *inst
	return &gangWrite{gangPassShared: &p.gangPassShared, inst: &own, name: name, pods: p.strays[name]}
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
func (p *podGroupPass) ensureGang(ctx context.Context, inst *workload.InstancePlan) error {
	if _, terminal := p.terminalOwned[inst.Index]; terminal {
		return nil
	}
	write := p.writeOf(inst)
	if err := write.readName(ctx); err != nil {
		return fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
	}
	obs := write.observe()
	if obs.State == workload.GangStateFailed && resettableIndex(p.input.ObservedState, inst.Index) {
		return p.resetFailedGroup(ctx, write, obs)
	}
	key, err := write.write(ctx)
	if err != nil {
		if nameUnusable(err) {
			return nil
		}
		return fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
	}
	p.recordEnsured(inst.Index, key)
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
func (p *podGroupPass) resetFailedGroup(ctx context.Context, write *gangWrite, obs workload.GangObservation) error {
	if anyTerminalMember(write.pods) {
		return nil
	}
	if err := p.inventory.DeleteOwnedName(ctx, p.deps.Client, obs.Name); err != nil {
		return fmt.Errorf("EnsurePodGroups: reset instance %d: %w", write.inst.Index, err)
	}
	return warnPodGroupReset(ctx, p.deps, *p.input, p.plan.Component, write.inst.Index, obs)
}

// recordEnsured publishes the gang's topology key and keeps the first
// ensured Instance that has a row as the place for the scheduler-name
// warning.
func (p *podGroupPass) recordEnsured(idx int32, key string) {
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

// nameUnusable reports the two reasons a deterministic name cannot be
// written this pass: the object at it is terminating, or another
// controller holds it.
func nameUnusable(err error) bool {
	return errors.Is(err, podgroup.ErrPodGroupTerminating) ||
		errors.Is(err, podgroup.ErrPodGroupOwnershipConflict)
}

// unusableNameError re-spells an unusable name as the workload-level
// error, so the op that invoked the surge callback can defer to the
// escalation pass without importing the optional gang-scheduling API.
// Anything else is a real failure and travels unchanged.
func unusableNameError(err error) error {
	if nameUnusable(err) {
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
