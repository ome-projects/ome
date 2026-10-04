package sliceprovision

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/tpuslice/gke"
)

const (
	// EventReasonSliceDemandInvalid is recorded when an Instance's pods
	// select a provision-only pool but cannot fill a provisionable slice.
	EventReasonSliceDemandInvalid workload.EventReason = "SliceDemandInvalid"
	// EventReasonSliceOwnershipConflict is recorded when a slice the owner
	// does not hold has the name of one of the owner's slots.
	EventReasonSliceOwnershipConflict workload.EventReason = "SliceOwnershipConflict"
	// EventReasonSliceHostUnavailable is recorded when an Instance's pods are
	// withheld from a ready slice with a host that cannot take them, and the
	// slice is provisioned again.
	EventReasonSliceHostUnavailable workload.EventReason = "SliceHostUnavailable"
	// EventReasonSliceReadyTimeout is recorded when an Instance's pods are
	// withheld from a slice that has stayed out of a ready state for longer
	// than the ready timeout, and the slice is provisioned again.
	EventReasonSliceReadyTimeout workload.EventReason = "SliceReadyTimeout"
)

// Placer places the workload engine's pods on provisioned slices. It serves
// one reconcile pass: each Instance's demand is resolved, each slot's slice
// ensured, and each slice vetted, at most once per pass, and Sweep keeps
// every slice the pass placed pods on.
type Placer struct {
	p        *Provisioner
	nodes    client.Reader
	pods     func(context.Context) ([]*corev1.Pod, error)
	recorder record.EventRecorder
	demands  map[demandKey]*resolution
	ensured  map[placedSlot]*ensureResult
	// vetted holds, by UID, each slice the pass vetted. Sweep releases the
	// slices whose vetting says so.
	vetted map[types.UID]vetting
}

// vetting is a slice's vetting in one pass. A non-empty reason withholds its
// slot's pods; release has Sweep release the slice so the slot is provisioned
// again; event is the warning a withholding Place records, empty for none.
type vetting struct {
	reason  string
	release bool
	event   workload.EventReason
}

var _ workload.Provisioner = (*Placer)(nil)

// demandKey identifies an Instance's pods by the templates the engine
// renders them from and the runners they fill. A pass may render an
// Instance from templates other than the desired ones, as a migration surge
// does, and a pass never modifies a template it renders from.
type demandKey struct {
	pod, worker *corev1.PodSpec
	layout      string
}

// resolution is a resolved demand and the templates of the pods it is
// resolved from. err is set only for ErrInvalidDemand.
type resolution struct {
	demand Demand
	pods   []*corev1.PodSpec
	needed bool
	err    error
	warned bool
}

// placedSlot is a slot and the demand the pass placed its pods for.
type placedSlot struct {
	slot   Slot
	demand Demand
}

// ensureResult is an ensured slot and its slice's vetting. err is set only
// for gke.ErrOwnershipConflict.
type ensureResult struct {
	placement Placement
	err       error
	vetting   vetting
	warned    bool
}

// NewPlacer returns a Placer for one reconcile pass of p's owner. nodes
// serves the node reads that decide whether a pool provisions slices and
// whether a slice's hosts can take pods. pods must read the owner's pods
// live, terminating ones included.
func NewPlacer(p *Provisioner, nodes client.Reader, pods func(context.Context) ([]*corev1.Pod, error), recorder record.EventRecorder) *Placer {
	return &Placer{
		p:        p,
		nodes:    nodes,
		pods:     pods,
		recorder: recorder,
		demands:  map[demandKey]*resolution{},
		ensured:  map[placedSlot]*ensureResult{},
		vetted:   map[types.UID]vetting{},
	}
}

// Place ensures the slice of the pod's slot and confines the pod to it once
// the slice is ready. A pod on no provision-only pool is placed unconfined.
// A pod whose Instance cannot fill a provisionable slice, or whose slot's
// name is taken by a slice the owner does not hold, is withheld and a
// warning is recorded. So is a pod whose slice no pod holds and either has a
// host that cannot take the slot's pods or has stayed out of a ready state
// past the ready timeout; Sweep releases that slice, and a later pass
// provisions the slot again. A pod waits, with no warning, until every host
// its ready slice needs is visible.
func (pl *Placer) Place(ctx context.Context, input workload.ReconcileInput, _ workload.ComponentPlan, inst workload.InstancePlan, _ workload.RunnerPlan, ordinal int32) (map[string]string, bool, error) {
	r, err := pl.resolve(ctx, input, inst)
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if !r.warned {
			r.warned = true
			workload.RecordWarning(pl.recorder, workload.EventTarget(input), EventReasonSliceDemandInvalid,
				"OMENative %s withheld: %v", workload.InstanceKey(input.Key.Component, inst.Index), r.err)
		}
		return nil, false, nil
	}
	if !r.needed {
		return nil, true, nil
	}
	e, err := pl.ensure(ctx, r, SlotFor(inst, ordinal), input.Now())
	if err != nil {
		return nil, false, err
	}
	if e.err != nil {
		if !e.warned {
			e.warned = true
			workload.RecordWarning(pl.recorder, workload.EventTarget(input), EventReasonSliceOwnershipConflict,
				"OMENative %s withheld: %v", workload.InstanceKey(input.Key.Component, inst.Index), e.err)
		}
		return nil, false, nil
	}
	if e.vetting.event != "" && !e.warned {
		e.warned = true
		workload.RecordWarning(pl.recorder, workload.EventTarget(input), e.vetting.event,
			"OMENative %s withheld: %s", workload.InstanceKey(input.Key.Component, inst.Index), e.placement.Reason)
	}
	if !e.placement.Ready {
		return nil, false, nil
	}
	return e.placement.NodeSelector, true, nil
}

// Pending reports why Place withholds the pod. It creates no slice and
// records nothing. Slices and nodes are read from the cache; a ready slice
// with a host that cannot take the pods also costs a live read of the
// owner's pods.
func (pl *Placer) Pending(ctx context.Context, input workload.ReconcileInput, _ workload.ComponentPlan, inst workload.InstancePlan, _ workload.RunnerPlan, ordinal int32) (string, bool, error) {
	r, err := pl.resolve(ctx, input, inst)
	if err != nil {
		return "", false, err
	}
	if r.err != nil {
		return r.err.Error(), true, nil
	}
	if !r.needed {
		return "", false, nil
	}
	placement, err := pl.p.Observe(ctx, r.demand, SlotFor(inst, ordinal))
	if errors.Is(err, gke.ErrOwnershipConflict) {
		return err.Error(), true, nil
	}
	if err != nil {
		return "", false, err
	}
	if placement, _, err = pl.vet(ctx, placement, r, input.Now()); err != nil {
		return "", false, err
	}
	return placement.Reason, !placement.Ready, nil
}

// Sweep releases the owner's slices that no pod is pinned to and on which no
// pod may be created. A slice is kept for a slot the pass placed pods on, or
// a slot a planned Instance or the target of a row's surge may create a pod
// at, while it has the shape that slot's pods ask for, unless the pass's
// vetting releases it.
func (pl *Placer) Sweep(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan) error {
	wanted := map[Slot][]Demand{}
	for placed := range pl.ensured {
		wanted[placed.slot] = append(wanted[placed.slot], placed.demand)
	}
	for _, inst := range liveInstances(input, plan) {
		r, err := pl.resolve(ctx, input, inst)
		if err != nil {
			return fmt.Errorf("sweep slices: %w", err)
		}
		if r.err != nil || !r.needed {
			continue
		}
		for _, ordinal := range liveOrdinals(input, inst) {
			slot := SlotFor(inst, ordinal)
			wanted[slot] = append(wanted[slot], r.demand)
		}
	}
	return pl.p.Sweep(ctx, func(slot Slot, s gke.Slice) bool {
		if pl.vetted[s.UID].release {
			return false
		}
		for _, d := range wanted[slot] {
			if pl.p.Fits(s, d, slot) {
				return true
			}
		}
		return false
	}, pl.pinned)
}

// pinned is a live read of the slices the owner's pods are pinned to.
func (pl *Placer) pinned(ctx context.Context) (map[string]struct{}, error) {
	pods, err := pl.ownerPods(ctx)
	if err != nil {
		return nil, err
	}
	return pl.p.Pinned(pods), nil
}

// ownerPods is a live read of the owner's pods.
func (pl *Placer) ownerPods(ctx context.Context) ([]*corev1.Pod, error) {
	if pl.pods == nil {
		return nil, errors.New("no source of the owner's pods")
	}
	return pl.pods(ctx)
}

// resolve returns inst's demand as input renders its pods. Only a
// successful resolution or an invalid demand is remembered: a failed read
// is retried.
func (pl *Placer) resolve(ctx context.Context, input workload.ReconcileInput, inst workload.InstancePlan) (*resolution, error) {
	desired := input.DesiredSpec
	key := demandKey{pod: desired.PodSpec, worker: desired.WorkerPodSpec, layout: layoutOf(inst)}
	if r, ok := pl.demands[key]; ok {
		return r, nil
	}
	pods := podSpecs(desired, inst)
	d, needed, err := Resolve(ctx, pl.nodes, pl.p.cfg, pods)
	if err != nil && !errors.Is(err, ErrInvalidDemand) {
		return nil, err
	}
	r := &resolution{demand: d, pods: pods, needed: needed, err: err}
	pl.demands[key] = r
	return r, nil
}

// ensure runs Ensure, and vets the slice, once per slot and demand. Only a
// placement or an ownership conflict is remembered: a failed read or write is
// retried.
func (pl *Placer) ensure(ctx context.Context, r *resolution, slot Slot, now time.Time) (*ensureResult, error) {
	key := placedSlot{slot: slot, demand: r.demand}
	if e, ok := pl.ensured[key]; ok {
		return e, nil
	}
	placement, err := pl.p.Ensure(ctx, r.demand, slot)
	if err != nil && !errors.Is(err, gke.ErrOwnershipConflict) {
		return nil, err
	}
	e := &ensureResult{placement: placement, err: err}
	if err == nil {
		if e.placement, e.vetting, err = pl.vet(ctx, placement, r, now); err != nil {
			return nil, err
		}
	}
	pl.ensured[key] = e
	return e, nil
}

// vet applies the slice's vetting to its placement: a ready slice's hosts are
// vetted, a slice that is not ready is vetted against the ready timeout. Each
// slice is vetted once per pass.
func (pl *Placer) vet(ctx context.Context, placement Placement, r *resolution, now time.Time) (Placement, vetting, error) {
	s := placement.Slice
	if s.Terminating {
		return placement, vetting{}, nil
	}
	v, ok := pl.vetted[s.UID]
	if !ok {
		var err error
		if placement.Ready {
			v, err = pl.vetHosts(ctx, s, r)
		} else {
			v, err = pl.vetStuck(ctx, s, r, now)
		}
		if err != nil {
			return Placement{}, vetting{}, err
		}
		pl.vetted[s.UID] = v
	}
	if v.reason == "" {
		return placement, v, nil
	}
	return Placement{Slice: s, Reason: v.reason}, v, nil
}

// vetHosts vets a ready slice's hosts. A host that cannot take any of the
// pods, while no pod holds the slice, withholds the pods and releases the
// slice: every host takes one of the slot's pods, so the slot cannot start
// there. A slice a pod holds is left to the scheduler, since the pods it
// still needs may fit on the hosts that can take them. Until every host the
// slice needs is visible the pods wait: hosts are found by a label the
// provider adds as the slice activates, which may lag its state.
func (pl *Placer) vetHosts(ctx context.Context, s gke.Slice, r *resolution) (vetting, error) {
	hosts, found, err := unavailableHosts(ctx, pl.nodes, pl.p.cfg.NodeLabels.Slice, s.Name, r.pods)
	if err != nil {
		return vetting{}, err
	}
	if len(hosts) > 0 {
		held, err := pl.held(ctx, s)
		if err != nil {
			return vetting{}, err
		}
		if !held {
			return vetting{
				reason: fmt.Sprintf("slice %s is %s, but %s; it is released and provisioned again once no pod holds it",
					s.Name, s.State, strings.Join(hosts, ", ")),
				release: true,
				event:   EventReasonSliceHostUnavailable,
			}, nil
		}
	}
	need, err := r.demand.Shape.Topology.Hosts(pl.p.cfg.Accelerators[r.demand.Shape.Accelerator].ChipsPerHost)
	if err != nil {
		return vetting{}, fmt.Errorf("hosts of slice %s: %w", s.Name, err)
	}
	if int64(found) < need {
		return vetting{reason: fmt.Sprintf("slice %s is %s, but only %d of its %d hosts are visible yet", s.Name, s.State, found, need)}, nil
	}
	return vetting{}, nil
}

// vetStuck releases a slice that has partitions but has stayed out of a ready
// state for longer than the ready timeout, while no pod holds it: a new slice
// gets a new partition. A slice still waiting for a partition is waiting for
// capacity, which a new slice would wait for too.
func (pl *Placer) vetStuck(ctx context.Context, s gke.Slice, r *resolution, now time.Time) (vetting, error) {
	timeout := pl.p.cfg.Slice.ReadyTimeoutDuration()
	if timeout <= 0 || len(s.PartitionIDs) == 0 || s.Type != r.demand.SliceType || s.Topology != r.demand.Shape.Topology.String() {
		return vetting{}, nil
	}
	since := s.StateSince
	if since.IsZero() {
		since = s.Created
	}
	waited := now.Sub(since)
	if since.IsZero() || waited <= timeout {
		return vetting{}, nil
	}
	held, err := pl.held(ctx, s)
	if err != nil || held {
		return vetting{}, err
	}
	return vetting{
		reason: fmt.Sprintf("%s for %s, longer than the %s ready timeout; it is released and provisioned again once no pod holds it",
			notReadyReason(s), waited.Round(time.Second), timeout),
		release: true,
		event:   EventReasonSliceReadyTimeout,
	}, nil
}

// held reports whether a live read finds a pod of the owner that holds s.
func (pl *Placer) held(ctx context.Context, s gke.Slice) (bool, error) {
	owned, err := pl.ownerPods(ctx)
	if err != nil {
		return false, fmt.Errorf("read the pods that hold slice %s: %w", s.Name, err)
	}
	_, held := pl.p.Held(owned)[s.Name]
	return held, nil
}

// podSpecs are the templates of inst's pods, one per pod: a worker renders
// from the worker template when there is one, every other pod from the pod
// template.
func podSpecs(desired workload.WorkloadDesiredSpec, inst workload.InstancePlan) []*corev1.PodSpec {
	specs := make([]*corev1.PodSpec, 0, inst.TotalPods())
	for _, runner := range inst.Runners {
		spec := desired.PodSpec
		if runner.Name == workload.RunnerWorker && desired.WorkerPodSpec != nil {
			spec = desired.WorkerPodSpec
		}
		for i := int32(0); i < runner.Size; i++ {
			specs = append(specs, spec)
		}
	}
	return specs
}

func layoutOf(inst workload.InstancePlan) string {
	var b strings.Builder
	for _, runner := range inst.Runners {
		fmt.Fprintf(&b, "%s=%d;", runner.Name, runner.Size)
	}
	return b.String()
}

// liveInstances are the Instances whose pods the engine may create: the
// planned ones, and the targets of the rows' surges, which the plan omits
// until they have rows of their own. A surge target mirrors its source's
// runners.
func liveInstances(input workload.ReconcileInput, plan workload.ComponentPlan) []workload.InstancePlan {
	live := append([]workload.InstancePlan(nil), plan.Instances...)
	planned := make(map[int32]workload.InstancePlan, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = inst
	}
	for _, row := range input.ObservedState.InstanceStatuses {
		if row.Operation == nil || row.Operation.SurgeIndex == nil {
			continue
		}
		target := *row.Operation.SurgeIndex
		if _, ok := planned[target]; ok {
			continue
		}
		source, ok := planned[row.Index]
		if !ok {
			continue
		}
		inst := workload.InstancePlan{Index: target, Runners: source.Runners}
		planned[target] = inst
		live = append(live, inst)
	}
	return live
}

// liveOrdinals are the ordinals inst's pods may be created at. A single-pod
// Instance moves between ordinals 0 and 1 across surges, so while it has an
// operation in flight either may be the one it creates.
func liveOrdinals(input workload.ReconcileInput, inst workload.InstancePlan) []int32 {
	if inst.TotalPods() > 1 {
		return []int32{0}
	}
	row := input.ObservedState.Instance(inst.Index)
	switch {
	case row == nil:
		return []int32{0}
	case row.Operation != nil:
		return []int32{0, 1}
	default:
		return []int32{row.ActiveOrdinal}
	}
}
