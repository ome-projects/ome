package types

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DeadlineAt is the Operation.Deadline an operation opening at now runs
// under. A non-positive timeout means no readiness backstop is in force —
// neither the per-Component spec.lifecycle.instanceReadyTimeout nor the
// operator's lifecycle.instanceReadyTimeout is set — and yields the zero
// Time, which the deadline predicate reads as "never expires". Without
// this the operation would open on a deadline equal to its own start and
// expire on the next pass.
func DeadlineAt(now metav1.Time, timeout time.Duration) metav1.Time {
	if timeout <= 0 {
		return metav1.Time{}
	}
	return metav1.NewTime(now.Add(timeout))
}

// ComponentPlan is the desired Component → Instance → Runner → Pod
// shape computed each reconcile from desired + observed state. Not
// persisted; policy fields hold the effective values after defaults.
type ComponentPlan struct {
	// Component identifies which of router / engine / decoder this plan
	// describes. Typed as the workload-side ComponentType so the
	// workload package stays free of v1beta1 imports; adapters convert
	// from v1beta1.ComponentType at the boundary.
	Component ComponentType

	// Replicas is the desired number of Instances for this Component
	// (= MinReplicas, or 1 when MinReplicas is unset).
	Replicas int32

	// Instances enumerates the desired Instances. Normally one entry
	// per index 0..Replicas-1; indices may be sparse while a surge
	// migration is in flight (the surge Instance carries an
	// out-of-band index until promotion settles the plan).
	Instances []InstancePlan

	// RestartPolicy is the effective Instance-group restart policy.
	RestartPolicy RestartPolicy

	// UpdateStrategy is the effective rollout policy across Instances.
	UpdateStrategy UpdateStrategy

	// ReadyPolicy is the effective Instance-level readiness aggregation.
	ReadyPolicy InstanceReadyPolicy

	// InstanceReadyTimeout is the wait ceiling on a newly-created
	// Instance becoming Ready: the per-Component
	// spec.lifecycle.instanceReadyTimeout, else the operator's
	// lifecycle.instanceReadyTimeout. Zero means neither is configured and
	// operations open with no deadline.
	InstanceReadyTimeout time.Duration

	// GangScheduleTimeout is the range of PodGroup schedule timeouts the
	// gang scheduler accepts; the placement grace a multi-pod Instance's
	// PodGroup carries as its timeout is kept inside it. Operator config
	// (lifecycle.gangScheduleTimeout); nil means unconfigured and the
	// grace reaches the scheduler as it is.
	GangScheduleTimeout *GangScheduleTimeoutClamp

	// MinReadySeconds is how long a newly Ready pod must stay Ready before
	// it is Available. Rollout drains and promotions wait on Available
	// pods, so the per-Component budgets stay held for the window. 0 means
	// Available as soon as Ready.
	MinReadySeconds int32

	// MigrationMode is the effective migration disposition (auto / surge /
	// never).
	MigrationMode MigrationMode

	// Paused stops the dispatcher from starting a new operation or a new
	// STEP: no Migration, Update, or Create work begins, while an attempt
	// already in flight runs the step it is on to that step's boundary,
	// so a hold never leaves an Instance mid-step. Scale-down remains
	// active so a reduced desired replica count can still release
	// capacity, and the restart pass keeps repairing existing Instances
	// at their current revision unless PauseFreeze is also set.
	Paused bool

	// PauseFreeze deepens Paused onto the restart pass: no repair starts,
	// and one already under way only finishes its step. Meaningless
	// unless Paused is true.
	PauseFreeze bool

	// TopologyKey is the resolved gang co-location node-label key for
	// this Component (e.g. an NVLink/RDMA fabric-domain label). Empty for
	// single-pod Components or when unset. When non-empty on a multi-pod
	// Component, Render auto-generates the per-Instance worker→leader
	// podAffinity and the PodGroup carries the same key for topology-aware
	// gang schedulers. Both constraints therefore select the same configured
	// topology domain.
	TopologyKey string

	// InstanceTopologyKeys holds a temporary per-Instance override while live
	// pods still carry affinity rendered from an older revision. Missing pods in
	// that active gang must use the same immutable topology contract; fresh
	// Instances and surge indices fall back to TopologyKey.
	InstanceTopologyKeys map[int32]string

	// TopologySpread is the gang spreading policy ("Preferred" / "Required";
	// empty = pure bin-packing) the PodGroup builder advertises to the
	// topology-aware gang scheduler. Placement-time preference only — it
	// never constrains rendered pods.
	TopologySpread string

	// TopologySpreadKey is the fault-domain node-label key TopologySpread
	// spreads across; empty defaults to TopologyKey.
	TopologySpreadKey string

	// PairingProtocol is the P/D wire-compatibility token of the revision
	// being rendered; Render stamps it as the ome.io/pairing-protocol label
	// on every pod. Empty renders no label (pairs with anything).
	PairingProtocol string

	// Runners is the Runner layout the roll target renders: one default
	// Runner, or a leader beside the desired worker count. An Instance
	// rendered at another revision carries that revision's layout on its
	// own entry (InstancePlan.Runners).
	Runners []RunnerPlan

	// WorkerSizeByRevision is the worker count each referenced revision was
	// minted at (WorkloadObservedState.WorkerSizeByRevision). A revision
	// absent here records no width and renders the desired one.
	WorkerSizeByRevision map[string]int32
}

// RunnersFor returns the Runner layout Instance inst renders at revision
// rev: the roll target's layout at the width rev recorded, when the plan
// knows it, else as the target renders it. A plan built without a target
// layout answers with the Instance's own entry. The layout is the plan's
// own when rev changes no width in it and a copy at rev's width otherwise,
// so a caller reads it and never writes into it.
func (p ComponentPlan) RunnersFor(inst InstancePlan, rev string) []RunnerPlan {
	base := p.layoutFor(inst)
	if len(base) == 0 {
		return nil
	}
	size, known := p.workerSizeAt(rev)
	if !known || !workerOfAnotherWidth(base, size) {
		return base
	}
	out := append([]RunnerPlan(nil), base...)
	for i := range out {
		if out[i].Name == RunnerWorker {
			out[i].Size = size
		}
	}
	return out
}

// workerOfAnotherWidth reports whether a worker of layout runs at a width
// other than size, so that the layout at size is another layout.
func workerOfAnotherWidth(layout []RunnerPlan, size int32) bool {
	for _, runner := range layout {
		if runner.Name == RunnerWorker && runner.Size != size {
			return true
		}
	}
	return false
}

// TotalPodsFor returns how many pods Instance inst renders at revision
// rev: the pods of the layout RunnersFor builds, counted without building
// it. The update selection reads it for every Instance on every pass.
func (p ComponentPlan) TotalPodsFor(inst InstancePlan, rev string) int32 {
	size, known := p.workerSizeAt(rev)
	var total int32
	for _, runner := range p.layoutFor(inst) {
		if known && runner.Name == RunnerWorker {
			total += size
			continue
		}
		total += runner.Size
	}
	return total
}

// layoutFor is the layout RunnersFor and TotalPodsFor read for inst: the
// roll target's, or the Instance's own entry when the plan carries none.
func (p ComponentPlan) layoutFor(inst InstancePlan) []RunnerPlan {
	if len(p.Runners) == 0 {
		return inst.Runners
	}
	return p.Runners
}

// workerSizeAt is the worker count revision rev recorded, and whether the
// plan knows it; a layout rendered at a revision it does not know keeps
// its own width.
func (p ComponentPlan) workerSizeAt(rev string) (int32, bool) {
	if rev == "" {
		return 0, false
	}
	size, known := p.WorkerSizeByRevision[rev]
	return size, known
}

// TopologyKeyForInstance returns the live-safe topology contract for an
// Instance, falling back to the Component's current desired key when the group
// has no active revision-specific override.
func (p ComponentPlan) TopologyKeyForInstance(index int32) string {
	if key, ok := p.InstanceTopologyKeys[index]; ok {
		return key
	}
	return p.TopologyKey
}

// DemotionSelection is one Instance the truth pass corrects, with the
// observed evidence that justifies the status-only transition.
type DemotionSelection struct {
	Index  int32
	Reason string
}

// NodeExclusion is one node an Instance's rebuild must not land on, and
// the revision the exclusion was recorded for. A relocation directive is
// evidence about one revision on one node; a rebuild at any other
// revision is a different workload and lands wherever there is room.
type NodeExclusion struct {
	Node     string
	Revision string
}

// InstancePlan is the desired state for one Instance — the atomic
// unit of gang scheduling, restart, and migration.
type InstancePlan struct {
	// Index is the stable Instance ordinal. Sparse after surge
	// migration.
	Index int32

	// Incarnation is the monotonic lifecycle token for this
	// (Component, Index) pair. Initial create stamps 1; full recreate
	// / restart-on-loss bumps it; in-place update does not. Distinguishes
	// old pod materializations from current ones when stable names get
	// reused after a recreate.
	Incarnation int64

	// Runners enumerates the Runners that constitute this Instance.
	// Single-pod has one "default" Runner of size 1; multi-pod has
	// leader + worker.
	Runners []RunnerPlan

	// MigrationOverlay carries placement hints from an in-flight surge
	// migration. Render injects hard anti-affinity from the source node
	// and preferred affinity to hint target nodes. Nil for non-migration
	// paths.
	MigrationOverlay *MigrationOverlay

	// ExcludedNodes lists nodes this Instance's pods must not land on —
	// the relocation-directive memory (AutoRecover ledger entries)
	// projected through WorkloadObservedState.ExcludedNodesByInstance.
	// Render materializes each entry as the same required NotIn
	// hostname term the migration overlay uses, on a pod of the
	// revision the entry was recorded for. Empty for normal Instances.
	ExcludedNodes []NodeExclusion

	// PeerHostnames is an optional render-time cache of the Instance's
	// ordered peer-DNS host list (one entry per pod, flat gang-rank
	// order). The list is identical for every pod in the gang, so the
	// gang-render loop computes it once and stashes it here to avoid the
	// O(gangsize^2) per-pod recompute. Nil when unset; Render then
	// derives the list from Runners as before. Transient render scratch,
	// not part of the desired state and never persisted.
	PeerHostnames []string
}

// MigrationOverlay records the per-Instance placement constraints a
// surge migration carries onto the rendered pod. Materialized as pod
// affinity in Render and intentionally absent from the canonical
// revision payload — transient operation overlay, not a steady-state
// template field.
type MigrationOverlay struct {
	// FromNode surfaces as RequiredDuringScheduling anti-affinity on
	// kubernetes.io/hostname.
	FromNode string
	// HintTargetNodes surface as PreferredDuringScheduling affinity;
	// the scheduler is free to pick another node when these are
	// unavailable.
	HintTargetNodes []string
}

// Runner names. A single-pod Instance has one RunnerDefault; a multi-pod
// Instance has one RunnerLeader of size 1 and one RunnerWorker of the
// configured worker size. The name is a segment of every pod name.
const (
	RunnerDefault = "default"
	RunnerLeader  = "leader"
	RunnerWorker  = "worker"
)

// RunnerPlan is the desired state for one Runner within an Instance.
type RunnerPlan struct {
	// Name is RunnerLeader, RunnerWorker, or RunnerDefault (single-pod).
	Name string
	// Size is the number of pods of this Runner within the Instance.
	Size int32
}

// TotalPods returns the total desired pod count across all Runners —
// 1 for single-pod, 1+N for leader+worker.
func (i InstancePlan) TotalPods() int32 {
	var n int32
	for _, r := range i.Runners {
		n += r.Size
	}
	return n
}

// SurgeIndexAllocator hands out the Instance indexes the surges of one
// pass take: each is the lowest index that no row holds and no surge in
// flight has claimed. A source mid-surge records its replacement's index
// in Operation.SurgeIndex before the replacement's row exists, so those
// claims count as taken; without them every surge of one pass would land
// on the same index, and when that one replacement became Ready every
// source sharing it would drain at once. The rows are read once, on the
// first ask, and every index below from is taken, so the surges of a pass
// pay one pass over the indexes in all. The indexes claimed through this
// allocator are kept apart: the plan was computed without their rows, so
// a fresh Instance the plan placed on one of them must wait for the next
// pass.
type SurgeIndexAllocator struct {
	rows    []InstanceStatus
	taken   map[int32]struct{}
	claimed map[int32]struct{}
	from    int32
}

// NewSurgeIndexAllocator is the allocator over rows, which it reads when
// the first index is asked for.
func NewSurgeIndexAllocator(rows []InstanceStatus) *SurgeIndexAllocator {
	return &SurgeIndexAllocator{rows: rows}
}

// Next is the index the next surge takes. It is offered again until Claim
// marks it, so a surge that fails to claim it leaves it to the next. The
// taken set is finite, so the scan ends.
func (a *SurgeIndexAllocator) Next() int32 {
	a.read()
	for {
		if _, taken := a.taken[a.from]; !taken {
			return a.from
		}
		a.from++
	}
}

// Claim marks idx as taken by a surge of this pass.
func (a *SurgeIndexAllocator) Claim(idx int32) {
	a.read()
	a.taken[idx] = struct{}{}
	if a.claimed == nil {
		a.claimed = map[int32]struct{}{}
	}
	a.claimed[idx] = struct{}{}
}

// Claimed is the set of indexes the surges of this pass took through
// Claim; nil when none did.
func (a *SurgeIndexAllocator) Claimed() map[int32]struct{} {
	return a.claimed
}

// read builds the taken set from the rows once: every row's index and
// every surge index a row's operation claims.
func (a *SurgeIndexAllocator) read() {
	if a.taken != nil {
		return
	}
	a.taken = make(map[int32]struct{}, len(a.rows)*2)
	for i := range a.rows {
		s := &a.rows[i]
		a.taken[s.Index] = struct{}{}
		if s.Operation != nil && s.Operation.SurgeIndex != nil {
			a.taken[*s.Operation.SurgeIndex] = struct{}{}
		}
	}
}

// AllocateSurgeIndex is the lowest index that no row of instances holds
// and no surge in flight has claimed: the one index a caller outside a
// pass's allocator takes.
func AllocateSurgeIndex(instances []InstanceStatus) int32 {
	return NewSurgeIndexAllocator(instances).Next()
}
