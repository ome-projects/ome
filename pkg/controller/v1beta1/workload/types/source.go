package types

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkloadDesiredSpec.Lifecycle uses a workload-owned mirror type.
// Adapters project the CRD-shape values into it at the boundary.

// WorkloadDesiredSpec is the per-reconcile projection of the source
// spec the workload pipeline drives toward. Read-only from workload
// code.
type WorkloadDesiredSpec struct {
	// Replicas is the desired Instance count.
	Replicas int32
	// AllowZeroReplicas marks an explicitly resolved scale-to-zero policy.
	// Without it, zero retains the workload adapter's unset-count fallback.
	AllowZeroReplicas bool

	// MinReadySeconds is how long (in seconds) a newly Ready pod must stay
	// Ready before it is Available. BuildPlan copies it onto
	// ComponentPlan.MinReadySeconds; the rollout engine paces drains and
	// promotions on Available pods and status aggregation counts only
	// Available pods. 0 means Available as soon as Ready.
	MinReadySeconds int32

	// Runners enumerates the Runner roles (default | leader | worker)
	// and per-role pod counts that constitute one Instance.
	Runners []Runner

	// PodSpec is the rendered leader / single-pod template the
	// renderer drives toward. May be nil when Replicas=0 (nothing to
	// render).
	PodSpec *corev1.PodSpec

	// WorkerPodSpec is the rendered worker template for multi-pod
	// Instances. Nil for single-pod Components.
	WorkerPodSpec *corev1.PodSpec

	// PodTemplateObjectMeta is the rendered per-Component metadata
	// (labels, annotations, owner refs) the renderer stamps onto each
	// emitted pod.
	PodTemplateObjectMeta *metav1.ObjectMeta

	// MultiPod indicates the workload uses Leader + Worker Runners
	// (each Instance materializes more than one pod).
	MultiPod bool

	// TopologyKey is the resolved gang co-location node-label key for
	// this Component (e.g. an NVLink/RDMA fabric-domain label). When non-empty on
	// a multi-pod Component, the renderer auto-generates the per-Instance
	// worker→leader podAffinity and stamps the same key on the PodGroup for
	// topology-aware gang schedulers. Empty means no controller-generated
	// topology constraint. Adapters project it from the owner-CRD component
	// spec at the boundary.
	TopologyKey string

	// TopologySpread is the gang spreading policy ("Preferred" /
	// "Required"; empty = pure bin-packing) advertised on each
	// multi-pod Instance's PodGroup for the topology-aware gang
	// scheduler. Adapters project it from the owner-CRD component spec.
	TopologySpread string

	// TopologySpreadKey is the fault-domain node-label key TopologySpread
	// spreads across; empty defaults to TopologyKey.
	TopologySpreadKey string

	// PairingProtocol is the P/D wire-compatibility token this Component's
	// revision is minted under. It participates in the revision hash (a
	// change mints a new revision) and is stamped as the
	// ome.io/pairing-protocol label on rendered pods. Empty means the
	// Component pairs with anything. Adapters project it from the owner-CRD
	// spec at the boundary; only Components that pair (engine, decoder)
	// carry it.
	PairingProtocol string

	// Lifecycle holds RestartPolicy / UpdateStrategy / ReadyPolicy /
	// InstanceReadyTimeout / MigrationPolicy as workload-owned mirror
	// values; adapters project the CRD-shape lifecycle into this struct
	// at the boundary.
	Lifecycle Lifecycle

	// Pacing is the projected rollout pacing for this reconcile. Nil
	// means "no pacing constraint" (treated as allowed).
	Pacing *WorkloadPacing

	// Paused, when true, stops the reconciler from starting a new Update,
	// Create, or Migration operation and from starting a new step of one
	// already in flight, which runs the step it is on to that step's
	// boundary. The RestartPolicy keeps repairing existing Instances
	// unless PauseFreeze is also set.
	Paused bool

	// PauseFreeze deepens Paused onto the restart pass: no repair starts,
	// and one already under way only finishes its step. Meaningless
	// unless Paused is true.
	PauseFreeze bool

	// GangSchedulingAvailable is true when the scheduler-plugins
	// PodGroup CRD was discovered at controller startup. Drives
	// whether podgroup.EnsurePodGroup runs for multi-pod Instances.
	GangSchedulingAvailable bool
}

// WorkloadObservedState is the per-reconcile snapshot of the source
// status subtree the reconciler reasons about.
type WorkloadObservedState struct {
	// CurrentRevision names the ControllerRevision currently serving
	// traffic.
	CurrentRevision string

	// UpdateRevision names the ControllerRevision being rolled out.
	UpdateRevision string

	// RollTarget names the ControllerRevision the roll converges to this
	// pass: the spec's revision, or the one a rollback names, where
	// UpdateRevision keeps naming the spec's. The adapter fills it beside
	// the plan's other target-derived inputs; a scale-down reads it to keep
	// the Instances already there ahead of the ones the roll has yet to
	// move. Empty when the pass has no roll target, as under teardown.
	RollTarget string

	// InstanceStatuses reports per-Instance state. Read-only here;
	// writes go through ReconcileInput.MutateInstance.
	InstanceStatuses []InstanceStatus

	// RetryBlocks mirrors the owner's persisted per-revision retry
	// authority. Read-only from workload code; writes go through
	// ReconcileInput.MutateRetryBlock.
	RetryBlocks []RetryBlock

	// Migrations mirrors the owner's persisted migration records
	// (status.migrations) — the single source of truth for migration
	// work. Read-only from workload code; writes go through
	// ReconcileInput.MutateMigration.
	Migrations []MigrationRecord

	// ExcludedNodesByInstance maps Instance index → nodes its rebuild
	// must avoid, each with the revision it was recorded for, projected
	// per reconcile by the adapter from the audit ledger's AutoRecover
	// relocation directives (bounded to the operator's
	// autoMigrate.maxAttempts most recent entries). BuildPlan copies it
	// onto InstancePlan.ExcludedNodes; Render materializes the entries
	// recorded for the revision it renders as a required NodeAffinity
	// NotIn overlay. Nil / missing index = no exclusion (zero change for
	// normal instances).
	ExcludedNodesByInstance map[int32][]NodeExclusion

	// CordonedInstances is the set of Instance indices with a live pod on
	// a cordoned node (spec.unschedulable), projected per reconcile by the
	// adapter from cached pods and nodes. A scale-down removes these
	// Instances before any other, and its wave admits them first. Nil
	// means no Instance is known to sit on a cordoned node.
	CordonedInstances map[int32]struct{}
	// WorkerSizeByRevision is the worker pods per Instance each revision the
	// rows reference was minted at, projected per reconcile by the adapter
	// from the ControllerRevisions. A revision absent here records no width
	// and renders the desired one.
	WorkerSizeByRevision map[string]int32

	// rowIndex is where each row sits in InstanceStatuses, built by IndexRows
	// at the start of a pass and shared by every copy of the state made after
	// it. nil until then, and a lookup without one scans the slice.
	rowIndex *rowIndex
}

// rowIndex is the position of each Instance's row in one row slice, so a
// lookup by Instance index costs the same on 4,000 rows as on 4. It is
// built for one slice and never edited afterwards: a state whose slice was
// replaced builds a new one, and a copy of the state still holding this
// one reads the slice it was built for.
type rowIndex struct {
	rows     []InstanceStatus
	position map[int32]int
}

// indexRows records the first row of each Instance index.
func indexRows(rows []InstanceStatus) *rowIndex {
	position := make(map[int32]int, len(rows))
	for i := range rows {
		if _, seen := position[rows[i].Index]; !seen {
			position[rows[i].Index] = i
		}
	}
	return &rowIndex{rows: rows, position: position}
}

// covers reports whether the index was built from rows: the same backing
// array read at the same length. The engine reads rows in place and never
// moves them, so an index that covers the slice is current.
func (x *rowIndex) covers(rows []InstanceStatus) bool {
	if x == nil || len(x.rows) != len(rows) {
		return false
	}
	return len(rows) == 0 || &x.rows[0] == &rows[0]
}

// IndexRows indexes the rows once for this state and for every copy of it
// made afterwards, so a pass that calls it on entry reads rows through one
// index all the way down. A state that already indexed this slice keeps
// its index. Only this method builds one: a lookup never does, so a copy
// made before the index existed scans its slice as it always has.
func (o *WorkloadObservedState) IndexRows() {
	if !o.rowIndex.covers(o.InstanceStatuses) {
		o.rowIndex = indexRows(o.InstanceStatuses)
	}
}

// Instance returns the observed row at idx, or nil when the observation
// carries none. The pointer aliases the observation and is read-only: a
// write goes through MutateInstance, which re-reads the row it lands on.
// With an index that covers the slice the lookup is one map read; without
// one, or when the row at the recorded position does not carry idx, the
// slice is scanned.
func (o WorkloadObservedState) Instance(idx int32) *InstanceStatus {
	if o.rowIndex.covers(o.InstanceStatuses) {
		position, found := o.rowIndex.position[idx]
		if !found {
			return nil
		}
		if o.InstanceStatuses[position].Index == idx {
			return &o.InstanceStatuses[position]
		}
	}
	for i := range o.InstanceStatuses {
		if o.InstanceStatuses[i].Index == idx {
			return &o.InstanceStatuses[i]
		}
	}
	return nil
}

// WorkloadPacing is the projected rollout pacing: the rollout-control
// values a composer (the ISVC controller's canary machine) owns, kept
// apart from the user's Lifecycle so a step never writes into the
// user's update strategy. Adapters compute it once per reconcile from
// IR spec.pacing. The engine reads Partition (ops.EffectivePartition:
// it takes precedence over the user's RollingUpdate.Partition); the
// per-Component disruption budget comes from RollingUpdate.MaxUnavailable
// and the group ceiling from the UpdateGate callback. (IR
// spec.pacing.rollbackToRevision is handled by the IR reconciler, not
// carried here.)
type WorkloadPacing struct {
	// Partition holds back updates for the lowest-indexed `Partition`
	// Instances still off the target revision. 0 releases every
	// Instance even when the user's RollingUpdate carries a partition;
	// nil defers to the user's RollingUpdate.Partition. Used for canary
	// step holds.
	Partition *int32
}

// Runner is a named role within an Instance with a per-Instance pod
// count. Single-pod has one "default" Runner of Size=1; multi-pod has
// "leader" (Size=1) + "worker" (Size=N).
type Runner struct {
	// Name is "leader" | "worker" | "default".
	Name string
	// Size is the per-Instance pod count for this Runner.
	Size int32
}

// WorkerSizeOf returns the worker Runner's pod count, or 0 when the layout
// has no worker Runner (a single-pod Component).
func WorkerSizeOf(runners []Runner) int32 {
	for _, r := range runners {
		if r.Name == RunnerWorker {
			return r.Size
		}
	}
	return 0
}

// RunnersWithWorkerSize returns a copy of runners with the worker Runner at
// size; a layout without a worker Runner is returned unchanged.
func RunnersWithWorkerSize(runners []Runner, size int32) []Runner {
	out := append([]Runner(nil), runners...)
	for i := range out {
		if out[i].Name == RunnerWorker {
			out[i].Size = size
		}
	}
	return out
}
