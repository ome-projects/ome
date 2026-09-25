package types

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// WorkloadDesiredSpec.Lifecycle and WorkloadAggregateStatus.Traffic
// use workload-owned mirror types. Adapters project the CRD-shape
// values into these structs at the boundary.

// WorkloadDesiredSpec is the per-reconcile projection of the source
// spec the workload pipeline drives toward. Read-only from workload
// code.
type WorkloadDesiredSpec struct {
	// Replicas is the desired Instance count.
	Replicas int32

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
	// ObservedGeneration is the spec generation the last status flush
	// reflects.
	ObservedGeneration int64

	// CollisionCount is the ControllerRevision-hash salt. nil on the
	// first reconcile.
	CollisionCount *int32

	// CurrentRevision names the ControllerRevision currently serving
	// traffic.
	CurrentRevision string

	// UpdateRevision names the ControllerRevision being rolled out.
	UpdateRevision string

	// InstanceStatuses reports per-Instance state. Read-only here;
	// writes go through ReconcileInput.MutateInstance.
	InstanceStatuses []InstanceStatus

	// Conditions reports component-level conditions.
	Conditions []metav1.Condition

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
	// must avoid, projected per reconcile by the adapter from the
	// audit ledger's AutoRecover relocation directives (bounded to the
	// operator's autoMigrate.maxAttempts most recent entries). BuildPlan
	// copies it onto InstancePlan.ExcludedNodes; Render materializes it
	// as a required NodeAffinity NotIn overlay. Nil / missing index =
	// no exclusion (zero change for normal instances).
	ExcludedNodesByInstance map[int32][]string
}

// Instance returns the observed row at idx, or nil when the observation
// carries none. The pointer aliases the observation and is read-only: a
// write goes through MutateInstance, which re-reads the row it lands on.
func (o WorkloadObservedState) Instance(idx int32) *InstanceStatus {
	for i := range o.InstanceStatuses {
		if o.InstanceStatuses[i].Index == idx {
			return &o.InstanceStatuses[i]
		}
	}
	return nil
}

// WorkloadAggregateStatus is the per-reconcile flush of counters,
// conditions, and traffic that the reconciler hands off to
// Source.WriteAggregateStatus.
type WorkloadAggregateStatus struct {
	ObservedGeneration   int64
	Replicas             int32
	ReadyReplicas        int32
	ServingReplicas      int32
	AvailableReplicas    int32
	UpdatedReplicas      int32
	UpdatedReadyReplicas int32
	CurrentRevision      string
	UpdateRevision       string
	LabelSelector        string
	Conditions           []metav1.Condition
	Traffic              []ComponentTrafficTarget
}

// WorkloadPacing is the projected rollout pacing: the rollout-control
// values a composer (the ISVC controller's canary machine) owns, kept
// apart from the user's Lifecycle so a step never writes into the
// user's update strategy. Adapters compute it once per reconcile from
// IR spec.pacing.
//
// Only Partition is read by the engine (escalation.EffectivePartition:
// it takes precedence over the user's RollingUpdate.Partition).
// MaxUnavailable has no producer and is not read — the per-Component
// budget comes from RollingUpdate.MaxUnavailable and the group ceiling
// from the UpdateGate callback. Decisions is never constructed in
// production. (IR spec.pacing.rollbackToRevision is handled by the IR
// reconciler, not carried here.)
type WorkloadPacing struct {
	// Partition holds back updates for the lowest-indexed `Partition`
	// Instances still off the target revision. 0 releases every
	// Instance even when the user's RollingUpdate carries a partition;
	// nil defers to the user's RollingUpdate.Partition. Used for canary
	// step holds.
	Partition *int32

	// MaxUnavailable caps in-rollout disruption. Nothing projects it
	// and the engine does not read it; the budget comes from
	// RollingUpdate.MaxUnavailable.
	MaxUnavailable *intstr.IntOrString

	// Decisions is the projected per-gate allow/deny map for this
	// reconcile. The ISVC adapter fills it from coordination gates;
	// the IR adapter leaves it nil. nil means allowed.
	Decisions *PacingDecisions
}

// PacingDecisions is the workload-facing projection of cross-Component
// coordination gates. Plain data — adapters compute it outside the
// workload package.
type PacingDecisions struct {
	// UpdateAllowed is false when the reconciler MUST NOT start any
	// fresh Update operations this reconcile.
	UpdateAllowed bool

	// UpdateDenyReason is the operator-visible reason recorded onto
	// events / conditions when UpdateAllowed is false.
	UpdateDenyReason string

	// SurgeBudgetRemaining is the upper bound on in-flight surge pods
	// the reconciler may keep alive concurrently. The dispatcher
	// tracks its own in-flight count within a pass; this is a
	// per-reconcile ceiling.
	SurgeBudgetRemaining int32
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
