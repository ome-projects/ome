// Package snapshot builds Alfred's ClusterSnapshot: an immutable, in-memory
// model of the physical GPU layer — nodes and their GPU accounting, OME
// workloads broken into components and instances, model availability, and
// pending-pod pressure — captured once per loop and shared read-only across
// every policy (OEP-0008).
package snapshot

import (
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ClusterSnapshot is the read surface every Alfred policy consumes. It is
// built once per observation pass and never mutated afterwards; policies that
// need hypothetical state derive a copy (see WithVirtualPending).
type ClusterSnapshot struct {
	// Timestamp is when the snapshot was built.
	Timestamp time.Time

	// Nodes indexes every node in the cluster by name, GPU-bearing or not
	// (GPU-less nodes carry TotalGPUs == 0 and are ignored by scoring).
	Nodes map[string]*Node

	// Workloads indexes every InferenceService by namespace/name.
	Workloads map[types.NamespacedName]*Workload

	// Models indexes model availability by ModelKey for every model
	// referenced by at least one workload.
	Models map[ModelKey]*ModelAvailability

	// PendingPods are unscheduled GPU-requesting pods, real and virtual
	// (a virtual entry is a blocked evacuation fed back by the engine).
	PendingPods []PendingPod

	// OMENativeExecutor is the checked capability observation for the
	// cluster's OMENative executor. Its zero value is unavailable.
	OMENativeExecutor OMENativeExecutorState

	// OMENativeAvailable is retained temporarily for compile compatibility
	// with legacy policy and wiring code. New snapshot consumers must use
	// OMENativeExecutor; Task 4 removes this field.
	OMENativeAvailable bool
}

// OMENativeExecutorState is the structured executor capability observed for
// this snapshot.
type OMENativeExecutorState struct {
	Available   bool
	WireVersion string
	RenewTime   time.Time
	Reason      string
}

// Node is one node's physical GPU state plus the placement-relevant flags
// policies filter on.
type Node struct {
	Name string

	// Labels is the node's label set, retained because model readiness,
	// spot detection, and PVC topology all resolve against it.
	Labels map[string]string

	// GPUPool is the hardware-pool key scoring partitions on (free L4s
	// must never mask H100 fragmentation). Derived from the node itself
	// via GPUPoolForNode — GPU product label, instance shape, or the GPU
	// resource name — never from AcceleratorClass.Status.Nodes, which is
	// a selection surface that shape-scoped classes (H100x1..x8) can all
	// claim without partitioning hardware. Empty only on GPU-less nodes.
	GPUPool string

	// GPUResource is the extended resource name this node exposes
	// (e.g. nvidia.com/gpu); empty when the node has no GPU resource.
	GPUResource string

	// TotalGPUs is the node's allocatable GPU count.
	TotalGPUs int64
	// AllocatedGPUs is the sum of GPU requests of non-terminal pods
	// bound to this node (OME-managed or not).
	AllocatedGPUs int64
	// FreeGPUs = TotalGPUs - AllocatedGPUs, floored at zero.
	FreeGPUs int64
	// TerminatingGPUs is the share of AllocatedGPUs held by pods with a
	// deletion timestamp — capacity that is about to free but must not be
	// treated as free yet.
	TerminatingGPUs int64

	// Unhealthy is set when any configured trigger condition (default
	// GpuUnhealthy) is True on the node.
	Unhealthy bool
	// UnhealthyConditions lists which trigger conditions were True.
	UnhealthyConditions []string
	// Cordoned mirrors spec.unschedulable.
	Cordoned bool
	// ScaleDownDisabled mirrors the cluster-autoscaler
	// scale-down-disabled annotation.
	ScaleDownDisabled bool
	// ScaleDownMarked is set when the cluster-autoscaler has tainted the
	// node for deletion (ToBeDeletedByClusterAutoscaler).
	ScaleDownMarked bool
	// Preemptible is set when the node matches a configured
	// spot/preemptible label.
	Preemptible bool
	// Suspect is set by the engine for nodes inside their post-evacuation
	// suspicion window; such nodes are excluded from every policy's
	// target hints even after the health condition clears. The builder
	// always leaves it false.
	Suspect bool

	// OMEPods are the OME-managed GPU pods bound to this node.
	OMEPods []PodInfo
	// OtherOccupants are non-OME GPU pods (notebooks, batch jobs, ...):
	// they count against capacity but are never migration candidates.
	OtherOccupants []PodInfo
}

// PodInfo is the slice of pod state the snapshot keeps per GPU-consuming pod.
type PodInfo struct {
	Namespace string
	Name      string
	Node      string
	// GPUs is the pod's GPU request.
	GPUs int64
	// Ready reports the pod Ready condition.
	Ready bool
	// Terminating reports a non-nil deletion timestamp.
	Terminating bool
	// StartTime is pod.status.startTime when set (drives the
	// authorship-blind placement cooldown).
	StartTime *time.Time
	// ISVC is the owning InferenceService (zero for non-OME pods).
	ISVC types.NamespacedName
	// Component is the OME component label value (engine/decoder/router);
	// empty for non-OME pods.
	Component v1beta1.ComponentType
	// ManagedBy is the ome.io/managed-by label value.
	ManagedBy string

	// OMENative identity labels retain both parsed values and whether each
	// label was present and valid. This lets checked joins distinguish a
	// missing label from a valid zero without retaining arbitrary payloads.
	InstanceIndex        int32
	InstanceIndexPresent bool
	InstanceIndexValid   bool
	Incarnation          int64
	IncarnationPresent   bool
	IncarnationValid     bool
	Runner               v1beta1.RunnerName
	RunnerPresent        bool
	RunnerValid          bool
	PodOrdinal           int32
	PodOrdinalPresent    bool
	PodOrdinalValid      bool

	// ControllerOwnerUID is the sole controller OwnerReference UID when one
	// structurally valid reference is present.
	ControllerOwnerUID     types.UID
	ControllerOwnerPresent bool
	ControllerOwnerValid   bool
}

// Workload is one InferenceService with everything policies need to reason
// about moving it.
type Workload struct {
	NamespacedName types.NamespacedName

	// ISVC is the source object (read-only; shared with the informer
	// cache — never mutate).
	ISVC *v1beta1.InferenceService

	// Components maps component type to its resolved state; only
	// components present in the spec appear.
	Components map[v1beta1.ComponentType]*Component

	// ModelKey identifies the referenced model in Models; zero-valued
	// when the workload references no model.
	ModelKey ModelKey

	// Movable is the effective per-workload gate: the
	// alfred.ome.io/movable annotation when present, else the
	// cluster-wide default.
	Movable bool
	// Priority is alfred.ome.io/priority (default 0.5; lower = more
	// protected).
	Priority float64
	// CooldownOverride is alfred.ome.io/cooldown-minutes when set.
	CooldownOverride *time.Duration
	// TenantGroup is alfred.ome.io/tenant-group ("" = namespace-scoped).
	TenantGroup string
	// SpotPolicy is alfred.ome.io/spot-policy (avoid|migrate|ignore; ""
	// = cluster default).
	SpotPolicy string

	// LastMigration is the completion time of the newest terminal
	// authoritative InferenceReplica migration status (CompletedAt when set,
	// else StartedAt); it drives the per-workload cooldown.
	LastMigration *time.Time
	// ActiveMigrations are in-flight requests reconstructed from live request
	// annotations and non-terminal authoritative InferenceReplica statuses,
	// one per UUID.
	ActiveMigrations []InFlight
	// MalformedRequests maps request-annotation UUIDs that failed parse
	// or validation to the reason. The workload still carries a write the
	// executor must ack-reject, and the reporter can surface it; hiding
	// a corrupt request would make the workload look clean.
	MalformedRequests map[string]string
	// MigrationStateValid reports whether all bounded migration evidence was
	// internally consistent. False keeps the workload advisory/busy rather
	// than allowing incomplete status evidence to make it executable.
	MigrationStateValid bool
	// MigrationStateReason is a bounded, payload-free reason when migration
	// state is invalid.
	MigrationStateReason string
}

// Component is one component (engine/decoder/router) of a workload.
type Component struct {
	Type v1beta1.ComponentType
	// DeploymentMode is the resolved per-component mode (RawDeployment,
	// MultiNode, OMENative, ...), which determines the execution surface.
	DeploymentMode constants.DeploymentModeType
	// IR is the accepted read-only InferenceReplica source for an OMENative
	// component. It must never be mutated by snapshot consumers.
	IR *v1beta1.InferenceReplica
	// StatusFresh reports whether exactly one tuple-matching IR has the exact
	// parent controller identity and current observed generation.
	StatusFresh bool
	// ObservationValid reports structural agreement between the accepted IR
	// status and live Pods.
	ObservationValid bool
	// ObservationReason is a bounded, payload-free invalidity reason.
	ObservationReason string
	// Instances are the atomic units Alfred reasons about: one per pod
	// for RawDeployment; one per atomic group for multi-pod modes.
	Instances []*Instance
}

// Instance is the atomic migration unit: a single pod for RawDeployment, an
// atomic multi-pod group for MultiNode/OMENative.
type Instance struct {
	// Index is a stable ordinal within the component (pod-name order).
	Index int32
	// Incarnation and lifecycle fields are copied from checked IR status.
	Incarnation     int64
	Phase           v1beta1.OMENativeInstancePhase
	RunningRevision string
	TargetRevision  string
	Admitted        bool
	ActiveOrdinal   int32
	ServingPods     int32
	AvailablePods   int32
	Operation       *v1beta1.InstanceOperation
	DesiredPods     int32
	// StatusPods is the IR status row's reported PodCount.
	StatusPods int32
	// ObservedPods is the number of live Pods joined to this Instance.
	ObservedPods      int32
	ObservationValid  bool
	ObservationReason string
	// Pods are the member pods.
	Pods []PodInfo
	// NodesSet counts member pods per node.
	NodesSet map[string]int
	// TotalGPUs is the summed GPU request of all member pods — the
	// instance's surge footprint.
	TotalGPUs int64
	// ReadyPods counts members whose Ready condition is true.
	ReadyPods int32
}

// InFlight is one in-flight migration touching a workload, reconstructed
// from cluster state (request annotations and non-terminal InferenceReplica
// status entries) so it survives Alfred leader failover.
type InFlight struct {
	UUID      string
	Component v1beta1.ComponentType
	// Instance is the source Instance index.
	Instance int32
	// Mode is empty while the request is annotation-only (the executor
	// fixes the mode at admission).
	Mode v1beta1.MigrationMode
	// Phase is empty while the request is annotation-only.
	Phase       v1beta1.MigrationPhase
	FromNode    string
	RequestedAt time.Time
	RequestedBy string
}

// Model reference kinds for ModelKey.Kind.
const (
	ModelKindBaseModel        = "BaseModel"
	ModelKindClusterBaseModel = "ClusterBaseModel"
)

// ModelKey identifies a referenced model. Namespace is empty for
// ClusterBaseModel.
type ModelKey struct {
	Kind      string // ModelKindBaseModel | ModelKindClusterBaseModel
	Namespace string
	Name      string
}

// Zero reports whether the key is unset.
func (k ModelKey) Zero() bool { return k.Name == "" }

// Model storage backends.
const (
	// BackendPerNode models are downloaded per node by the model-agent;
	// readiness is per-node (node labels / Status.NodesReady).
	BackendPerNode = "PerNode"
	// BackendPVC models live on a PersistentVolume; there is no per-node
	// copy and readiness filtering must not apply — reachability is the
	// volume's CSI topology.
	BackendPVC = "PVC"
)

// ModelAvailability is the storage-aware answer to "where can this model's
// workloads run".
type ModelAvailability struct {
	Key     ModelKey
	Backend string

	// NodesReady lists nodes whose model-readiness label is Ready
	// (BackendPerNode only; derived from node labels, the same predicate
	// the scheduler enforces through the readiness nodeSelector).
	NodesReady []string

	// PVCAccessModes are the claim's access modes (BackendPVC only).
	PVCAccessModes []string
	// PVCTopologyNodes are the nodes satisfying the bound PV's node
	// affinity; nil means unconstrained (BackendPVC only).
	PVCTopologyNodes []string
	// VolumePinned is set for RWO/RWOP-backed models: the volume attaches
	// to one node at a time, so no surge-shaped mechanism can move the
	// workload — candidates degrade to advisory (VolumePinned).
	VolumePinned bool
	// ResolveError records why availability could not be resolved (e.g.
	// the model object or PVC is missing); policies treat the model as
	// having no feasible target and surface the reason.
	ResolveError string
}

// PendingPod is one unscheduled GPU demand, real or virtual.
type PendingPod struct {
	Namespace string
	Name      string
	// ISVC is the owning InferenceService (zero for non-OME pods).
	ISVC types.NamespacedName
	// GPUsNeeded is the pod's GPU request.
	GPUsNeeded int64
	// NodeSelector is the pod's nodeSelector, retained for pool
	// attribution.
	NodeSelector map[string]string
	// GPUPool is the demand's pool attribution; empty when the pod's
	// constraints match no single pool (counts toward every pool).
	GPUPool string
	// PendingSince is when the pod became unschedulable (drives
	// pending-pressure urgency).
	PendingSince time.Time
	// Virtual marks a blocked evacuation fed back by the engine rather
	// than a real pod.
	Virtual bool
}

// WithVirtualPending derives a snapshot with extra virtual pending pods
// appended. The receiver is not modified; maps and slices other than
// PendingPods are shared, which is safe because snapshots are read-only by
// contract.
func (s *ClusterSnapshot) WithVirtualPending(virtual []PendingPod) *ClusterSnapshot {
	if len(virtual) == 0 {
		return s
	}
	derived := *s
	derived.PendingPods = make([]PendingPod, 0, len(s.PendingPods)+len(virtual))
	derived.PendingPods = append(derived.PendingPods, s.PendingPods...)
	derived.PendingPods = append(derived.PendingPods, virtual...)
	return &derived
}

// PoolNodes returns the GPU-bearing nodes of one hardware pool, in map order
// (callers needing determinism sort the result).
func (s *ClusterSnapshot) PoolNodes(pool string) []*Node {
	var nodes []*Node
	for _, n := range s.Nodes {
		if n.GPUPool == pool && n.TotalGPUs > 0 {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// GPUPools returns the sorted set of hardware pools that have at least one
// GPU-bearing node.
func (s *ClusterSnapshot) GPUPools() []string {
	seen := map[string]struct{}{}
	var pools []string
	for _, n := range s.Nodes {
		if n.TotalGPUs == 0 {
			continue
		}
		if _, ok := seen[n.GPUPool]; !ok {
			seen[n.GPUPool] = struct{}{}
			pools = append(pools, n.GPUPool)
		}
	}
	sort.Strings(pools)
	return pools
}
