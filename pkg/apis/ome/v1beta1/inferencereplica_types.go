/*
InferenceReplica is the pod-set workload CRD behind OMENative: N Instances
of the same rendered pod templates, one revision history, one rollout.

A replica has one of two forms. An InferenceService projects one replica
per OMENative Component and is the only writer of that replica's spec
(the projected form). A user may also create a replica directly with
rendered runners and no parent (the standalone form); such a replica is
governed by RBAC. The validating webhook in
pkg/webhook/admission/inferencereplica enforces the ownership rules of
each form. The InferenceReplica controller is the only writer of status.
*/

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// InferenceReplica is one pod set: N Instances of the same rendered pod
// templates with one revision history and one rollout. A projected replica
// exists per (InferenceService, Component) tuple and is named
// <inferenceservice>-<component>; a standalone replica is named by its
// creator.
//
// The scale subresource lets HPA/KEDA target the InferenceReplica directly.
//
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.labelSelector
// +kubebuilder:resource:path=inferencereplicas,shortName=irep,singular=inferencereplica
// +kubebuilder:printcolumn:name="Component",type="string",JSONPath=".spec.component"
// +kubebuilder:printcolumn:name="Desired",type="integer",JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.replicas"
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="Available",type="integer",JSONPath=".status.availableReplicas"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +ome:since=v1.3
type InferenceReplica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InferenceReplicaSpec   `json:"spec,omitempty"`
	Status InferenceReplicaStatus `json:"status,omitempty"`
}

// InferenceReplicaList contains a list of InferenceReplica objects.
//
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type InferenceReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InferenceReplica `json:"items"`
}

// InferenceReplicaSpec is the desired state of one pod set. Its fields fall
// into three groups:
//
//   - User fields describe the pod set itself: component, set at create and
//     immutable; the template source (runners, modelRef and runtimeRef),
//     replicas, minReadySeconds, topologyKey, topologySpread,
//     topologySpreadKey, lifecycle, revisionHistoryLimit and autoscaler. The
//     owner of the replica writes them: the InferenceService controller for
//     a projected replica, the creating user for a standalone one. Exactly
//     one template source: runners, or modelRef and/or runtimeRef; the
//     validating webhook enforces it.
//   - Rollout-control fields steer a rollout across pod sets: pacing, paused,
//     pauseMode and pairingProtocol. The InferenceService controller writes
//     them on the replicas it orchestrates; on a standalone replica its owner
//     sets them.
//   - Controller-only fields carry state only the InferenceService controller
//     can know: parentRef, placementExecution and placementReplicaLimit.
type InferenceReplicaSpec struct {
	// PlacementExecution is allocation authority projected from a derived service.
	// Its generation must be observed before the control plane uses member evidence.
	// Controller-only.
	// +optional
	PlacementExecution *PlacementExecutionPolicy `json:"placementExecution,omitempty"`
	// PlacementReplicaLimit reserves the largest replica count this component
	// may request while placement pauses growth. Only a raised placement floor
	// can increase this limit; autoscaler requests remain in Replicas.
	// Controller-only.
	// +optional
	// +kubebuilder:validation:Minimum=1
	PlacementReplicaLimit *int32 `json:"placementReplicaLimit,omitempty"`

	// ParentRef names the InferenceService that projects this replica. The
	// InferenceService controller sets it at create time and it is immutable
	// thereafter; a standalone replica omits it. Pod, Service and
	// ControllerRevision names derive from the parent name when set and from
	// the replica's own name otherwise. Controller-only.
	// +optional
	ParentRef *ParentReference `json:"parentRef,omitempty"`

	// Component is the role this pod set fills: engine | decoder | router.
	// Immutable; moving a workload between roles requires recreating the
	// InferenceReplica.
	Component ComponentType `json:"component"`

	// ModelRef names the BaseModel (in the replica's namespace) or
	// ClusterBaseModel this replica serves. With it set the controller renders
	// the pods from the model and the runtime: the runtime named by RuntimeRef,
	// or the runtime selected for the model when RuntimeRef is absent. Fine-tuned
	// weights and overlays render as they do on an InferenceService. Exclusive
	// with Runners. User field.
	// +optional
	ModelRef *ModelRef `json:"modelRef,omitempty"`

	// RuntimeRef names the ServingRuntime (in the replica's namespace) or
	// ClusterServingRuntime whose piece for Component this replica renders.
	// The runtime must declare that piece. Without ModelRef the piece renders
	// as-is, with no model mounted. The runtime's pod spec places the pods
	// (node selector, affinity, resources); a second accelerator pool is a
	// second runtime. A pin (autoSync=false, revision) is not honored: the
	// live runtime renders, and the validating webhook rejects a pinned
	// reference. Exclusive with Runners. User field.
	// +optional
	RuntimeRef *ServingRuntimeRef `json:"runtimeRef,omitempty"`

	// Replicas is the desired Instance count; nil or 0 runs one Instance.
	// Whatever targets the scale subresource (HPA, KEDA or an external
	// scaler) writes this field. On a projected replica the InferenceService
	// controller writes the component's minReplicas (at least 1) at create,
	// and thereafter while no autoscaler owns the count or the stored count
	// is nil or 0.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// MinReadySeconds is the minimum time a newly Ready pod must stay Ready
	// before it counts as Available. The workload engine paces rollout
	// drains and promotions on Available pods and counts only Available pods
	// in availableReplicas; 0 means Available as soon as Ready. On a
	// projected replica the InferenceService controller copies the
	// component's effective lifecycle.minReadySeconds: the InferenceService's
	// value, else the runtime's, else the operator's deploy default.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinReadySeconds int32 `json:"minReadySeconds,omitempty"`

	// TopologyKey is the gang co-location node-label key for this Component.
	// When set on a multi-node Component, the InferenceReplica controller
	// auto-generates the per-Instance worker→leader podAffinity that
	// co-locates every worker onto its gang's leader on a node sharing this
	// label value. Nil means no auto-generated gang affinity. On a projected
	// replica the value is the effective InferenceService or runtime
	// component setting.
	// +optional
	TopologyKey *string `json:"topologyKey,omitempty"`

	// TopologySpread is the spreading policy for this Component. The
	// InferenceReplica controller renders it as a topologySpreadConstraint on
	// each Instance's anchor pod; nil keeps pure bin-packing. On a projected
	// replica the value is the effective InferenceService or runtime
	// component setting.
	// +optional
	// +kubebuilder:validation:Enum=Preferred;Required
	TopologySpread *TopologySpreadPolicy `json:"topologySpread,omitempty"`

	// TopologySpreadKey is the fault-domain node-label key TopologySpread
	// spreads across; nil defaults to TopologyKey. On a projected replica the
	// value is the effective InferenceService or runtime component setting.
	// +optional
	TopologySpreadKey *string `json:"topologySpreadKey,omitempty"`

	// PairingProtocol is the engine/decoder wire-compatibility token. It is
	// folded into the revision hash (a change mints a new revision) and
	// stamped as the ome.io/pairing-protocol label on rendered pods; nil pairs
	// with anything. Only engine and decoder replicas pair on it; a change on
	// any replica still mints a new revision. On a projected replica the
	// InferenceService controller copies spec.rollout.pairingProtocol onto
	// the engine and decoder replicas and never onto the router, which must
	// not re-roll on a protocol change.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`
	PairingProtocol *string `json:"pairingProtocol,omitempty"`

	// Runners is the fully-rendered set of pod templates per Instance. A
	// single-pod Instance has one Runner named "default" with Size=1; a
	// multi-node Instance has "leader" (Size=1) and "worker" (Size=N). Required
	// unless ModelRef or RuntimeRef is set, in which case it must be absent: the
	// controller renders the runners from the refs and never stores them. The
	// controller treats each Runner.Template as opaque input. User field.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	Runners []Runner `json:"runners,omitempty"`

	// Lifecycle holds the OMENative lifecycle policies (RestartPolicy,
	// UpdateStrategy, ReadyPolicy, InstanceReadyTimeout, MigrationPolicy).
	// An unset policy takes the fixed fallback its LifecycleSpec field
	// names; an unset or zero instanceReadyTimeout falls back to the
	// operator's lifecycle.instanceReadyTimeout on every replica, and with
	// neither set operations open with no deadline. The operator's deploy
	// defaults are applied only to a projected replica, before the copy.
	// lifecycle.minReadySeconds is not read on a replica and is rejected on
	// a standalone one; set spec.minReadySeconds. On a projected replica
	// this is the component's effective lifecycle: the InferenceService's
	// value, else the runtime's, with unset fields filled from the
	// operator's deploy defaults.
	// +optional
	Lifecycle *LifecycleSpec `json:"lifecycle,omitempty"`

	// Pacing is rollout control written onto this replica: the canary
	// partition, the rollback target and a reserved disruption budget. The
	// InferenceService controller writes it on the replicas it orchestrates;
	// on a standalone replica its owner sets it. Nil means the replica rolls
	// independently.
	// +optional
	Pacing *InferenceReplicaPacing `json:"pacing,omitempty"`

	// Autoscaler is the autoscaler configuration downstream scalers (HPA /
	// KEDA / External) target. Only the InferenceService controller creates
	// a scaler from it, so on a standalone replica the HPA and KEDA classes
	// are rejected and an External scaler targets the scale subresource
	// directly. On a projected replica the InferenceService controller
	// writes the resolved per-Component autoscaler, replacing the whole
	// block, whenever that autoscaler resolves; while an autoscaler policy
	// fails to render, the stored block is kept as the last known good one.
	// +optional
	Autoscaler *ComponentAutoscaler `json:"autoscaler,omitempty"`

	// Paused, when true, stops the InferenceReplica controller from
	// initiating fleet changes: Update, Create, and Migration
	// operations are neither started nor advanced. The component's
	// RestartPolicy keeps repairing existing Instances at their current
	// revision unless PauseMode is Freeze. In-flight operations resume
	// on unpause.
	// +optional
	Paused bool `json:"paused,omitempty"`

	// PauseMode selects how much of the lifecycle Paused suspends.
	// Only meaningful while Paused is true.
	//   - "Recover" (default when empty): fleet changes are suspended;
	//     the RestartPolicy continues to repair existing Instances at
	//     the revision they already run.
	//   - "Freeze": every lifecycle operation is suspended, including
	//     Instance repair. Only kubelet-level container restarts and
	//     deliberate replica reduction (scale-down) remain active.
	// Status stays truthful in every depth: a Ready Instance whose pods
	// are all gone (and whose RestartPolicy will not repair it) is
	// demoted to Pending without running any recovery operation.
	// +optional
	// +kubebuilder:validation:Enum=Recover;Freeze
	PauseMode PauseMode `json:"pauseMode,omitempty"`

	// RevisionHistoryLimit caps how many non-live ControllerRevisions the
	// InferenceReplica controller retains. Live revisions (CurrentRevision /
	// UpdateRevision and every per-Instance running/target revision) are never
	// deleted regardless of the limit. Nil falls back to the operator-level
	// lifecycle.revisionHistoryLimit config; when that is also absent, no
	// revisions are pruned. On a projected replica the InferenceService
	// controller copies the parent's ome.io/revision-history-limit annotation.
	// +optional
	// +kubebuilder:validation:Minimum=1
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty"`
}

// ParentReference holds the InferenceService owner reference info.
// Distinct from the OwnerReference list because the InferenceReplica
// controller composes the parent's name into per-IR label and Service
// names without re-reading the metadata field.
type ParentReference struct {
	// Name is the name of the parent InferenceService.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// ParentName returns the name of the InferenceService that projects this
// replica, or "" for a standalone replica.
func (ir *InferenceReplica) ParentName() string {
	if ir == nil || ir.Spec.ParentRef == nil {
		return ""
	}
	return ir.Spec.ParentRef.Name
}

// NamePrefix is the name every pod, Service and ControllerRevision of this
// replica derives from, and the value of the ome.io/inferenceservice label
// on its pods: the parent InferenceService's name when one is set,
// otherwise the replica's own name.
func (ir *InferenceReplica) NamePrefix() string {
	if ir == nil {
		return ""
	}
	if parent := ir.ParentName(); parent != "" {
		return parent
	}
	return ir.Name
}

// PauseMode is the enum of pause depths for InferenceReplicaSpec.Paused.
type PauseMode string

const (
	// PauseModeRecover suspends fleet changes while the RestartPolicy
	// keeps repairing existing Instances at their current revision.
	PauseModeRecover PauseMode = "Recover"
	// PauseModeFreeze suspends every lifecycle operation, including
	// Instance repair.
	PauseModeFreeze PauseMode = "Freeze"
)

// RunnerName is the enum of recognized Runner roles within an
// Instance. Matches the established Runner vocabulary so existing
// ome.io/runner label semantics carry through.
// +kubebuilder:validation:Enum=default;leader;worker
type RunnerName string

const (
	// RunnerNameDefault is the single-pod Instance Runner role.
	RunnerNameDefault RunnerName = "default"
	// RunnerNameLeader is the multi-node Instance leader Runner role.
	RunnerNameLeader RunnerName = "leader"
	// RunnerNameWorker is the multi-node Instance worker Runner role.
	RunnerNameWorker RunnerName = "worker"
)

// Runner is one logical pod role within an Instance.
//
// Single-pod Instances declare one Runner named "default" with Size=1.
// Multi-node Instances declare "leader" (Size=1) + "worker" (Size>=1).
//
// The revision hash includes every Runner's Template; a worker-only
// image bump still triggers a coordinated rollout across the whole
// Instance because tensor-parallel weights require leader+workers to
// move together.
type Runner struct {
	// Name is the Runner role within the Instance.
	Name RunnerName `json:"name"`

	// Size is the pod count for this Runner per Instance.
	// Constraints:
	//   - "default" Size MUST equal 1
	//   - "leader" Size MUST equal 1
	//   - "worker" Size MUST be >= 1
	// +kubebuilder:validation:Minimum=1
	Size int32 `json:"size"`

	// Template is the fully-rendered pod template for this Runner.
	// Labels include ome.io/runner=<name>. The InferenceReplica
	// controller stamps OME_RUNNER (leader|worker|default) and,
	// for non-leader pods, OME_LEADER_ADDRESS (short-form DNS,
	// matching LWS conventions) onto the rendered pod at create.
	Template corev1.PodTemplateSpec `json:"template"`
}

// InferenceReplicaPacing is the rollout control written onto one replica: the
// partition that holds Instances back, a reserved disruption budget, and the
// revision to roll back to.
type InferenceReplicaPacing struct {
	// Partition holds back updates: the Partition lowest-indexed Instances
	// not yet on the target revision, skipping any already updating to it,
	// keep their current revision. When set it takes precedence over
	// lifecycle.updateStrategy.rollingUpdate.partition; nil defers to it.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Partition *int32 `json:"partition,omitempty"`

	// MaxUnavailable is reserved; setting it has no effect. Disruption during
	// a rollout is paced by lifecycle.updateStrategy.rollingUpdate.maxUnavailable.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// RollbackToRevision, when set, names a ControllerRevision the replica
	// rolls every Instance back to: that revision's stored pod template
	// becomes the update target while UpdateRevision keeps reporting the
	// spec's own revision. The InferenceService controller sets it during a
	// canary rollback; empty in steady state. If the named revision does not
	// exist, the replica rolls to its spec. A revision this replica does not
	// control is ignored and reported with a Warning event.
	// +optional
	RollbackToRevision *string `json:"rollbackToRevision,omitempty"`
}

// InferenceReplicaStatus is the observed state of one
// InferenceReplica. The InferenceReplica controller is the sole
// writer. Fields mirror the LifecycleStatus shape so the ISVC's
// aggregated per-Component status is a verbatim projection of this
// block.
//
// The per-Instance rows have exactly one representation on a committed
// object. Without instanceStatusEncoding the rows are the DenseV1 list
// in instanceStatuses; with instanceStatusEncoding: ColumnarV2 the same
// rows live in instanceStatusColumns and instanceStatuses is absent.
// The rules below reject every mixed spelling of that union.
//
// +kubebuilder:validation:XValidation:rule="has(self.instanceStatusEncoding) || !has(self.instanceStatusColumns)",message="instanceStatusColumns requires instanceStatusEncoding; an unmarked status carries only instanceStatuses"
// +kubebuilder:validation:XValidation:rule="!has(self.instanceStatusEncoding) || (has(self.instanceStatusColumns) && !has(self.instanceStatuses))",message="instanceStatusEncoding: ColumnarV2 requires instanceStatusColumns and forbids instanceStatuses"
type InferenceReplicaStatus struct {
	// ObservedGeneration is the InferenceReplica.metadata.generation
	// the most recent status flush reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// PlacementObservedGeneration identifies the generation reconciled with
	// placement rollout and replica-growth guards. The control plane requires
	// this acknowledgement before sharing surge allowance with the member.
	// +optional
	// +kubebuilder:validation:Minimum=1
	PlacementObservedGeneration int64 `json:"placementObservedGeneration,omitempty"`

	// Replicas is the total Instance count (any phase).
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of Instances whose every pod is Ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ServingReplicas is the number of Instances whose every pod is
	// both ContainersReady AND has serving=True on the
	// controller-managed readiness gate (i.e., actually in the load
	// balancer rotation). MaxUnavailable budgets are computed
	// against this count, not ReadyReplicas, so "missing from
	// traffic" reflects reality during in-place updates.
	// +optional
	ServingReplicas int32 `json:"servingReplicas,omitempty"`

	// AvailableReplicas is the number of Instances whose desired pod
	// count is Available: in rotation on the Component's headless
	// Service and, when spec.minReadySeconds is set, Ready for at least
	// that long.
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// UpdatedReplicas is the number of Instances on UpdateRevision.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// UpdatedReadyReplicas is the number of Instances on
	// UpdateRevision that are fully Ready. UpdatedReadyReplicas /
	// Replicas drives MaxUnavailable rollout pacing.
	// +optional
	UpdatedReadyReplicas int32 `json:"updatedReadyReplicas,omitempty"`

	// CurrentRevision names the ControllerRevision currently serving
	// traffic. Set by the InferenceReplica controller.
	// +optional
	CurrentRevision string `json:"currentRevision,omitempty"`

	// UpdateRevision names the ControllerRevision being rolled out.
	// +optional
	UpdateRevision string `json:"updateRevision,omitempty"`

	// CollisionCount is the salt folded into ControllerRevision hash
	// input after a name collision is observed, so retries produce a
	// different revision name. Same pattern as StatefulSet.
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty"`

	// LabelSelector identifies pods of this InferenceReplica for the
	// HPA scale subresource. Uses the same selector formula as the
	// Component's pod labels, so HPAs targeting either the ISVC or the
	// InferenceReplica resolve to the same pod set.
	// +optional
	LabelSelector string `json:"labelSelector,omitempty"`

	// InstanceStatuses is the DenseV1 representation of the per-Instance
	// status, one row per Instance. Absent when instanceStatusEncoding is
	// ColumnarV2, in which case instanceStatusColumns carries the same rows.
	// +optional
	// +listType=map
	// +listMapKey=index
	InstanceStatuses []OMENativeInstanceStatus `json:"instanceStatuses,omitempty"`

	// InstanceStatusEncoding names the representation that carries the
	// per-Instance rows. Absent means DenseV1 (instanceStatuses). The only
	// accepted value is ColumnarV2, which requires instanceStatusColumns
	// and forbids instanceStatuses.
	// +optional
	InstanceStatusEncoding *InstanceStatusEncoding `json:"instanceStatusEncoding,omitempty"`

	// InstanceStatusColumns is the ColumnarV2 representation of the
	// per-Instance rows. Present exactly when instanceStatusEncoding is
	// ColumnarV2; it decodes to the same rows, in the same order, that
	// instanceStatuses would carry.
	// +optional
	InstanceStatusColumns *InstanceStatusColumns `json:"instanceStatusColumns,omitempty"`

	// RetryBlocks records per-target-revision retry authority for update
	// attempts. Revision-scoped: one block per failed target revision,
	// shared by every Instance attempting it — allocating a new Instance
	// cannot evade the block.
	// +optional
	// +listType=map
	// +listMapKey=targetRevision
	RetryBlocks []RetryBlock `json:"retryBlocks,omitempty"`

	// Migrations is the authoritative set of migration records for
	// this InferenceReplica — one entry per request UUID. Manual
	// entries are resumable processes born Accepted; Auto entries are
	// born-terminal Relocated records. Executors select on
	// non-terminal phase only — terminal entries are records, never
	// work. Non-terminal entries are never trimmed; terminal entries
	// are pruned once older than the migration-capacity rate window,
	// so the list stays bounded. Full history remains in the audit
	// ledger and events.
	// +optional
	// +listType=map
	// +listMapKey=requestUUID
	// +patchStrategy=merge
	// +patchMergeKey=requestUUID
	Migrations []MigrationStatus `json:"migrations,omitempty" patchStrategy:"merge" patchMergeKey:"requestUUID"`

	// Conditions reports InferenceReplica-level conditions. Defined
	// types include Available, Progressing, FailedScale,
	// FailedUpdate, GangSchedulingUnavailable, OperationStuck.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Traffic reports per-revision traffic weights for this
	// InferenceReplica. Each entry corresponds to one revision
	// currently receiving traffic. The ISVC controller mirrors this
	// onto ISVC.status.components[c].traffic[] for the
	// HTTPRoute builder.
	// +optional
	// +listType=map
	// +listMapKey=revisionName
	Traffic []ComponentTrafficTarget `json:"traffic,omitempty"`

	// CoordinationGroupRef back-references the
	// RolloutCoordinationGroup this replica is currently bound into
	// (if any). Lets operators find the owning coord group from the
	// child without parsing the ISVC spec.
	// +optional
	CoordinationGroupRef string `json:"coordinationGroupRef,omitempty"`

	// RolloutHold records the most recent reason a per-Instance Update was
	// denied for this Component — cross-Component coordination (Ratio,
	// Sequential), a surge/unavailability budget, or a same-target retry
	// hold (RetryBlock, Held). Nil while the Component is progressing or
	// has no rollout in flight. See RolloutHold for the field semantics.
	// +optional
	RolloutHold *RolloutHold `json:"rolloutHold,omitempty"`
}

// InstanceStatusEncoding names a stored representation of the per-Instance
// rows. The wire carries only ColumnarV2; an absent marker means DenseV1.
// +kubebuilder:validation:Enum=ColumnarV2
type InstanceStatusEncoding string

const (
	// InstanceStatusEncodingColumnarV2 stores the per-Instance rows in
	// instanceStatusColumns instead of the dense instanceStatuses list.
	InstanceStatusEncodingColumnarV2 InstanceStatusEncoding = "ColumnarV2"
)

// InstanceStatusColumns is the ColumnarV2 representation of the per-Instance
// rows. A value shared by many Instances is stored once against the index
// set of the Instances that carry it; records that are rare or unique per
// Instance stay keyed by index in entries.
//
// An index set is a string of comma-separated decimal terms, each a single
// index or a closed "first-last" range, for example "0-8,10-114,116". The
// canonical form is the only accepted one: terms are strictly ascending,
// disjoint, and nonadjacent (adjacent terms are merged), a range has
// first < last, indices are nonnegative int32 values without leading zeros,
// and there is no whitespace, sign, empty term, or trailing comma.
//
// Members is the complete row set; every other index set is a subset of it.
// Within one column the group index sets are pairwise disjoint and the groups
// are in canonical value order: bytewise for phases and revisions, signed
// numeric for incarnations and counts. A member absent from every group of
// an optional column takes that column's documented default.
type InstanceStatusColumns struct {
	// Members is the index set of every Instance that has a row.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$`
	Members string `json:"members"`

	// RowOrder is the dense row order when it is not the ascending Members
	// order: every member exactly once, in the order the DenseV1 list would
	// carry. Absent means ascending Members order.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:items:Minimum=0
	// +listType=atomic
	RowOrder []int32 `json:"rowOrder,omitempty"`

	// Phases groups Instances by lifecycle phase. Every member appears in
	// exactly one group; there is no default phase.
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Phases []InstanceStatusPhaseGroup `json:"phases"`

	// RunningRevisions groups Instances by nonempty running revision. A
	// member absent from every group has an empty running revision.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	RunningRevisions []InstanceStatusRevisionGroup `json:"runningRevisions,omitempty"`

	// TargetRevisions groups Instances by nonempty target revision. A
	// member absent from every group has an empty target revision.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	TargetRevisions []InstanceStatusRevisionGroup `json:"targetRevisions,omitempty"`

	// Incarnations groups Instances by nonzero incarnation. A member absent
	// from every group has incarnation 0.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Incarnations []InstanceStatusIncarnationGroup `json:"incarnations,omitempty"`

	// PodCounts groups Instances by positive podCount. A member absent
	// from every group has podCount 0.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	PodCounts []InstanceStatusCountGroup `json:"podCounts,omitempty"`

	// ServingPodCounts groups Instances by positive servingPodCount. A
	// member absent from every group has servingPodCount 0.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	ServingPodCounts []InstanceStatusCountGroup `json:"servingPodCounts,omitempty"`

	// AvailablePodCounts groups Instances by positive availablePodCount. A
	// member absent from every group has availablePodCount 0.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	AvailablePodCounts []InstanceStatusCountGroup `json:"availablePodCounts,omitempty"`

	// Admitted is the index set of Instances whose admitted is true. Absent
	// means no Instance is admitted.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$`
	Admitted *string `json:"admitted,omitempty"`

	// ActiveOrdinalOne is the index set of Instances whose
	// activeOrdinal is 1. Absent means every activeOrdinal is 0.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$`
	ActiveOrdinalOne *string `json:"activeOrdinalOne,omitempty"`

	// Entries carries the per-Instance records that are not grouped:
	// conditions, readySince, operation, and lastFailure. At most one entry
	// per member, in ascending index order, each carrying at least one of
	// those records. A member without an entry has none of them.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Entries []InstanceStatusEntry `json:"entries,omitempty"`
}

// InstanceStatusPhaseGroup is one phase and the Instances that carry it.
type InstanceStatusPhaseGroup struct {
	// Value is the shared lifecycle phase.
	Value OMENativeInstancePhase `json:"value"`

	// Indexes is the canonical index set of the Instances with this value.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$`
	Indexes string `json:"indexes"`
}

// InstanceStatusRevisionGroup is one full ControllerRevision name and the
// Instances that carry it.
type InstanceStatusRevisionGroup struct {
	// Value is the shared revision name; the empty revision is never grouped.
	// +kubebuilder:validation:MinLength=1
	Value string `json:"value"`

	// Indexes is the canonical index set of the Instances with this value.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$`
	Indexes string `json:"indexes"`
}

// InstanceStatusIncarnationGroup is one incarnation and the Instances that
// carry it. The value keeps the int64 domain of the dense row, including
// negative values; zero is the absent default and is never grouped.
// +kubebuilder:validation:XValidation:rule="self.value != 0",message="an incarnation group value must be nonzero; zero is the absent default"
type InstanceStatusIncarnationGroup struct {
	// Value is the shared nonzero incarnation.
	Value int64 `json:"value"`

	// Indexes is the canonical index set of the Instances with this value.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$`
	Indexes string `json:"indexes"`
}

// InstanceStatusCountGroup is one positive pod count and the Instances that
// carry it. Zero is the absent default and is never grouped.
type InstanceStatusCountGroup struct {
	// Value is the shared positive count.
	// +kubebuilder:validation:Minimum=1
	Value int32 `json:"value"`

	// Indexes is the canonical index set of the Instances with this value.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$`
	Indexes string `json:"indexes"`
}

// InstanceStatusEntry carries the records of one Instance that are not
// grouped into columns. Each record is the dense row's value, unchanged.
// +kubebuilder:validation:XValidation:rule="has(self.conditions) || has(self.readySince) || has(self.operation) || has(self.lastFailure) || has(self.announced)",message="an entry must carry at least one of conditions, readySince, operation, lastFailure, or announced"
type InstanceStatusEntry struct {
	// Index is the Instance this entry belongs to; it is a member.
	// +kubebuilder:validation:Minimum=0
	Index int32 `json:"index"`

	// Conditions is the Instance's conditions.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ReadySince is the Instance's readySince.
	// +optional
	ReadySince *metav1.Time `json:"readySince,omitempty"`

	// Operation is the Instance's in-flight operation record.
	// +optional
	Operation *InstanceOperation `json:"operation,omitempty"`

	// LastFailure is the Instance's preserved failure diagnostics.
	// +optional
	LastFailure *InstanceTermination `json:"lastFailure,omitempty"`

	// Announced is the Instance's record of once-only messages already
	// delivered.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	Announced []string `json:"announced,omitempty"`
}

// RetryBlockState is the retry authority state for one target revision.
// +kubebuilder:validation:Enum=Backoff;Held;RetryInProgress
type RetryBlockState string

const (
	// RetryBlockBackoff: attempts remain; the next one is authorized at
	// NextRetryAt.
	RetryBlockBackoff RetryBlockState = "Backoff"
	// RetryBlockHeld: attempts exhausted (or retry unconfigured). Only a
	// new target revision releases the hold.
	RetryBlockHeld RetryBlockState = "Held"
	// RetryBlockRetryInProgress: exactly one retry attempt is authorized
	// and running.
	RetryBlockRetryInProgress RetryBlockState = "RetryInProgress"
)

// RetryBlock is the persisted per-revision retry authority. The
// subject is the owning IR plus TargetRevision.
type RetryBlock struct {
	// TargetRevision is the ControllerRevision name this block scopes.
	TargetRevision string `json:"targetRevision"`

	State RetryBlockState `json:"state"`

	// AttemptsStarted counts lifecycle attempts at this revision.
	// Kubelet container restarts are diagnostic and not counted here.
	AttemptsStarted int32 `json:"attemptsStarted"`

	// NextRetryAt is the persisted resume time while State=Backoff.
	// Never recomputed from process-local history (restart-safe).
	// +optional
	NextRetryAt *metav1.Time `json:"nextRetryAt,omitempty"`

	// +optional
	FirstFailureAt *metav1.Time `json:"firstFailureAt,omitempty"`
	// +optional
	LastFailureAt *metav1.Time `json:"lastFailureAt,omitempty"`

	// Reason is the operator-facing last-failure summary.
	// +optional
	Reason string `json:"reason,omitempty"`
}

// MigrationTrigger identifies who initiated a migration entry.
// +kubebuilder:validation:Enum=Manual;Auto
type MigrationTrigger string

const (
	// MigrationTriggerManual marks an operator-requested migration.
	// Manual entries are resumable processes: born Accepted, they
	// walk the phase chain to a terminal phase.
	MigrationTriggerManual MigrationTrigger = "Manual"
	// MigrationTriggerAuto marks a controller-initiated relocation.
	// Auto entries are born-terminal Relocated records — visibility
	// mirrors of a relocation directive, never resumable work.
	MigrationTriggerAuto MigrationTrigger = "Auto"
)

// MigrationPhase is the lifecycle phase of a migration.
//
// Manual migrations walk Accepted -> SurgePending -> SurgeReady ->
// Draining -> Completed | Failed. Auto relocations are born terminal
// (Relocated). Terminal phases are Completed, Failed, and Relocated —
// executors select work on non-terminal phase only; terminal entries
// are records, never work.
//
// Pending and InProgress are legacy values used only by the ISVC
// status migrationHistory window.
// +kubebuilder:validation:Enum=Accepted;SurgePending;SurgeReady;Draining;Completed;Failed;Relocated;Pending;InProgress
type MigrationPhase string

const (
	// MigrationPhaseAccepted: the request was validated and admitted;
	// no surge Instance is allocated yet.
	MigrationPhaseAccepted MigrationPhase = "Accepted"
	// MigrationPhaseSurgePending: the surge Instance is allocated and
	// waiting to become Ready.
	MigrationPhaseSurgePending MigrationPhase = "SurgePending"
	// MigrationPhaseSurgeReady: the surge Instance is Ready; the
	// source may begin draining.
	MigrationPhaseSurgeReady MigrationPhase = "SurgeReady"
	// MigrationPhaseDraining: the source Instance is draining out of
	// rotation.
	MigrationPhaseDraining MigrationPhase = "Draining"
	// MigrationPhaseCompleted: terminal — the migration succeeded.
	MigrationPhaseCompleted MigrationPhase = "Completed"
	// MigrationPhaseFailed: terminal — the migration did not complete.
	MigrationPhaseFailed MigrationPhase = "Failed"
	// MigrationPhaseRelocated: terminal — a born-terminal Auto
	// relocation record.
	MigrationPhaseRelocated MigrationPhase = "Relocated"

	// MigrationPhasePending is a legacy migrationHistory-only value.
	MigrationPhasePending MigrationPhase = "Pending"
	// MigrationPhaseInProgress is a legacy migrationHistory-only value.
	MigrationPhaseInProgress MigrationPhase = "InProgress"
)

// Terminal reports whether p is a terminal phase (Completed, Failed,
// or Relocated). Executors select migrations on non-terminal phase
// only — terminal entries are records, never work.
func (p MigrationPhase) Terminal() bool {
	switch p {
	case MigrationPhaseCompleted, MigrationPhaseFailed, MigrationPhaseRelocated:
		return true
	}
	return false
}

// MigrationStatus is one migration request's authoritative record on
// the InferenceReplica. status.migrations is the single source of
// truth for migration work: executors resume from non-terminal
// entries; the audit ledger and events are history, not work.
//
// Lifecycle contract:
//   - Manual entries (Trigger=Manual) are resumable processes: born
//     Accepted, they walk Accepted -> SurgePending -> SurgeReady ->
//     Draining -> Completed | Failed.
//   - Auto entries (Trigger=Auto) are born-terminal Relocated records
//     mirroring a relocation directive; Succeeded is stamped post-hoc
//     when the relocated Instance next reaches Ready.
//   - Executors select on non-terminal phase only — terminal entries
//     are records, never work.
type MigrationStatus struct {
	// RequestUUID uniquely identifies the migration request. Manual:
	// supplied by the requester. Auto: generated by the controller.
	RequestUUID string `json:"requestUUID"`

	// Trigger records who initiated the migration.
	Trigger MigrationTrigger `json:"trigger"`

	// SourceInstance is the Instance index being migrated away from.
	SourceInstance int32 `json:"sourceInstance"`

	// SurgeInstance is the Instance index allocated for the surge
	// replacement. Manual only; nil until allocated. Distinct from
	// index 0, which is a valid surge index.
	// +optional
	SurgeInstance *int32 `json:"surgeInstance,omitempty"`

	// AllocatedAt is when the surge Instance index was allocated —
	// the start of execution. Nil while the entry is queued (Accepted
	// but not yet picked up by the executor). The capacity gate counts
	// execution from this stamp, never queued intent.
	// +optional
	AllocatedAt *metav1.Time `json:"allocatedAt,omitempty"`

	// FromNode is the node the source is being moved off.
	// Auto: the excluded node.
	// +optional
	FromNode string `json:"fromNode,omitempty"`

	// HintTargetNodes are requester-supplied preferred placement
	// targets for the surge replacement. Rendered as preferred (soft)
	// node affinity; the scheduler may pick other nodes.
	// +optional
	// +listType=atomic
	HintTargetNodes []string `json:"hintTargetNodes,omitempty"`

	// Phase is the migration lifecycle phase. See MigrationPhase for
	// the Manual chain, the Auto born-terminal rule, and the terminal
	// set.
	Phase MigrationPhase `json:"phase"`

	// Attempt is the relocation attempt ordinal.
	// Auto: N of autoMigrate.maxAttempts.
	// +optional
	Attempt int32 `json:"attempt,omitempty"`

	// Reason is the requester-supplied reason (Manual) or the
	// disposition branch that produced the entry (Auto).
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message describes the current blocker (non-terminal) or the
	// terminal outcome.
	// +optional
	Message string `json:"message,omitempty"`

	// StartedAt is when the entry was created.
	StartedAt metav1.Time `json:"startedAt"`

	// Deadline is when a non-terminal entry expires.
	// Manual: accept time plus InstanceReadyTimeout.
	Deadline metav1.Time `json:"deadline"`

	// CompletedAt is when the entry reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Succeeded reports the post-hoc outcome. Auto: stamped true when
	// the relocated Instance next reaches Ready.
	// +optional
	Succeeded *bool `json:"succeeded,omitempty"`
}

func init() {
	SchemeBuilder.Register(&InferenceReplica{}, &InferenceReplicaList{})
}
