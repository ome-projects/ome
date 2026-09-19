// Package types holds the workload package's value types — the
// per-Instance / per-Component / per-Plan data structures every layer
// of the workload pipeline reads and writes. Lives separately from
// the parent `workload` package so `workload/ops` and
// `workload.Reconcile` can both depend on these types without closing
// an import cycle.
//
// The workload package owns its own status / operation / component /
// phase / key types and does NOT depend on the OME CRD API package.
// Every type here mirrors the corresponding v1beta1 status type
// field-for-field; the v1beta1convert helpers
// (`controller/v1beta1/v1beta1convert`) perform the conversion, and
// the InferenceReplica adapter (inferencereplica) wires those
// converters into its reconcile loop.
//
// External callers reference these types as `workload.X` — the parent
// `workload` package re-exports them as Go aliases.
package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ComponentType identifies router | engine | decoder. String-mirrors
// the CRD ComponentType so the adapter conversion is a verbatim cast;
// the distinct Go type catches accidental cross-package coupling.
type ComponentType string

const (
	ComponentRouter  ComponentType = "router"
	ComponentEngine  ComponentType = "engine"
	ComponentDecoder ComponentType = "decoder"
)

// InstancePhase is the per-Instance lifecycle phase. String-mirrors
// the CRD OMENativeInstancePhase.
type InstancePhase string

const (
	// InstancePhaseEmpty is the zero value used before the first
	// observation lands on a freshly-allocated InstanceStatus.
	InstancePhaseEmpty      InstancePhase = ""
	InstancePhasePending    InstancePhase = "Pending"
	InstancePhaseCreating   InstancePhase = "Creating"
	InstancePhaseReady      InstancePhase = "Ready"
	InstancePhaseUpdating   InstancePhase = "Updating"
	InstancePhaseRestarting InstancePhase = "Restarting"
	InstancePhaseMigrating  InstancePhase = "Migrating"
	InstancePhaseFailed     InstancePhase = "Failed"
	InstancePhaseDeleting   InstancePhase = "Deleting"
)

// UpdateStrategyType is the rollout-mechanism selector. String-mirrors
// the CRD UpdateStrategyType; the distinct Go type catches accidental
// drift between workload-side dispatch logic and the v1beta1 enum.
type UpdateStrategyType string

const (
	// UpdateStrategySurgeThenDrain creates a new pod for the target
	// revision, waits for it to serve, then drains + deletes the old.
	UpdateStrategySurgeThenDrain UpdateStrategyType = "SurgeThenDrain"
	// UpdateStrategyRecreatePod always deletes pods and recreates them
	// at a bumped Incarnation.
	UpdateStrategyRecreatePod UpdateStrategyType = "RecreatePod"
	// UpdateStrategyInPlaceIfPossible tries image-patch in place; falls
	// through to recreate on non-image-only diffs.
	UpdateStrategyInPlaceIfPossible UpdateStrategyType = "InPlaceIfPossible"
	// UpdateStrategyInPlaceOnly image-patches in place when eligible;
	// errors otherwise.
	UpdateStrategyInPlaceOnly UpdateStrategyType = "InPlaceOnly"
)

// RestartPolicy is the Instance-group restart policy. String-mirrors
// the CRD InstanceRestartPolicy.
type RestartPolicy string

const (
	// RestartPolicyNone restarts only the failed pod; OMENative leaves
	// the Instance Phase=Ready and lets the kubelet recover in place.
	RestartPolicyNone RestartPolicy = "None"
	// RestartPolicyRecreateInstance drains, deletes, and recreates
	// every pod in the Instance at a bumped Incarnation.
	RestartPolicyRecreateInstance RestartPolicy = "RecreateInstanceOnPodRestart"
)

// MigrationMode is the migration disposition. String-mirrors the CRD
// MigrationPolicyMode.
type MigrationMode string

const (
	// MigrationModeAuto / MigrationModeSurge — operator may trigger
	// migration via the per-UUID annotation; controller drives the
	// surge cycle. Surge is a spelling alias for Auto.
	MigrationModeAuto  MigrationMode = "Auto"
	MigrationModeSurge MigrationMode = "Surge"
	// MigrationModeNever — controller refuses any migration;
	// annotations are ignored.
	MigrationModeNever MigrationMode = "Never"
)

// InstanceOperationType identifies the kind of an in-flight
// destructive operation against one Instance. String-mirrors the CRD
// InstanceOperationType.
type InstanceOperationType string

const (
	InstanceOperationCreate  InstanceOperationType = "Create"
	InstanceOperationUpdate  InstanceOperationType = "Update"
	InstanceOperationRestart InstanceOperationType = "Restart"
	InstanceOperationMigrate InstanceOperationType = "Migrate"
	InstanceOperationDelete  InstanceOperationType = "Delete"
)

// UpdateStepSurge marks the source Instance while a single-pod or gang
// SurgeThenDrain replacement is being created.
const UpdateStepSurge = "Surge"

// UpdateStepGangSurgeTarget is the InstanceOperation.Step marking the
// surge-target (replacement) Instance of an in-flight multi-pod (gang)
// SurgeThenDrain update. Lives here in the leaf types package so both the
// plan (root workload) and the ops dispatcher reference one canonical
// value. The gang SOURCE uses Step="Surge" so the surge budget counts it.
const UpdateStepGangSurgeTarget = "GangSurgeTarget"

// UpdateStepGangSurgeTargetCleanup marks a replacement gang whose terminal
// cleanup owns its Pods and PodGroup until the marker status is removed.
const UpdateStepGangSurgeTargetCleanup = "GangSurgeTargetCleanup"

// UpdateStepSurgeDrain marks a SurgeThenDrain source whose replacement is
// ready and whose source pods are being drained.
const UpdateStepSurgeDrain = "SurgeDrain"

// InstanceOperation is the durable recovery anchor written before any
// destructive action against an Instance and cleared on completion.
// Mirrors the CRD InstanceOperation field-for-field.
type InstanceOperation struct {
	// ID is the idempotency key. Retries with the same ID are a no-op.
	ID string

	Type InstanceOperationType

	// Step is the fine-grained resume point within the operation
	// (e.g., Drain, DeletePods, WaitZero, Recreate, WaitReady).
	Step string

	StartedAt metav1.Time
	// LastProgressAt is when the operation last advanced. Used for
	// stall detection.
	LastProgressAt metav1.Time
	// Deadline is the hard timeout for the current Step.
	Deadline metav1.Time
	// RetryCount is the per-step escalation counter.
	RetryCount int32

	// TargetRevision is the ControllerRevision the operation is
	// converging toward.
	TargetRevision string

	// Reason names WHY the operation exists (restart trigger,
	// revision-roll cause). Written once at the start by the owning state
	// machine and part of the terminal-finalize identity tuple, so it
	// must not be reused for transient annotations.
	Reason string

	// Waiting names an EXTERNAL condition currently holding the operation
	// back, as a short token (see RejectionReason*). It comes and goes
	// while the operation runs — deliberately NOT part of the
	// terminal-finalize identity tuple, or a blip would read as the
	// operation being replaced.
	Waiting string

	// Migrate-only fields. The Operation is a pin: SurgeIndex correlates
	// the source/surge pair and RequestUUID keys the authoritative
	// status.migrations record.
	SurgeIndex  *int32
	RequestUUID string

	// FromNode / HintTargetNodes are never stamped (migration facts live
	// on the owner's status.migrations record) and have no readers; they
	// exist only so the CRD InstanceOperation mirror round-trips
	// already-stamped values unchanged. Do not add writers.
	FromNode        string
	HintTargetNodes []string
}

// InstanceStatus is the per-Instance status. Mirrors the CRD
// OMENativeInstanceStatus field-for-field; adapters round-trip between
// this type and the CRD type.
type InstanceStatus struct {
	// Index is the stable Instance ordinal. Values may be sparse
	// after surge migration.
	Index int32

	// Incarnation increments each time the Instance is recreated
	// (full recreate update, restart-on-loss). In-place updates do
	// not increment.
	Incarnation int64

	Phase InstancePhase

	// RunningRevision / TargetRevision are the ControllerRevision the
	// live pods are running and converging toward, respectively.
	RunningRevision string
	TargetRevision  string

	PodCount int32
	// ReadyPodCount counts pods reporting Ready=True.
	ReadyPodCount int32
	// ServingPodCount counts pods that are BOTH ContainersReady AND
	// have serving=True on the controller-managed readiness gate.
	ServingPodCount int32
	// AvailablePodCount counts pods in rotation on the Component's
	// headless Service that, when MinReadySeconds is set, have been Ready
	// for at least that long.
	AvailablePodCount int32
	// ScheduledPodCount counts pods with Spec.NodeName set.
	ScheduledPodCount int32
	// Admitted reports that the Instance has pods and none remain behind an
	// admission scheduling gate.
	Admitted bool

	// NodesOccupied is the set of node names hosting pods of this
	// Instance.
	NodesOccupied []string

	Conditions []metav1.Condition

	// ReadySince is when the Instance last entered Ready. It anchors
	// post-Ready failure detection: container restarts that finished
	// before this time belong to boot and are never restart triggers.
	ReadySince *metav1.Time

	// Operation is the durable record of an in-flight destructive
	// action against this Instance. Set before the action starts,
	// cleared after it completes.
	Operation *InstanceOperation

	// ActiveOrdinal identifies which of two pod-naming slots (0 or 1)
	// currently holds the canonical pod for this single-pod Instance.
	ActiveOrdinal int32

	// LastFailure preserves the container-termination diagnostics of the
	// pod whose failure (or stuck-pod escalation) last triggered a
	// recreate of this Instance. Survives the drain+recreate that deletes
	// the failing pod. Mirrors the CRD OMENativeInstanceStatus.LastFailure.
	LastFailure *InstanceTermination
}

// InstanceTermination captures the container-termination diagnostics of a
// pod that failed or was escalated. Mirrors the CRD InstanceTermination
// field-for-field; the v1beta1convert helpers round-trip it at the adapter
// boundary.
type InstanceTermination struct {
	// PodName is the name of the pod that failed or was escalated.
	PodName string

	// ContainerName is the container the diagnostics were read from.
	// Empty when only a pod-level signal was available.
	ContainerName string

	// Reason is the kubelet terminated- or waiting-state reason
	// (OOMKilled, Error, CrashLoopBackOff, ImagePullBackOff, ...), or
	// "PodFailed" when only the pod phase was available.
	Reason string

	// ExitCode is the container exit code when the signal came from a
	// terminated state; nil for a stuck waiting-state signal.
	ExitCode *int32

	// Message is the kubelet-supplied detail message, if any.
	Message string

	// Time is when OMENative recorded this termination.
	Time metav1.Time
}

// Key uniquely identifies one logical workload (a per-Component slice
// of an owner). Adapters populate every field; workload code reads
// them opaquely.
type Key struct {
	Namespace string
	Component ComponentType

	// OwnerName is the bare name of the workload's owner object
	// (ISVC.Name or IR.Name). workload composes pod / Service names
	// from this — never reaches back into OwnerObject for the field.
	OwnerName string

	// OwnerLabels is the seed label set stamped on every emitted
	// ControllerRevision / PodGroup / pod's metadata labels block.
	// Read-only from workload code.
	OwnerLabels map[string]string

	// SelectorLabels is the seed label set used in every pod LIST
	// selector and per-revision Service selector. Read-only from
	// workload code.
	SelectorLabels map[string]string
}

// WorkloadName is the composed identity used for ControllerRevision
// naming and as the prefix on emitted pod / PodGroup / PDB names.
// Always "<OwnerName>-<Component>" — the pod-naming convention every
// adapter shares so existing selectors keep matching.
func (k Key) WorkloadName() string {
	return k.OwnerName + "-" + string(k.Component)
}

// Lifecycle is the workload-owned mirror of the v1beta1 LifecycleSpec.
// Adapters project the CRD-shape lifecycle into this struct so the
// workload package can read it without importing v1beta1. Fields
// match the CRD field-for-field; nil pointers signal "unset, default
// at planning time".
type Lifecycle struct {
	// RestartPolicy is the per-Instance restart disposition. nil falls
	// back to the per-shape default (multi-pod → RecreateInstance,
	// single-pod → None) in BuildPlan.
	RestartPolicy *RestartPolicy

	// UpdateStrategy controls how OMENative rolls template changes
	// across the Component's Instances. nil falls back to defaults.
	UpdateStrategy *UpdateStrategy

	// ReadyPolicy controls how Instance-level readiness is aggregated
	// from the underlying pods. nil falls back to the per-shape default.
	ReadyPolicy *InstanceReadyPolicy

	// InstanceReadyTimeout is the wait ceiling on a newly-created
	// Instance becoming Ready. nil falls back to the 30m default.
	InstanceReadyTimeout *metav1.Duration

	// MigrationPolicy controls whether and how OMENative honors a
	// migration request annotation for this Component. nil falls back
	// to the Auto default.
	MigrationPolicy *MigrationPolicy
}

// UpdateStrategy is the workload-owned mirror of the v1beta1
// UpdateStrategy struct. Adapters project the CRD-shape strategy onto
// this struct so the workload package can read it without importing
// v1beta1.
type UpdateStrategy struct {
	// Type selects the rollout mechanism. Empty defaults to
	// SurgeThenDrain in BuildPlan.
	Type UpdateStrategyType

	// RollingUpdate paces the rollout across Instances of the
	// Component.
	RollingUpdate *RollingUpdate

	// InPlaceUpdateStrategy tunes lifecycle drain timing. Its grace period
	// also provides the post-unroute connection-settle window before a
	// SurgeThenDrain source pod is deleted.
	InPlaceUpdateStrategy *InPlaceUpdateStrategy
}

// RollingUpdate paces rollout across Instances of one Component.
// Workload-owned mirror of v1beta1.RollingUpdate.
type RollingUpdate struct {
	// Partition holds back updates for Instances whose index is <
	// Partition. 0 (the default) updates all Instances.
	Partition *int32

	// MaxUnavailable is the maximum number of Instances (or percent
	// expression) allowed to be in a non-Ready state during the rollout.
	// nil falls back to the reconciler default. Mirrors
	// v1beta1.RollingUpdate.MaxUnavailable.
	MaxUnavailable *intstr.IntOrString

	// MaxSurge is the maximum number of extra Instances (or percent
	// expression) the rollout may create above the Component's desired
	// replica count during a rolling update. nil falls back to the
	// reconciler default. Mirrors v1beta1.RollingUpdate.MaxSurge.
	MaxSurge *intstr.IntOrString
}

// InPlaceUpdateStrategy tunes the per-pod in-place update sequence.
// Workload-owned mirror of v1beta1.InPlaceUpdateStrategy.
type InPlaceUpdateStrategy struct {
	// GracePeriodSeconds is the time OMENative waits between marking
	// the pod not-ready and applying an in-place mutation. SurgeThenDrain
	// also waits this long after EndpointSlice removal before deletion so
	// persistent load-balancer connections can drain while workers live.
	GracePeriodSeconds *int32

	// MarkNotReadyDuringLifecycle, when true, flips ome.io/serving=False
	// on the pod before the in-place mutation so EndpointSlice drains
	// traffic first.
	MarkNotReadyDuringLifecycle *bool
}

// InstanceReadyPolicy controls how Instance-level readiness is
// aggregated from the underlying pods. Workload-owned mirror of the
// v1beta1.InstanceReadyPolicy enum.
type InstanceReadyPolicy string

const (
	// InstanceReadyPolicyAllPodReady reports the Instance Ready only
	// when every pod has Ready=True. Default for multi-pod Instances.
	InstanceReadyPolicyAllPodReady InstanceReadyPolicy = "AllPodReady"
	// InstanceReadyPolicyNone disables Instance-level aggregation; pods
	// are reported individually. Default for single-pod Instances.
	InstanceReadyPolicyNone InstanceReadyPolicy = "None"
)

// MigrationPolicy controls whether and how OMENative honors a
// migration request annotation for this Component. Workload-owned
// mirror of v1beta1.MigrationPolicy.
type MigrationPolicy struct {
	// Mode selects the migration disposition. Empty defaults to Auto.
	Mode MigrationMode
}

// ComponentTrafficTarget describes the percentage of traffic routed to
// one revision of one Component. Workload-owned mirror of
// v1beta1.ComponentTrafficTarget; serialized field-for-field by the
// adapter when projecting onto ISVC status.
type ComponentTrafficTarget struct {
	// RevisionName is the per-revision Service name for this target.
	RevisionName string

	// Percent is the percentage of traffic the consumer should route to
	// RevisionName. Sum across all entries for one Component is 100.
	Percent int32

	// Tag is an optional short identifier for the target (e.g.
	// "latest", "prev"). Cosmetic; not used by the consumer.
	Tag string

	// LatestRevision is true when this entry corresponds to the
	// LatestRolledoutRevision for the Component.
	LatestRevision bool
}
