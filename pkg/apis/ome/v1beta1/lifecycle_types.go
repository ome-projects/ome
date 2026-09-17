package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// LifecycleStatus reports OMENative-managed lifecycle state for one Component.
// Nil when the Component does not use OMENative. Counterpart of the
// LifecycleSpec sub-block on ComponentExtensionSpec.
type LifecycleStatus struct {
	// ObservedGeneration is the InferenceService.metadata.generation reflected by this block.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CurrentRevision names the ControllerRevision currently serving traffic.
	// +optional
	CurrentRevision string `json:"currentRevision,omitempty"`

	// UpdateRevision names the ControllerRevision being rolled out.
	// +optional
	UpdateRevision string `json:"updateRevision,omitempty"`

	// CollisionCount is the salt folded into ControllerRevision hash input
	// after a name collision is observed, so retries produce a different
	// revision name. Same pattern as StatefulSet.
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty"`

	// LabelSelector identifies pods of this Component for HPA-style consumers.
	// +optional
	LabelSelector string `json:"labelSelector,omitempty"`

	// Replicas is the total Instance count (any phase).
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of Instances whose every pod is Ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ServingReplicas is the number of Instances whose every pod is BOTH
	// ContainersReady AND has serving=True on the controller-managed
	// readiness gate — i.e., the pod is actually in the load balancer's
	// rotation. This is the count operators care about for traffic
	// availability, and the count MaxUnavailable budgets gate against.
	//
	// Diverges from ReadyReplicas during in-place updates and any other
	// scenario where the controller flips the serving gate to False while
	// containers remain technically Ready.
	// +optional
	ServingReplicas int32 `json:"servingReplicas,omitempty"`

	// AvailableReplicas is the number of Instances whose desired pod count
	// is Available: in rotation on the Component's headless Service and,
	// when lifecycle.minReadySeconds is set, Ready for at least that long.
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// UpdatedReplicas is the number of Instances on UpdateRevision.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// UpdatedReadyReplicas is the number of Instances on UpdateRevision and
	// fully Ready. UpdatedReadyReplicas / Replicas drives maxUnavailable
	// rollout pacing.
	// +optional
	UpdatedReadyReplicas int32 `json:"updatedReadyReplicas,omitempty"`

	// Conditions reports Component-level conditions. Defined types include
	// Available, Progressing, FailedScale, FailedUpdate,
	// GangSchedulingUnavailable, OperationStuck.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// RolloutHold reports the most recent reason this Component's rollout
	// did not advance, mirrored verbatim from the owning InferenceReplica's
	// status. Nil while the Component is progressing or has no rollout in
	// flight.
	// +optional
	RolloutHold *RolloutHold `json:"rolloutHold,omitempty"`

	// NOTE: per-Instance detail (InstanceStatuses) is intentionally NOT
	// mirrored here. The authoritative source of truth is the owning
	// InferenceReplica's status; the ISVC carries only this aggregated
	// summary. In-cluster consumers read the IR directly via
	// irprojector.ComponentIRStatus.
}

// RolloutHoldGate names the layer that most recently denied a
// per-Instance Update for a Component.
// +kubebuilder:validation:Enum=Pairing;Ratio;Sequential;Budget;RetryBlock;Held;Plan
type RolloutHoldGate string

const (
	// RolloutHoldGatePlan: the Component belongs to a rollout group but no
	// run has pinned an effective plan yet (run opening, or parked on an
	// unresolvable plan) — updates fail closed rather than roll forward on a
	// plan nothing validated.
	RolloutHoldGatePlan RolloutHoldGate = "Plan"
	// RolloutHoldGatePairing: the P/D pair-floor refused the step because it
	// would leave no pairable serving engine+decoder pair during a
	// pairing-protocol transition (coordination.CheckPairing).
	RolloutHoldGatePairing RolloutHoldGate = "Pairing"
	// RolloutHoldGateRatio: cross-Component RatioBalanced coordination
	// refused the surge because it would skew the observed ratio past
	// tolerance (coordination.CheckRatio).
	RolloutHoldGateRatio RolloutHoldGate = "Ratio"
	// RolloutHoldGateSequential: cross-Component Sequential coordination is
	// waiting on a different Component's turn, or on the inter-Component
	// soak window (coordination.CheckSequential).
	RolloutHoldGateSequential RolloutHoldGate = "Sequential"
	// RolloutHoldGateBudget: a surge or unavailability budget is
	// exhausted — either the per-Component cap or the cross-Component
	// MaxSurge/MaxUnavailable cap (coordination.CheckSurge /
	// CheckUnavailability).
	RolloutHoldGateBudget RolloutHoldGate = "Budget"
	// RolloutHoldGateRetryBlock: a same-target RetryBlock is in Backoff,
	// waiting for its NextRetryAt.
	RolloutHoldGateRetryBlock RolloutHoldGate = "RetryBlock"
	// RolloutHoldGateHeld: a same-target RetryBlock is Held — retry
	// attempts are exhausted; only a new target revision releases it.
	RolloutHoldGateHeld RolloutHoldGate = "Held"
)

// RolloutHold records the most recent reason a per-Instance Update was
// denied for a Component — the detail behind "why has this rollout not
// moved". Overwritten whenever a reconcile observes a fresh denial;
// cleared once the Component's rollout advances (an Update is admitted)
// or completes (CurrentRevision == UpdateRevision, or no rollout is in
// flight). Since is preserved across reconciles that keep reporting the
// same Gate and Target, so it anchors "held since" rather than "last
// observed at".
type RolloutHold struct {
	// Gate names the layer that produced Reason.
	Gate RolloutHoldGate `json:"gate"`

	// Reason is the operator-facing denial string from the gate that
	// produced it (e.g. "Sequential waiting on decoder").
	Reason string `json:"reason"`

	// Target is the ControllerRevision name the held Update was aimed at.
	// +optional
	Target string `json:"target,omitempty"`

	// Since is when this Gate+Target combination was first observed
	// without interruption. A change in Gate or Target restarts the
	// clock; a change in Reason text alone does not (mirrors how
	// apimeta.SetStatusCondition preserves LastTransitionTime while a
	// condition's Status is unchanged).
	Since metav1.Time `json:"since"`
}

// OMENativeInstancePhase is the lifecycle phase of an OMENative Instance.
// +kubebuilder:validation:Enum=Pending;Creating;Ready;Updating;Restarting;Migrating;Failed;Deleting
type OMENativeInstancePhase string

const (
	OMENativeInstancePending    OMENativeInstancePhase = "Pending"
	OMENativeInstanceCreating   OMENativeInstancePhase = "Creating"
	OMENativeInstanceReady      OMENativeInstancePhase = "Ready"
	OMENativeInstanceUpdating   OMENativeInstancePhase = "Updating"
	OMENativeInstanceRestarting OMENativeInstancePhase = "Restarting"
	OMENativeInstanceMigrating  OMENativeInstancePhase = "Migrating"
	OMENativeInstanceFailed     OMENativeInstancePhase = "Failed"
	OMENativeInstanceDeleting   OMENativeInstancePhase = "Deleting"
)

// OMENativeInstanceStatus reports state for one OMENative Instance.
type OMENativeInstanceStatus struct {
	// Index is the stable Instance ordinal. Values may be sparse after surge migration.
	Index int32 `json:"index"`

	// Incarnation increments each time the Instance is recreated (full
	// recreate update, restart-on-loss). In-place updates do not increment.
	// Stamped on pods via the `ome.io/instance-incarnation` label.
	// +optional
	Incarnation int64 `json:"incarnation,omitempty"`

	// Phase is the lifecycle phase of the Instance.
	Phase OMENativeInstancePhase `json:"phase"`

	// RunningRevision is the ControllerRevision the live pods are running.
	// +optional
	RunningRevision string `json:"runningRevision,omitempty"`

	// TargetRevision is the ControllerRevision the live pods are converging toward.
	// +optional
	TargetRevision string `json:"targetRevision,omitempty"`

	// PodCount is the total number of pods owned by this Instance.
	// +optional
	PodCount int32 `json:"podCount,omitempty"`

	// ReadyPodCount is retained for API compatibility. OMENative derives
	// readiness from Pods and does not persist this field.
	// +optional
	ReadyPodCount int32 `json:"readyPodCount,omitempty"`

	// ServingPodCount is the number of pods that are BOTH ContainersReady
	// AND have serving=True on the controller-managed readiness gate —
	// i.e., pods actually in the load balancer rotation. Diverges from
	// ReadyPodCount whenever the controller flips the serving gate
	// (in-place updates, drain steps, etc.). MaxUnavailable budgets are
	// computed against the serving count, not the ready count, so that
	// "missing from traffic" reflects reality.
	// +optional
	ServingPodCount int32 `json:"servingPodCount,omitempty"`

	// AvailablePodCount is the number of pods that are Available: in
	// rotation on the Component's headless Service and, when
	// lifecycle.minReadySeconds is set, Ready for at least that long.
	// +optional
	AvailablePodCount int32 `json:"availablePodCount,omitempty"`

	// ScheduledPodCount is retained for API compatibility. OMENative derives
	// scheduling state from Pods and does not persist this field.
	// +optional
	ScheduledPodCount int32 `json:"scheduledPodCount,omitempty"`

	// Admitted is true when this Instance's pods have left the Kueue admission
	// scheduling gate (quota granted): the Instance has pods and none are
	// admission-gated. False while gated/queued or before pods exist. Used by
	// the control plane to decide the fan-out race winner.
	// +optional
	Admitted bool `json:"admitted,omitempty"`

	// NodesOccupied is retained for API compatibility. OMENative reads current
	// placement from Pods and does not persist this field.
	// +optional
	// +listType=set
	NodesOccupied []string `json:"nodesOccupied,omitempty"`

	// Conditions reports Instance-level conditions. Defined types include
	// AllPodsScheduled, AllPodsReady, AllPodsAvailable, Drained, Failed,
	// ProgressDeadlineExceeded.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ReadySince is when the Instance last entered Ready. Anchors
	// post-Ready failure detection: container restarts that finished
	// before this time belong to boot and never trigger a recreate.
	// +optional
	ReadySince *metav1.Time `json:"readySince,omitempty"`

	// Operation is the durable record of an in-flight destructive action
	// against this Instance. Set before the action starts, cleared after
	// it completes. Lets a crashed controller resume work without diffing
	// pod state.
	// +optional
	Operation *InstanceOperation `json:"operation,omitempty"`

	// ActiveOrdinal identifies which of two pod-naming slots
	// (0 or 1) currently holds the canonical pod for this single-pod
	// Instance. SurgeThenDrain alternates between slots: the surge
	// creates a new pod at `1 - ActiveOrdinal`, waits Ready, drains
	// the old pod, then advances ActiveOrdinal to the new slot. Stays
	// 0 for InPlace and Recreate strategies (those reuse the same
	// pod name). Defaults to 0 on initial create.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +optional
	ActiveOrdinal int32 `json:"activeOrdinal,omitempty"`

	// LastFailure preserves the diagnostics of the pod whose failure
	// (or stuck-pod escalation) last triggered a recreate of this
	// Instance. OMENative drains and deletes every gang pod on recreate,
	// so the failing pod and its container termination state vanish from
	// the cluster — this field is the surviving trace operators read when
	// a multi-node Instance keeps recreating. Set on the first pass of a
	// failure-driven Restart and on stuck-pod escalation; left untouched
	// by revision-roll recreates (those carry their cause in
	// Operation.Reason instead) so the most recent genuine failure isn't
	// overwritten by a clean rollout.
	// +optional
	LastFailure *InstanceTermination `json:"lastFailure,omitempty"`
}

// InstanceTermination captures the container-termination diagnostics of
// a pod that failed or was escalated, preserved on OMENativeInstanceStatus
// so the trace survives the recreate that deletes the pod.
type InstanceTermination struct {
	// PodName is the name of the pod that failed or was escalated.
	PodName string `json:"podName"`

	// ContainerName is the container the diagnostics were read from.
	// Empty when the signal is pod-level (e.g. a pod-phase Failed with no
	// per-container terminated state available).
	// +optional
	ContainerName string `json:"containerName,omitempty"`

	// Reason is the kubelet reason for the termination or wedge — a
	// terminated-state reason (OOMKilled, Error, ContainerCannotRun) or a
	// terminal waiting-state reason (CrashLoopBackOff, ImagePullBackOff,
	// CreateContainerError). Falls back to "PodFailed" when only the pod
	// phase is available.
	// +optional
	Reason string `json:"reason,omitempty"`

	// ExitCode is the container's exit code when the diagnostics came from
	// a terminated state. Nil when the pod was stuck in a waiting state
	// (no process ever ran to produce an exit code).
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`

	// Message is the kubelet-supplied detail message, if any.
	// +optional
	Message string `json:"message,omitempty"`

	// Time is when OMENative recorded this termination (reconcile time, not
	// the kubelet transition time, which isn't always exposed).
	// +optional
	Time metav1.Time `json:"time,omitempty"`
}

// InstanceOperation is the recovery anchor written before any destructive
// action against an Instance and cleared on completion.
type InstanceOperation struct {
	// ID is the idempotency key for this operation. Retries with the same
	// ID are a no-op.
	ID string `json:"id"`

	// Type identifies the operation kind: Create, Update, Restart, Migrate, Delete.
	// +kubebuilder:validation:Enum=Create;Update;Restart;Migrate;Delete
	Type InstanceOperationType `json:"type"`

	// Step is the fine-grained resume point within the operation
	// (e.g., Drain, DeletePods, WaitZero, Recreate, WaitReady).
	Step string `json:"step"`

	// StartedAt is when this operation began.
	StartedAt metav1.Time `json:"startedAt"`

	// LastProgressAt is when the operation last advanced. Used for stall detection.
	LastProgressAt metav1.Time `json:"lastProgressAt"`

	// Deadline is the hard timeout for the current Step. Zero (null) means the
	// step clock is parked (e.g. while the instance's pods are admission-gated);
	// optional + nullable so the parked null is accepted by the apiserver.
	// +optional
	// +nullable
	Deadline metav1.Time `json:"deadline"`

	// RetryCount is the per-step escalation counter.
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`

	// TargetRevision is the ControllerRevision the operation is converging toward.
	// +optional
	TargetRevision string `json:"targetRevision,omitempty"`

	// Reason is a short cause string for the operation. Set once, when the
	// operation starts, by whichever state machine owns it (the restart
	// trigger, the revision-roll cause): it names WHY the operation
	// exists and must survive for the operation's lifetime.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Waiting is a short token naming an EXTERNAL condition currently
	// holding the operation back — e.g. "QuotaExceeded" when admission
	// refuses the operation's pods for lack of quota. Distinct from
	// Reason, which names why the operation exists: Waiting comes and
	// goes while the operation runs, so overloading Reason would erase
	// the operation's cause. Empty means nothing external is blocking.
	// A token only: the blocking authority's own message is volatile (an
	// exceeded-quota message moves as other workloads come and go) and
	// belongs in the Event, not in status.
	// +optional
	Waiting string `json:"waiting,omitempty"`

	// SurgeIndex is the Instance index allocated for a surge replacement.
	// Set only when Type=Migrate.
	// +optional
	SurgeIndex *int32 `json:"surgeIndex,omitempty"`

	// FromNode is the node the migrating Instance is being moved off.
	// Set only when Type=Migrate.
	// +optional
	FromNode string `json:"fromNode,omitempty"`

	// HintTargetNodes is the caller-supplied preferred-node list for a migration.
	// Set only when Type=Migrate.
	// +optional
	// +listType=atomic
	HintTargetNodes []string `json:"hintTargetNodes,omitempty"`

	// RequestUUID is the migration request UUID. Set only when Type=Migrate.
	// +optional
	RequestUUID string `json:"requestUUID,omitempty"`
}

// InstanceOperationType is the kind of destructive action recorded in
// InstanceOperation.
type InstanceOperationType string

const (
	InstanceOperationCreate  InstanceOperationType = "Create"
	InstanceOperationUpdate  InstanceOperationType = "Update"
	InstanceOperationRestart InstanceOperationType = "Restart"
	InstanceOperationMigrate InstanceOperationType = "Migrate"
	InstanceOperationDelete  InstanceOperationType = "Delete"
)

// MigrationMode is the migration mechanism selected.
// +kubebuilder:validation:Enum=Surge
type MigrationMode string

const (
	// MigrationModeSurge creates a replacement Instance before draining the original.
	MigrationModeSurge MigrationMode = "Surge"
)

// MigrationPhase is declared in inferencereplica_types.go beside the
// authoritative status.migrations types.

// MigrationEvent is one observation logged during a migration.
type MigrationEvent struct {
	// At is the time the event was recorded.
	At metav1.Time `json:"at"`
	// Message describes the observation.
	Message string `json:"message"`
}

// MigrationHistoryEntry is one entry of the rolling MigrationHistory window
// recorded on InferenceServiceStatus.
type MigrationHistoryEntry struct {
	// ID is the request UUID supplied by the caller.
	ID string `json:"id"`

	// Component identifies which Component was migrated.
	Component ComponentType `json:"component"`

	// Instance is the original Instance index that was migrated away from.
	Instance int32 `json:"instance"`

	// ReplacementInstance is the Instance index allocated for the surge replacement.
	// Nil when the migration was rejected before allocation. Distinct from
	// index 0, which is a valid replacement index after sparse migration.
	// +optional
	ReplacementInstance *int32 `json:"replacementInstance,omitempty"`

	// Mode is the migration mechanism. Surge is the only defined mode.
	Mode MigrationMode `json:"mode"`

	// Phase is the current lifecycle phase.
	Phase MigrationPhase `json:"phase"`

	// RequestedAt is the time the request annotation was first observed.
	RequestedAt metav1.Time `json:"requestedAt"`

	// StartedAt is the time the controller began executing the migration.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is the time the migration reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Reason is the caller-supplied reason (e.g., "fragmentation").
	// +optional
	Reason string `json:"reason,omitempty"`

	// RequestedBy identifies the caller that wrote the request annotation.
	// +optional
	RequestedBy string `json:"requestedBy,omitempty"`

	// Events records observations made during the migration.
	// +optional
	// +listType=atomic
	Events []MigrationEvent `json:"events,omitempty"`

	// OutcomeReason is a short machine-readable reason describing why the
	// migration ended in its terminal phase (e.g., "InstanceReadyTimeout").
	// +optional
	OutcomeReason string `json:"outcomeReason,omitempty"`
}

// LifecycleSpec groups OMENative-specific lifecycle policies on a
// Component. Embedded via ComponentExtensionSpec.Lifecycle; all fields are
// no-ops unless the Component is annotated ome.io/deploymentMode=OMENative.
// Nested under `lifecycle:` so the fields don't collide with corev1.PodSpec
// fields (notably restartPolicy) that are also inlined into EngineSpec /
// DecoderSpec / RouterSpec.
type LifecycleSpec struct {
	// RestartPolicy controls what OMENative does when a managed pod fails.
	// Distinct from the inlined corev1.PodSpec.RestartPolicy: that one tells
	// kubelet whether to restart a container in-place; this one tells the
	// OMENative controller whether to recreate the whole Instance.
	// +optional
	RestartPolicy *InstanceRestartPolicy `json:"restartPolicy,omitempty"`

	// UpdateStrategy controls how OMENative rolls template changes across
	// the Component's Instances.
	// +optional
	UpdateStrategy *UpdateStrategy `json:"updateStrategy,omitempty"`

	// ReadyPolicy controls how Instance-level readiness is aggregated from
	// the underlying pods. None is accepted only for single-pod Instances,
	// where it is equivalent to AllPodReady; admission rejects None on
	// multi-pod OMENative Components because per-pod readiness reporting
	// is not supported.
	// +optional
	ReadyPolicy *InstanceReadyPolicy `json:"readyPolicy,omitempty"`

	// InstanceReadyTimeout is OMENative's upper bound for waiting on a
	// newly-created Instance to become Ready (during migration, rollout,
	// or initial creation).
	// +optional
	InstanceReadyTimeout *metav1.Duration `json:"instanceReadyTimeout,omitempty"`

	// MinReadySeconds is the minimum time a newly Ready pod must stay Ready
	// before OMENative treats it as Available. A pod is Available when its
	// PodReady condition is True and that condition's lastTransitionTime
	// plus MinReadySeconds is not after the current time, evaluated per pod
	// (the Deployment.spec.minReadySeconds rule). Ready remains the health
	// signal; Available is the pacing signal: a rollout drains or promotes
	// an Instance only once its new pods are Available, so the maxSurge /
	// maxUnavailable budgets stay held for the whole window, and
	// availableReplicas / availablePodCount count only Available pods.
	// Unset or 0 means Available as soon as Ready. A value authored on the
	// InferenceService always wins. Clusters may supply an admission-time
	// default through the inferenceservice-config ConfigMap; it is stamped
	// onto the InferenceService, so it also takes precedence over a value
	// authored in the ServingRuntime's engineConfig / decoderConfig /
	// routerConfig lifecycle (the InferenceService overrides the runtime
	// when the two merge), as terminationGracePeriodSeconds does.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinReadySeconds *int32 `json:"minReadySeconds,omitempty"`

	// MigrationPolicy controls whether and how OMENative honors a migration
	// request annotation for this Component.
	// +optional
	MigrationPolicy *MigrationPolicy `json:"migrationPolicy,omitempty"`
}

// InstanceRestartPolicy controls what OMENative does when a managed pod
// fails. Applies only when the Component is annotated
// ome.io/deploymentMode=OMENative.
// +kubebuilder:validation:Enum=None;RecreateInstanceOnPodRestart
type InstanceRestartPolicy string

const (
	// InstanceRestartPolicyNone restarts only the failed pod; OMENative
	// does not trigger cross-pod coordination.
	InstanceRestartPolicyNone InstanceRestartPolicy = "None"
	// InstanceRestartPolicyRecreateInstance drains, deletes, and recreates
	// every pod in the failed pod's Instance. Default for multi-pod Instances.
	InstanceRestartPolicyRecreateInstance InstanceRestartPolicy = "RecreateInstanceOnPodRestart"
)

// UpdateStrategy controls how OMENative rolls template changes
// across an Instance's pods.
type UpdateStrategy struct {
	// Type selects the rollout mechanism. Defaults to SurgeThenDrain
	// for safety — preserves serving capacity throughout the rollout.
	// +optional
	Type UpdateStrategyType `json:"type,omitempty"`

	// RollingUpdate paces the rollout across Instances of the Component.
	// +optional
	RollingUpdate *RollingUpdate `json:"rollingUpdate,omitempty"`

	// InPlaceUpdateStrategy tunes the per-pod in-place update sequence.
	// Ignored when Type is RecreatePod.
	// +optional
	InPlaceUpdateStrategy *InPlaceUpdateStrategy `json:"inPlaceUpdateStrategy,omitempty"`
}

// UpdateStrategyType selects the rollout mechanism.
// +kubebuilder:validation:Enum=SurgeThenDrain;RecreatePod;InPlaceIfPossible;InPlaceOnly
type UpdateStrategyType string

const (
	// UpdateStrategySurgeThenDrain creates a new pod for the
	// target revision, waits for it to be serving, then drains and
	// deletes the old pod. Costs +1 pod briefly per replica but
	// preserves serving capacity throughout the rollout — no traffic gap.
	// Default strategy.
	UpdateStrategySurgeThenDrain UpdateStrategyType = "SurgeThenDrain"
	// UpdateStrategyRecreatePod always deletes pods and recreates them.
	UpdateStrategyRecreatePod UpdateStrategyType = "RecreatePod"
	// UpdateStrategyInPlaceIfPossible tries in-place first; falls
	// back to recreate if the template change is not in-place-capable.
	UpdateStrategyInPlaceIfPossible UpdateStrategyType = "InPlaceIfPossible"
	// UpdateStrategyInPlaceOnly rejects updates that cannot be
	// applied in-place.
	UpdateStrategyInPlaceOnly UpdateStrategyType = "InPlaceOnly"
)

// RollingUpdate paces rollout across Instances of one Component.
type RollingUpdate struct {
	// Partition holds back updates for Instances whose index is < Partition.
	// 0 (the default) updates all Instances. Used for canary holds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Partition *int32 `json:"partition,omitempty"`

	// MaxUnavailable is the maximum number of Instances (or fraction) allowed
	// to be in a non-Ready state during the rollout. Accepts either an
	// absolute integer count (e.g. 2) or a percent string (e.g. "25%").
	// Percent values resolve to ceil(replicas * percent / 100) at
	// reconcile time so the budget scales with the Component's replica count.
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// MaxSurge is the maximum number of extra Instances (or fraction) the
	// rollout may create above the Component's desired replica count
	// during a rolling update. Accepts either an absolute integer count
	// (e.g. 1) or a percent string (e.g. "25%"). Percent values resolve
	// to ceil(replicas * percent / 100) at reconcile time. Mirrors the
	// semantics of upstream appsv1.Deployment.Strategy.RollingUpdate.MaxSurge
	// — extra capacity is created and brought to Ready before old Instances
	// are drained, enabling zero-capacity-dip rollouts.
	//
	// When the Component participates in a RolloutCoordinationGroup with a
	// non-zero CoordinationPacing.MaxSurge, the group-wide ceiling caps the
	// per-Component MaxSurge.
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`
}

// InPlaceUpdateStrategy tunes per-pod lifecycle drain timing.
type InPlaceUpdateStrategy struct {
	// GracePeriodSeconds is the time OMENative waits between marking the
	// pod not-ready and applying an in-place mutation. SurgeThenDrain also
	// waits this long after EndpointSlice removal before deleting the old pod,
	// allowing persistent load-balancer connections to drain while its workers
	// remain available.
	// +optional
	// +kubebuilder:validation:Minimum=0
	GracePeriodSeconds *int32 `json:"gracePeriodSeconds,omitempty"`

	// MarkNotReadyDuringLifecycle, when true, flips ome.io/serving=False on
	// the pod before the in-place mutation so EndpointSlice drains traffic
	// first. Defaults to true.
	// +optional
	MarkNotReadyDuringLifecycle *bool `json:"markNotReadyDuringLifecycle,omitempty"`
}

// InstanceReadyPolicy controls how Instance-level readiness is aggregated
// from the underlying pods.
// +kubebuilder:validation:Enum=AllPodReady;None
type InstanceReadyPolicy string

const (
	// InstanceReadyPolicyAllPodReady reports the Instance Ready only when
	// every pod in the Instance has Ready=True. Default for multi-pod Instances.
	InstanceReadyPolicyAllPodReady InstanceReadyPolicy = "AllPodReady"
	// InstanceReadyPolicyNone is accepted only for single-pod Instances,
	// where it is behaviorally identical to AllPodReady. Admission rejects
	// it on multi-pod OMENative Components: per-pod readiness reporting is
	// not supported, so multi-pod readiness is always the AllPodReady
	// aggregation.
	InstanceReadyPolicyNone InstanceReadyPolicy = "None"
)

// MigrationPolicy controls whether and how OMENative honors migration requests
// (both automatic deadline-disposition relocation and explicit annotation-triggered migration).
type MigrationPolicy struct {
	// Mode gates migration: Auto enables both deadline-disposition relocation and
	// explicit migration requests; Never disables both. Defaults to Auto.
	// +optional
	Mode MigrationPolicyMode `json:"mode,omitempty"`
}

// MigrationPolicyMode gates migration mechanisms for a Component.
// +kubebuilder:validation:Enum=Auto;Surge;Never
type MigrationPolicyMode string

const (
	// MigrationPolicyModeAuto enables automatic relocation on deadline expiry
	// and honors explicit migration requests.
	MigrationPolicyModeAuto MigrationPolicyMode = "Auto"
	// MigrationPolicyModeSurge forces surge migration when capacity is available.
	MigrationPolicyModeSurge MigrationPolicyMode = "Surge"
	// MigrationPolicyModeNever disables all migration. Migration requests
	// are rejected with reason MigrationDisabled.
	MigrationPolicyModeNever MigrationPolicyMode = "Never"
)
