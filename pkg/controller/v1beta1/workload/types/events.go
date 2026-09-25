package types

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EventReason is the event-reason identifier the engine stamps on K8s
// Events. The values are an operator-facing contract — dashboards and
// `kubectl describe` match on them — and do not change.
type EventReason string

func (r EventReason) String() string { return string(r) }

// RecordNormal emits a Normal K8s Event against target. nil-safe: when
// the recorder isn't wired (tests that construct Deps{} directly) OR the
// target is nil, this is a no-op so callers don't have to plumb a
// recorder. Reason is typed as EventReason — call sites pass the
// workload-owned constant rather than a raw string so a typo or drift
// between reasons becomes a compile-time error.
func RecordNormal(rec record.EventRecorder, target client.Object, reason EventReason, messageFmt string, args ...any) {
	if rec == nil || target == nil {
		return
	}
	rec.Eventf(target, corev1.EventTypeNormal, string(reason), messageFmt, args...)
}

// RecordWarning emits a Warning K8s Event against target. Same nil-safe
// semantics as RecordNormal. Reason is workload-typed for the same
// drift-resistance rationale.
func RecordWarning(rec record.EventRecorder, target client.Object, reason EventReason, messageFmt string, args ...any) {
	if rec == nil || target == nil {
		return
	}
	rec.Eventf(target, corev1.EventTypeWarning, string(reason), messageFmt, args...)
}

// EventTarget returns the object emitted events are stamped against —
// ReconcileInput.EventTarget when set, falling back to OwnerObject. One
// rule for every emitter in the engine, matching the nil-OK semantics
// documented on the field, so no caller branches on EventTarget==nil
// itself.
func EventTarget(input ReconcileInput) client.Object {
	if input.EventTarget != nil {
		return input.EventTarget
	}
	return input.OwnerObject
}

// InstanceKey formats "component=engine instance=2" for event messages.
// Keeps the format consistent across every emitter so operators can grep
// on a single shape.
func InstanceKey(component ComponentType, idx int32) string {
	return fmt.Sprintf("component=%s instance=%d", component, idx)
}

const (
	// Create / scale-up (workload/ops/create.go).
	EventReasonInstanceCreated EventReason = "InstanceCreated"
	EventReasonInstanceReady   EventReason = "InstanceReady"

	// EventReasonInstanceRejected fires when the apiserver PERMANENTLY
	// rejects an Instance's pod create or in-place patch — an invalid pod
	// template (422) or a terminating namespace. The attempt is disposed
	// Failed; retrying the same revision would reproduce the rejection
	// byte for byte. Names the pod and the apiserver's own explanation.
	EventReasonInstanceRejected EventReason = "InstanceRejected"

	// EventReasonInstanceQuotaBlocked fires once per blocking episode
	// when admission refuses an Instance's pods for lack of quota. The
	// attempt is NOT failed: it waits with its InstanceReadyTimeout clock
	// parked until quota frees up.
	EventReasonInstanceQuotaBlocked EventReason = "InstanceQuotaBlocked"

	// EventReasonCreateAttemptSuperseded fires when the target revision
	// moves while a Create attempt is still building: the attempt is
	// replaced by a fresh one pinned to the new target. Normal, not a
	// failure — the retired revision never got the chance to fail. Names
	// the superseded revision and the one the replacement is pinned to.
	EventReasonCreateAttemptSuperseded EventReason = "CreateAttemptSuperseded"

	// In-place update (workload/ops/update.go).
	EventReasonInPlaceUpdateStarted     EventReason = "InPlaceUpdateStarted"
	EventReasonInPlaceUpdateCompleted   EventReason = "InPlaceUpdateCompleted"
	EventReasonInPlaceUpdateNotPossible EventReason = "InPlaceUpdateNotPossible"

	// Recreate / surge update (workload/ops/update.go).
	EventReasonRecreateUpdateStarted     EventReason = "RecreateUpdateStarted"
	EventReasonRecreateUpdateCompleted   EventReason = "RecreateUpdateCompleted"
	EventReasonFailedSurgeTargetRecycled EventReason = "FailedSurgeTargetRecycled"

	// Restart (workload/ops/restart.go).
	EventReasonRestartTriggered EventReason = "RestartTriggered"
	EventReasonRestartCompleted EventReason = "RestartCompleted"

	// EventReasonTerminalPodRecycled fires when Create or Restart deletes
	// a terminal pod (phase Failed or Succeeded) that occupied one of an
	// Instance's stable pod names, so the target can be recreated.
	EventReasonTerminalPodRecycled EventReason = "TerminalPodRecycled"

	// EventReasonFoundOrphan fires when Restart or recreate-Update
	// finds a pod under the OMENative selector but missing the
	// ome.io/instance-incarnation label. The reconciler refuses to
	// delete it; the operator must re-classify or remove the pod
	// manually.
	EventReasonFoundOrphan EventReason = "FoundOrphan"

	// EventReasonSupersededWreckageCleaned fires when the corrective-
	// edit cleanup deletes pods keyed to a superseded revision — debris
	// a failed rollout left behind that no revision-diff trigger could
	// reach.
	EventReasonSupersededWreckageCleaned EventReason = "SupersededWreckageCleaned"

	// EventReasonAutoMigrationTriggered fires when the deadline
	// disposition records a relocation directive (terminal AutoRecover
	// ledger entry) for a stuck Instance — its rebuild will be steered
	// off the recorded node.
	EventReasonAutoMigrationTriggered EventReason = "AutoMigrationTriggered"

	// EventReasonAutoMigrationCapReached fires exactly once per budget
	// fill — when the disposition records the relocation directive that
	// exhausts the (component, instance) AutoRecover budget
	// (lifecycle.autoMigrate.maxAttempts). Subsequent over-budget
	// dispositions dispose terminal silently. Operator intervention (or
	// an instance reaching Ready, which prunes its records) is required
	// before relocation resumes.
	EventReasonAutoMigrationCapReached EventReason = "AutoMigrationCapReached"

	// EventReasonInstanceFailed fires when an escalation backstop (the
	// stuck-pod fast path or the deadline disposition) stamps an
	// Instance Phase=Failed. Emitted by adapters through
	// ReconcileInput.WarnInstanceFailed.
	EventReasonInstanceFailed EventReason = "InstanceFailed"

	// EventReasonRetryHeld fires once, at the RetryBlock transition into
	// State=Held — same-target update retries exhausted; a corrected
	// revision (or raised retry limits) is required. Emitted by adapters
	// through ReconcileInput.WarnRetryHeld.
	EventReasonRetryHeld EventReason = "RetryHeld"

	// EventReasonRetryBlockReleased fires when the operator release
	// annotation (ome.io/release-held-revision) removes a Held
	// RetryBlock — the manual exit from the terminal Held state. Names
	// the released revision and that the removal was operator-requested.
	EventReasonRetryBlockReleased EventReason = "RetryBlockReleased"

	// EventReasonRetryBlockReleaseSkipped fires when the release
	// annotation names no releasable block — no RetryBlock exists for
	// the requested revision, or the matched block is not State=Held.
	// The annotation is still consumed; the event explains why nothing
	// changed.
	EventReasonRetryBlockReleaseSkipped EventReason = "RetryBlockReleaseSkipped"

	// EventReasonInstancesReset fires when the operator reset annotation
	// (ome.io/reset-instances) tears down Failed Instances for rebuild:
	// their pods are deleted and the preserved Operation cleared so the
	// ordinary Create/Restart passes recreate them. Names the indices.
	EventReasonInstancesReset EventReason = "InstancesReset"

	// EventReasonInstancesResetSkipped fires when the reset annotation
	// names Instances that cannot be reset — not Phase=Failed, no such
	// index, or nothing left to tear down. The annotation is still
	// consumed; the event lists each index with the reason it was skipped.
	EventReasonInstancesResetSkipped EventReason = "InstancesResetSkipped"

	// EventReasonInstancesResetRejected fires when the reset annotation
	// value is malformed (neither "all" nor a comma-separated list of
	// non-negative indices). Nothing is reset; the annotation is consumed.
	EventReasonInstancesResetRejected EventReason = "InstancesResetRejected"

	// EventReasonInstanceDemoted fires when the truth pass demotes a
	// Ready Instance with no live pods and no in-flight operation to
	// Pending. Status-only; recovery stays with the ordinary passes.
	EventReasonInstanceDemoted EventReason = "InstanceDemoted"

	// EventReasonPodForceDeleted fires when a teardown escalation
	// force-deletes (grace 0, UID-preconditioned) a Terminating pod
	// overdue past its own deletion deadline on a node that provably
	// cannot acknowledge the termination (gone, or unreachable-tainted /
	// NotReady beyond the configured threshold). Names the pod, node,
	// evidence branch, and overdue duration.
	EventReasonPodForceDeleted EventReason = "PodForceDeleted"

	// EventReasonRepairHeld fires when a crash-loop repair is denied by
	// the per-Component unavailability budget or the coordination gate
	// and no other repair opened in the same pass. The Component stays
	// wedged until the denial lifts and nothing else reports that, so
	// the pass names the Instance that is waiting and the layer it
	// waits on.
	EventReasonRepairHeld EventReason = "RepairHeld"

	// EventReasonDrainOverdue fires once per overdue episode when a
	// Deleting Instance's operation deadline elapses with pods still on
	// their way out. Visibility only: the delete wave keeps the index,
	// the phase does not move, and force-deleting a wedged pod stays
	// gated on the configured policy. Names the Instance, the pods and
	// how long overdue the drain is.
	EventReasonDrainOverdue EventReason = EventReason(DrainOverdueReason)

	// EventReasonPodDeleteBlockedByFinalizer fires (once per pod UID)
	// when a Terminating pod is overdue past its deletion deadline but
	// pinned by foreign finalizers. Report-only: OME never strips
	// another controller's finalizer, so the teardown stays blocked
	// until the finalizer owner resolves it.
	EventReasonPodDeleteBlockedByFinalizer EventReason = "PodDeleteBlockedByFinalizer"

	// Migration (workload/ops/migrate.go + the IR accept pass).
	EventReasonMigrationRequestAccepted EventReason = "MigrationRequestAccepted"
	EventReasonMigrationRequestRejected EventReason = "MigrationRequestRejected"
	// EventReasonUnsupportedSchemaVersion fires when a migration-request
	// annotation carries a schemaVersion the controller doesn't
	// understand. Kept distinct from MigrationRequestRejected so
	// dashboards can alert on requester/controller version skew.
	EventReasonUnsupportedSchemaVersion EventReason = "UnsupportedSchemaVersion"
	EventReasonMigrationCompleted       EventReason = "MigrationCompleted"
	// EventReasonMigrationExpired fires when a non-terminal Manual
	// migration record passes its Deadline: the record is closed
	// Failed, the pair's Migrate ops are cleared, the surge is torn
	// down by the ordinary scale-down batch pipeline, and the source
	// phase is restored from observation.
	EventReasonMigrationExpired EventReason = "MigrationExpired"
	// EventReasonMigrationSurgeWedged fires when a surge pod of an
	// in-flight migration is parked in a terminal kubelet waiting reason
	// past the stuck-pod grace: a replacement that cannot start can never
	// take over, so the record is closed Failed on that evidence instead
	// of idling to its Deadline. Closed the same way an expiry closes it —
	// surge unpinned for the scale-down pipeline, source restored from
	// observation — so the event names the pod and the reason.
	EventReasonMigrationSurgeWedged EventReason = "MigrationSurgeWedged"
	EventReasonRateLimited          EventReason = "RateLimited"
	// EventReasonMigrationPolicyUnconfigured is a Warning fired when a
	// migration request is held because the operator configured no
	// migration capacity policy. Distinct from RateLimited: no cap was
	// breached, there is no cap to judge against, and the request waits
	// rather than failing.
	EventReasonMigrationPolicyUnconfigured EventReason = "MigrationPolicyUnconfigured"
	// EventReasonInstanceReadyTimeoutUnconfigured is a Warning fired once,
	// on the pass that raises the matching condition, when a Component
	// opens operations with no readiness deadline because neither
	// spec.lifecycle.instanceReadyTimeout nor the operator's
	// lifecycle.instanceReadyTimeout is set. Nothing is failed: an Instance
	// that never becomes Ready waits for an operator instead.
	EventReasonInstanceReadyTimeoutUnconfigured EventReason = "InstanceReadyTimeoutUnconfigured"
	EventReasonMigrationSurgeCreateBlocked      EventReason = "MigrationSurgeCreateBlocked"
	EventReasonMigrationFromNodeMismatch        EventReason = "MigrationFromNodeMismatch"
	EventReasonMigrationNodeAffinityConflict    EventReason = "MigrationNodeAffinityConflict"

	// EventReasonPodGroupReset is a Warning fired when an Instance's
	// PodGroup is deleted so a fresh one can be built, because the gang
	// scheduler has ruled the existing group Failed. That verdict is
	// absorbing — the group keeps it for as long as the object lives, and
	// the controller reconciles only its labels, ownership and size — so
	// the name has to be rebuilt for the gang to be admitted again. The
	// event carries the group's own explanation, which is the only place
	// it survives: the Instance is never failed for it.
	EventReasonPodGroupReset EventReason = "PodGroupReset"

	// EventReasonMaybeNoGangScheduler is a soft Warning fired the first
	// time a multi-pod Instance's PodGroup is created under a pod
	// template whose `spec.schedulerName` is unset or equals the
	// upstream default ("default-scheduler"). A stock kube-scheduler
	// does NOT read scheduling.x-k8s.io/v1alpha1 PodGroup objects, so
	// the gang contract degrades to per-pod scheduling silently.
	// Operators install scheduler-plugins as a secondary scheduler
	// (`scheduler-plugins-scheduler`) or as a default-scheduler plugin
	// (in which case the warning is a false positive the controller
	// can't detect from inside the cluster). A standing warning: announced
	// once per Instance incarnation on the Instance's own row
	// (status.Announce), whatever attempt the row is on.
	EventReasonMaybeNoGangScheduler EventReason = "MaybeNoGangScheduler"

	// EventReasonGangSplitRisk is a soft Warning fired the first time a
	// multi-node gang WORKER pod is created with no co-location
	// podAffinity at all — neither an OME-injected topologyKey term nor a
	// user-declared one. Such a gang may schedule across separate
	// network / NVLink / TPU topology domains, which breaks the
	// tightly-coupled collectives a multi-node runtime needs (NCCL/RCCL/
	// NIXL all-reduce, multi-host TPU sessions). The operator sets
	// engine.topologyKey / decoder.topologyKey (e.g. a NVLink/RDMA domain
	// label, or the GKE TPU topology label) or declares a worker
	// podAffinity. Advisory only — never blocks the create. A standing
	// warning: announced once per Instance incarnation on the Instance's
	// own row (status.Announce), whatever attempt the row is on.
	EventReasonGangSplitRisk EventReason = "GangSplitRisk"
)
