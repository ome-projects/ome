package types

// EventReason is the workload-internal event-reason identifier the ops
// state machines stamp on K8s Events. Values match the legacy
// omenative/status reason strings byte-for-byte so existing operator
// dashboards and `kubectl describe` output keep matching.
type EventReason string

func (r EventReason) String() string { return string(r) }

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
	// off the recorded node. Value matches the legacy omenative
	// detector's reason string.
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
	EventReasonMigrationExpired              EventReason = "MigrationExpired"
	EventReasonRateLimited                   EventReason = "RateLimited"
	EventReasonMigrationSurgeCreateBlocked   EventReason = "MigrationSurgeCreateBlocked"
	EventReasonMigrationFromNodeMismatch     EventReason = "MigrationFromNodeMismatch"
	EventReasonMigrationNodeAffinityConflict EventReason = "MigrationNodeAffinityConflict"

	// EventReasonMaybeNoGangScheduler is a soft Warning fired the first
	// time a multi-pod Instance's PodGroup is created under a pod
	// template whose `spec.schedulerName` is unset or equals the
	// upstream default ("default-scheduler"). A stock kube-scheduler
	// does NOT read scheduling.x-k8s.io/v1alpha1 PodGroup objects, so
	// the gang contract degrades to per-pod scheduling silently.
	// Operators install scheduler-plugins as a secondary scheduler
	// (`scheduler-plugins-scheduler`) or as a default-scheduler plugin
	// (in which case the warning is a false positive the controller
	// can't detect from inside the cluster). Dedup'd per (owner,
	// Component) per process.
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
	// podAffinity. Advisory only — never blocks the create. Dedup'd per
	// (owner, Component) per process.
	EventReasonGangSplitRisk EventReason = "GangSplitRisk"
)
