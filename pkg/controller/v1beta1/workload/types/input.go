package types

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReconcileInput carries everything a workload reconcile needs as
// plain data. Callers populate this struct from their source-of-truth
// types; workload code reads it and never reaches back. The boundary
// lets one workload package serve both the ISVC and IR control
// planes without sharing a type system.
type ReconcileInput struct {
	// OwnerObject is the K8s object whose UID/Kind/APIVersion becomes
	// the controllerOwner on every emitted pod / revision / service /
	// PodGroup / PDB.
	OwnerObject client.Object

	// OwnerGVK is the GroupVersionKind stamped on emitted pods'
	// OwnerReference. Passed alongside OwnerObject because
	// controller-runtime strips TypeMeta during deserialization, so
	// the workload package cannot reliably derive the GVK.
	OwnerGVK schema.GroupVersionKind

	// EventTarget is the object emitted events are stamped against.
	// Usually OwnerObject; the IR caller may set this to the parent
	// ISVC instead so user-facing event streams stay coherent. Nil
	// falls back to OwnerObject.
	EventTarget client.Object

	// LedgerOwner owns the migration audit-ledger ConfigMap (load +
	// persist + its controller OwnerReference). Usually OwnerObject; the
	// IR caller sets it to the parent ISVC so the migration ledger lives
	// on the same user-facing resource as the operator's migration-request
	// annotation (the IR owns the pods; the ISVC owns the migration audit
	// trail). Nil falls back to OwnerObject. LedgerOwnerGVK is its GVK.
	LedgerOwner    client.Object
	LedgerOwnerGVK schema.GroupVersionKind

	// Key is the workload-owned identity. Adapters compose it from the
	// owner-CRD shape; workload code reads it opaquely.
	Key Key

	// DesiredSpec is the per-reconcile projection of the source spec
	// the workload pipeline drives toward.
	DesiredSpec WorkloadDesiredSpec

	// ObservedState is the per-reconcile snapshot of the source status
	// subtree. Read-only from workload code; status writes go through
	// MutateInstance.
	ObservedState WorkloadObservedState

	// MutateInstance applies the mutate callback to the idx-th
	// InstanceStatus entry. Callers wrap persistence (apiserver round-
	// trip under retry.RetryOnConflict, in-memory mirror) inside the
	// closure. The mutate callback returns true when a real change was
	// made; false short-circuits the status write.
	//
	// MUST be set when ObservedState carries any Instance entries —
	// workload code panics on a nil callback rather than silently
	// dropping status updates.
	MutateInstance func(ctx context.Context, idx int32, mutate func(*InstanceStatus) bool) error

	// ApplyInstanceMutations applies a batch of InstanceMutations in ONE
	// status write: one fresh read, every mutation applied to its slot,
	// one persist, retried on conflict as a whole. Only mutations whose
	// Mutate reports a change are persisted; a batch with no changes
	// writes nothing. Write-ahead mutations may be batched only when the
	// complete batch is persisted before any corresponding external effect.
	//
	// Optional: nil falls back to one MutateInstance call per mutation.
	ApplyInstanceMutations func(ctx context.Context, muts []InstanceMutation) error

	// ApplyInstanceMutationsWithRetryBlock atomically applies InstanceStatus
	// mutations and an optional RetryBlock mutation in one owner-status write.
	// The adapter re-reads once per conflict attempt, applies both mutation sets
	// to that fresh snapshot, and persists either all reported changes or none.
	// A nil mutateRetryBlock callback skips the RetryBlock mutation; an empty or
	// all-no-op mutation set writes nothing.
	//
	// Optional: nil preserves the separate InstanceStatus and RetryBlock writes
	// used by adapters that do not expose this stronger capability.
	ApplyInstanceMutationsWithRetryBlock func(ctx context.Context, muts []InstanceMutation, targetRevision string, mutateRetryBlock func(*RetryBlock) RetryBlockDisposition) error

	// ScaleUpPodBatchSize bounds the number of missing Pods selected by one
	// Create pass. Selection remains atomic at the Instance boundary, so a gang
	// is either selected in full or deferred. The first eligible Instance may
	// exceed a positive budget and proceeds alone. A nil pointer preserves the
	// unbounded compatibility behavior. A non-nil zero value fails closed.
	ScaleUpPodBatchSize *int32

	// ScaleDownPodBatchSize bounds active delete work in Pod-equivalent units.
	// Selection remains atomic at the Instance boundary, so a gang is either
	// selected in full or deferred. The first eligible Instance may exceed a
	// positive budget and proceeds alone. A nil pointer preserves unbounded
	// candidate selection. A non-nil zero value fails closed.
	ScaleDownPodBatchSize *int32

	// ScaleDownRequeueInterval is the configured poll cadence while a delete
	// wave is waiting on drain or resource disappearance. Zero disables cadence
	// polling; watched resources and exact force-delete deadlines still wake it.
	ScaleDownRequeueInterval time.Duration

	// AuthoritativePods is a live Component-wide Pod observation shared by
	// every destructive consumer in one reconcile pass. Nil means the caller
	// did not preload an observation; a non-nil snapshot is authoritative even
	// when Pods and ByInstance are empty.
	AuthoritativePods *ComponentPodSnapshot

	// FinalizeInstanceResources removes optional per-Instance resources after
	// the authoritative Pod snapshot is empty. It reports complete only after
	// those resources are authoritatively absent.
	FinalizeInstanceResources func(ctx context.Context, idx int32) (complete bool, err error)

	// RemoveInstance drops the InstanceStatus entry for idx and
	// returns (true, nil) when a real removal happened, (false, nil)
	// when already absent. Compatibility lifecycle paths use this seam
	// when they remove one status slot outside an atomic batch.
	//
	// It must be set whenever a caller can enter one of those paths; nil
	// fails closed rather than silently leaking a status entry.
	RemoveInstance func(ctx context.Context, idx int32) (bool, error)

	// WriteAggregateCondition merges cond into the owner's per-Component
	// condition list so workload code can stamp top-level Component-
	// scoped conditions (today only GangSchedulingUnavailable) without
	// reaching into the owner-CRD typed status.
	//
	// MUST be set on every constructed ReconcileInput — nil panics.
	// No-op adapters wire an explicit
	// `func(_ context.Context, _ metav1.Condition) error { return nil }`.
	WriteAggregateCondition func(ctx context.Context, cond metav1.Condition) error

	// WarnInstanceFailed emits a Warning event against EventTarget
	// reporting that the (idx) Instance escalated to Phase=Failed.
	//
	// MUST be set on every constructed ReconcileInput — nil panics.
	// No-op adapters wire `func(_ int32, _, _ string) {}`.
	WarnInstanceFailed func(idx int32, podName, reason string)

	// WarnRetryHeld emits the operator-facing Warning when a revision's
	// retry budget exhausts (spec: emitted at the Held transition, once).
	// Nil-safe: unset means no event.
	WarnRetryHeld func(targetRevision string, attempts int32, reason string)

	// MutateMigration reads-modifies-writes the owner's persisted
	// MigrationRecord for requestUUID (status.migrations). mutate
	// receives the existing record and returns true when a real change
	// was made; false short-circuits the status write. A missing record
	// (trimmed, or the owner was recreated) is a clean no-op — the
	// callback is not invoked.
	//
	// MUST be set when ObservedState.Migrations carries any records —
	// the Migrate executor errors on a nil callback rather than
	// silently dropping phase advancement.
	MutateMigration func(ctx context.Context, requestUUID string, mutate func(*MigrationRecord) bool) error

	// AppendMigration appends a NEW MigrationRecord to the owner's
	// persisted status.migrations. Idempotent: a record with the same
	// RequestUUID already present writes nothing. Deliberately separate
	// from MutateMigration — mutate-on-missing stays a no-op (a stamper
	// must never resurrect a trimmed record as a phantom), so record
	// CREATION gets its own seam. Optional: nil disables workload-side
	// record creation; callers treat the write as a best-effort mirror.
	AppendMigration func(ctx context.Context, rec MigrationRecord) error

	// UpdateGate, when non-nil, is called per Instance in the Update
	// pass before starting a fresh Update operation. Lets the adapter
	// inject cross-Component coordination gates (ratio-balanced
	// pacing, surge / unavailability budgets, sequential rollout
	// ordering) that workload code MUST NOT know about.
	//
	// allowed=false skips this Instance for this reconcile pass; the
	// dispatcher emits a short requeue. inFlightSurge / inFlightUnavail
	// are the dispatcher's within-pass counters so the gate can
	// project against the post-this-pass shape. gate names which layer
	// produced denyReason (RolloutHoldGateRatio / Sequential / Budget);
	// callers surfacing denyReason as a RolloutHold read gate from here
	// rather than parsing the message.
	//
	// Nil is treated as always-allowed.
	UpdateGate func(strategy UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (allowed bool, gate RolloutHoldGate, denyReason string)

	// RecordRolloutHold, when non-nil, is called at most once per Update
	// pass with the pass's verdict: non-nil when a StartingFresh Instance
	// was denied by the per-Component budget or UpdateGate and nothing
	// else progressed this pass, nil when the pass observed no denial or
	// an Update was admitted (forward progress clears any prior hold).
	// The adapter's status writer merges this against the same-target
	// RetryBlock/Held state it reads directly from persisted status, so
	// workload code never needs to know about RetryBlock semantics.
	RecordRolloutHold func(hold *RolloutHold)

	// MutateRetryBlock reads-modifies-writes the owner's persisted
	// RetryBlock for targetRevision. mutate receives the existing block
	// (or a zero block with TargetRevision set) and returns whether to
	// persist, remove, or leave it. Nil closure disables retry-block
	// WRITES only; the update-trigger gate still honors blocks present
	// in ObservedState.RetryBlocks.
	MutateRetryBlock func(ctx context.Context, targetRevision string, mutate func(*RetryBlock) RetryBlockDisposition) error

	// UpdateRetryPolicy bounds automatic same-target update retries.
	// nil = unconfigured → fail-safe: first failure Holds.
	UpdateRetryPolicy *RetryPolicy

	// ForceDelete gates the stuck-Terminating force-delete escalation
	// that scale-down, migration, and update teardown all share.
	// nil = unconfigured → the escalation is disabled entirely (does not
	// exist); when non-nil both durations are > 0 — config validation
	// guarantees it, consumers never re-check.
	ForceDelete *ForceDeletePolicy

	// StuckPodGrace is the wait window before the terminal-failure
	// escalation pass fast-fails an Instance on a pod parked in a
	// terminal kubelet waiting state. Operator config
	// (lifecycle.stuckPodGracePeriod); zero or negative disables fast
	// escalation (the InstanceReadyTimeout backstop still fires).
	StuckPodGrace time.Duration

	// Pacing collects the wake-up the apiserver itself asked for while
	// refusing writes during this pass. The op entry points report
	// progress as (done, error) and have no result channel of their own,
	// so a server-suggested Retry-After is deposited here and folded into
	// the pass's requeue by the dispatcher. Reconcile allocates one per
	// pass; nil is safe (every method no-ops) for adapters and tests that
	// call an op directly.
	Pacing *APIPacing

	// Disposition carries the operator-config inputs the terminal-failure
	// disposition (DisposeExpiredAttempt) branches on. The zero value
	// fails safe (no relocation, terminal branch for non-workload-caused
	// failures). MigrationMode is plan-derived: the escalation pass
	// overlays ComponentPlan.MigrationMode, so adapters leave it zero.
	Disposition DispositionDeps

	// Teardown marks this reconcile as owner-deletion teardown. When
	// set, the dispatcher treats the planned index set as empty — every
	// observed Instance is a scale-down extra and runs the scale-down batch
	// pipeline (drain gate, graceful delete, stuck-Terminating
	// force-delete escalation, audit) — and runs NOTHING else: no
	// Paused gate, no Restart / Migrate / Update / Create. The caller
	// owns completion detection (live component pod list) and all
	// finalizer decisions.
	Teardown bool

	// Clock mirrors Deps.Clock for helpers that receive only the input.
	// See Deps.Now for the rule.
	Clock clock.Clock
}

// ErrStatusOwnerGone reports that the object owning a requested atomic status
// transition disappeared before the transition could commit. Callers that
// guard an external effect must treat it as an aborted write, even though
// ordinary idempotent status writers may translate it to a successful no-op.
var ErrStatusOwnerGone = errors.New("status owner is gone")

// ErrStatusMutationPrecondition reports that an authoritative owner/status
// snapshot does not match the decision that produced a mutation batch.
// The complete batch is rejected and callers must replan before effects.
var ErrStatusMutationPrecondition = errors.New("status mutation precondition failed")

// ComponentPodSnapshot is one authoritative Component Pod LIST represented
// both as the complete set and as per-Instance buckets. OwnerUID identifies
// the controller owner that filtered the set. The two views share Pod pointers
// and are immutable for the lifetime of a reconcile pass.
type ComponentPodSnapshot struct {
	OwnerUID   k8stypes.UID
	Pods       []*corev1.Pod
	ByInstance map[int32][]*corev1.Pod
}

// InstanceMutationSnapshot is the authoritative owner/status view presented
// to a batch precondition on every conflict attempt.
type InstanceMutationSnapshot struct {
	OwnerUID        k8stypes.UID
	OwnerGeneration int64
	Instances       map[int32]InstanceStatus
}

// InstanceMutation is one buffered InstanceStatus mutation, keyed by Instance
// index. An upsert sets Mutate; a removal sets Remove and leaves Mutate nil.
// Precondition can reject a mutation against the fresh status snapshot.
// OnCommit receives isolated copies of the exact before/after values only after
// the containing status write commits; nil identifies an absent side. A
// mutation with OnCommit requires its index to appear only once in the batch.
type InstanceMutation struct {
	Index        int32
	Mutate       func(*InstanceStatus) bool
	Remove       bool
	Precondition func(*InstanceStatus) bool
	// BatchPrecondition guards the complete mutation set. When any batch
	// precondition rejects the fresh snapshot, no mutation in the set is
	// applied. Callers normally attach one shared guard to the first mutation.
	BatchPrecondition func(InstanceMutationSnapshot) bool
	// Postcondition identifies this mutation's committed representation after
	// an ambiguous status-update response. Every mutation in a confirmable
	// batch supplies one; removals are confirmed by authoritative absence.
	Postcondition func(*InstanceStatus) bool
	OnCommit      func(previous, current *InstanceStatus)
}

// DispositionDeps carries the operator-config inputs and adapter hooks
// the disposition branches on. Resolved once per reconcile by the
// adapter.
type DispositionDeps struct {
	// AutoMigrateMaxAttempts bounds the relocation branch per
	// (component, instance) against the audit ledger's AutoRecover
	// entry count. <= 0 (unconfigured) disables relocation entirely.
	AutoMigrateMaxAttempts int32
	// MigrationMode is the effective migration disposition from the
	// plan (MigrationModeOrDefault). Only Auto (and its Surge spelling
	// alias) enables relocation; the zero value fails safe to the
	// terminal branch.
	MigrationMode MigrationMode
	// PodSpec / WorkerPodSpec are the Component's desired pod templates
	// (leader/single-pod and worker role). The relocation branch
	// consults their REQUIRED node affinity before recording a
	// directive: when excluding the suspect node (plus the instance's
	// already-recorded exclusions) would leave a template unschedulable
	// — e.g. a required In[node] pin on the wedged node — the attempt
	// disposes terminal instead of recording an unsatisfiable exclusion
	// that would leave the rebuild permanently Pending. Nil skips the
	// guard.
	PodSpec       *corev1.PodSpec
	WorkerPodSpec *corev1.PodSpec
	// OnRelocationDirective, when non-nil, is invoked once per recorded
	// relocation directive with the component name — the adapter's
	// metrics hook (the IR adapter wires the auto-migration counter).
	OnRelocationDirective func(component string)
}

// ForceDeletePolicy configures the stuck-Terminating force-delete
// escalation: a Terminating pod overdue past its own deletion deadline
// by OverdueSlack, on a node whose unreachable evidence is at least
// NodeUnreachableThreshold old, may be force-deleted. Config-driven
// (chart values → inferenceservice-config); nil means unconfigured and
// the escalation is disabled entirely. Both durations are always > 0
// when the policy is non-nil (config validation rejects anything else).
type ForceDeletePolicy struct {
	// OverdueSlack is how long past the pod's own DeletionTimestamp
	// (which already includes the pod's own grace period) a Terminating
	// pod must be before it counts as wedged.
	OverdueSlack time.Duration
	// NodeUnreachableThreshold is the minimum age of the node's
	// unreachable evidence (taint TimeAdded / NotReady
	// LastTransitionTime, or the Node object gone) before the
	// escalation may act.
	NodeUnreachableThreshold time.Duration
}

// Now returns the injected clock's time, or time.Now() when no clock
// is wired. Lifecycle code holding a Deps or ReconcileInput should read
// time through Now, not time.Now(); subpackages without a seam
// (audit, podreadiness, gang) still read real time.
func (r *ReconcileInput) Now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}
