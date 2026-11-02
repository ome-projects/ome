package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// irGVK is the GroupVersionKind every IR-driven workload reconcile
// stamps on emitted pods / revisions / services. Decoded objects often
// have an empty TypeMeta (controller-runtime strips it on Get), so the
// workload package cannot derive the GVK from OwnerObject — the
// reconciler passes the correct value alongside via
// ReconcileInput.OwnerGVK.
var irGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceReplica")

// isvcGVK is the parent InferenceService's GVK — stamped as the owner of
// the migration audit-ledger ConfigMap (input.LedgerOwnerGVK) so the
// ledger lives on the user-facing ISVC, not the IR.
var isvcGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// IRGVK returns the InferenceReplica GroupVersionKind. Exposed so
// the reconciler can pass it directly to revision/podgroup helpers
// without re-deriving it from the scheme.
func IRGVK() schema.GroupVersionKind { return irGVK }

// buildReconcileInput projects (IR.Spec, IR.Status) onto a
// workload.ReconcileInput so the workload dispatcher can drive the
// per-Instance pipeline without taking on a backward import to
// v1beta1.InferenceReplica. THE bridge between the IR adapter's
// source-of-truth shape and the workload package's plain-data carrier.
//
// OwnerObject is the IR itself: every emitted pod / ControllerRevision
// / PodGroup is GC'd through the IR's controller OwnerReference.
//
// OwnerName is ir.NamePrefix(): the parent InferenceService name for a
// projected replica, the replica's own name for a standalone one. Pod
// names (<prefix>-<component>-<idx>-<runner>-<ord>), Service names and
// the HPA scale-selector formula all key off it. The SelectorLabels are
// the OMENative trio (name prefix, component, managed-by) the renderer
// stamps on every pod.
//
// EventTarget defaults to the parent ISVC when the caller passes a
// non-nil parent (so user-facing event streams stay coherent under
// `kubectl describe isvc`). When parent is nil — a standalone replica,
// or the parent fetch failed — EventTarget falls back to the IR itself
// so events still land somewhere observable.
//
// Migration work flows from IR.Status.Migrations (mirrored onto
// ObservedState.Migrations): the workload dispatcher selects the oldest
// non-terminal Manual record and MutateMigration is the write-back seam
// for phase advancement. The migration-request annotation is consumed
// into status.migrations by consumeMigrationRequests before dispatch.
//
// The coordination UpdateGate / DrainGate are not wired here: they read
// the pass's roll target, which is resolved after the input is built, so
// wireCoordinationGates installs them once it is known.
//
// Taking the typed *InferenceService (not client.Object) is deliberate:
// a nil parent passed as client.Object becomes a non-nil interface
// wrapping a nil pointer, so the `parent != nil` guards below (event
// target + ledger owner) would both misfire. The typed parameter makes
// the nil check correct.
// updateRetryPolicy is the same-target update retry policy resolved
// ONCE per reconcile by the caller (from the operator lifecycle config);
// nil means unconfigured and the workload layer fails safe (first
// same-target failure Holds).
//
// forceDeletePolicy is the stuck-Terminating force-delete policy,
// likewise resolved ONCE per reconcile by the caller; nil means
// unconfigured and the escalation is disabled entirely.
//
// settings and autoMigrateBudget carry the operator lifecycle tunables:
// the escalation and settle windows (a zero field disables that window)
// and the relocation budget of the terminal-failure disposition (zero
// disables the relocation branch). Both resolved ONCE per reconcile by
// the caller from the operator lifecycle config.
//
// coordDefaults carries the operator-configured group resolution
// fill-ins (from the coordination config), likewise resolved ONCE per
// reconcile by the caller; the zero value means unconfigured and each
// knob uses its documented unconfigured behavior.
//
// runners are the pod templates of this pass: the replica's stored runners,
// or the runners rendered from its model and runtime references, which are
// never stored on the replica.
func (r *Reconciler) buildReconcileInput(ctx context.Context, ir *v1beta1.InferenceReplica, runners []v1beta1.Runner, parent *v1beta1.InferenceService, updateRetryPolicy *workloadtypes.RetryPolicy, forceDeletePolicy *workloadtypes.ForceDeletePolicy, settings lifecycleSettings, autoMigrateBudget int32, coordDefaults coordination.GroupDefaults) workloadtypes.ReconcileInput {
	desired := desiredFromIR(ir, runners)
	// The parent annotation is the operator-facing source of truth whenever the
	// parent is readable. This deliberately overrides both stale true and stale
	// false values on the projected IR: a component render/projector failure must
	// not let an in-flight IR continue after the operator pauses, and annotation
	// removal must resume it without waiting for a successful projection pass.
	// When the parent is unavailable, fall back to the last projected IR value.
	if parent != nil {
		desired.Paused, desired.PauseFreeze = constants.RolloutPauseState(parent.Annotations)
	}
	// Thread the controller's cached gang-availability into DesiredSpec —
	// EnsurePodGroups gates per-Instance PodGroup creation on this flag.
	desired.GangSchedulingAvailable = r.GangSchedulingAvailable
	observed := observedFromIR(ir)
	eventTarget := client.Object(ir)
	if parent != nil {
		eventTarget = parent
	}
	applyInstanceMutationsWithRetryBlock := buildApplyInstanceMutationsWithRetryBlockFromReader(r.statusWriter(), r.liveReader(), ir, settings.RetryBlockHistoryLimit)
	input := workloadtypes.ReconcileInput{
		OwnerObject:                          ir,
		OwnerGVK:                             irGVK,
		EventTarget:                          eventTarget,
		Key:                                  buildKey(ir),
		DesiredSpec:                          desired,
		ObservedState:                        observed,
		MutateInstance:                       buildMutateInstance(r.statusWriter(), r.liveReader(), ir),
		ApplyInstanceMutations:               instanceOnlyMutationAdapter(applyInstanceMutationsWithRetryBlock),
		ApplyInstanceMutationsWithRetryBlock: applyInstanceMutationsWithRetryBlock,
		RemoveInstance:                       buildRemoveInstance(r.statusWriter(), r.liveReader(), ir, r.Expectations),
		WriteAggregateCondition:              buildWriteAggregateCondition(r.statusWriter(), r.liveReader(), ir),
		// Same-target RetryBlock persistence + policy. The
		// closure is always wired on the IR-managed path — the policy
		// alone decides Backoff vs fail-safe Held.
		MutateRetryBlock:  buildMutateRetryBlock(r.statusWriter(), r.liveReader(), ir, settings.RetryBlockHistoryLimit),
		UpdateRetryPolicy: updateRetryPolicy,
		// Migration-record persistence: the executor advances
		// status.migrations phases through this seam (same RMW +
		// in-memory-mirror discipline as MutateRetryBlock).
		MutateMigration: buildMutateMigration(r.statusWriter(), r.liveReader(), ir),
		// Record creation (born-terminal Auto mirrors from the
		// disposition). Separate from MutateMigration so mutate-on-
		// missing stays a structural no-op.
		AppendMigration: buildAppendMigration(r.statusWriter(), r.liveReader(), ir),
		// Stuck-Terminating force-delete gate. nil disables the
		// escalation; non-nil durations are validated > 0 upstream.
		ForceDelete: forceDeletePolicy,
		// Stuck-pod fast-escalation window for the workload escalation
		// pass. Zero (unconfigured) disables fast escalation; the
		// InstanceReadyTimeout backstop still fires.
		StuckPodGrace: settings.StuckPod,
		// Scheduler-hold escalation window, shared by the pod-level and
		// gang-level holds. Zero (unconfigured) leaves an unplaceable pod
		// or gang parking the deadline until an operator acts.
		UnschedulableGrace: settings.Unschedulable,
		// Shorter window for a gang held to a domain that cannot take it.
		// Zero (unconfigured) leaves such a hold on UnschedulableGrace.
		GangDomainFullGrace: settings.GangDomainFull,
		// Termination grace of a replacement a rollout abandons before it
		// ever served. Zero (unconfigured) leaves such a pod its own grace.
		AbandonedReplacementGrace: settings.AbandonedReplacement,
		// Migration admission caps. Nil (unconfigured) holds requests:
		// the caps are the only bound on destructive migration churn.
		MigrationAudit: settings.MigrationAudit,
		// Dispatcher wake-up cadence. Zero fields (unconfigured) leave a
		// pass with work left to the rate-limited backoff.
		Requeue: settings.Requeue,
		// Per-Instance PodGroup classification for this pass. The PodGroup
		// pass fills it before the dispatcher; the pod-create choke point
		// and the escalation pass read it back.
		Gangs: workloadtypes.NewGangObservations(),
		Clock: r.Clock,
		// WarnInstanceFailed bridges the workload escalation pass to the
		// reconciler's event recorder so the operator sees the
		// per-Instance Failed escalation against the EventTarget (parent
		// ISVC when set, else IR).
		WarnInstanceFailed: r.warnInstanceFailedFunc(ir, eventTarget),
		// WarnRetryHeld surfaces the terminal same-target retry exhaustion
		// (RetryBlock → Held) to the operator. Emitted by the workload
		// writer exactly once, at the transition into Held.
		WarnRetryHeld: r.warnRetryHeldFunc(ir, eventTarget),
	}
	// Operator-config inputs of the terminal-failure disposition the
	// escalation pass runs. The template specs feed the relocation
	// branch's required-affinity guard: an exclusion the template can
	// never satisfy (required In pin on the suspect node) disposes
	// terminal instead of recording. Captured from the spec-rendered
	// templates here — a canary rollback later swaps DesiredSpec's
	// templates, and the disposition judges the spec's own affinity, not
	// the rollback overlay's. MigrationMode is plan-derived and overlaid
	// by the escalation pass.
	input.Disposition = workloadtypes.DispositionDeps{
		AutoMigrateMaxAttempts: autoMigrateBudget,
		PodSpec:                desired.PodSpec,
		WorkerPodSpec:          desired.WorkerPodSpec,
		// Count each recorded relocation directive on the auto-migration
		// counter, keyed by the name prefix (the InferenceService name for
		// a projected replica).
		OnRelocationDirective: func(component string) {
			coordination.RecordAutoMigrationTriggered(ir.Namespace, ir.NamePrefix(), component, audit.ReasonAutoRecover)
		},
	}

	if policy := ir.Spec.PlacementExecution; policy != nil {
		input.PauseNewSurge = policy.PauseSurge || protocol.Validate(policy) != nil
	}
	// The migration audit ledger (history) lives on the user-facing
	// parent ISVC when resolvable; Migrate drives the IR's own pods and
	// resumes from IR.Status.Migrations. Nil parent (brief foreground-GC
	// window) → the ledger falls back to the IR via the workload-side
	// owner resolution.
	if parent != nil {
		input.LedgerOwner = parent
		input.LedgerOwnerGVK = isvcGVK
	}

	return input
}

// wireCoordinationGates installs the coordination UpdateGate and DrainGate
// on input, so a projected replica enforces its parent's Sequential /
// RatioBalanced / surge / unavailability / pairing rules through the shared
// coordination decision sites. Without them a projected replica would skip
// ALL cross-Component coordination (both Components of a Sequential group
// recreated concurrently; a group MaxSurge never capping a Component whose
// per-Component budget is larger).
//
// target is the ControllerRevision this pass moves Instances to: the
// rollback revision while a rollback is pinned, else the spec target. The
// pairing gate reads the target cohort's protocol from it, so a canary
// revert is simulated toward the stable cohort rather than toward the live
// spec field, which still names the rejected protocol until the rollback
// completes. Wired after the roll target is resolved for that reason.
//
// The gates read the parent ISVC's rollout block and authoritative
// per-Component IR status, so they only run when the parent is resolvable;
// a nil parent means there is no coordination block to enforce and the
// dispatcher's nil-gate fallback ("always allowed") is correct. A placement
// execution policy wraps the coordination gate with its own holds.
func (r *Reconciler) wireCoordinationGates(ctx context.Context, input *workloadtypes.ReconcileInput, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, coordDefaults coordination.GroupDefaults, target *appsv1.ControllerRevision) {
	if parent != nil {
		// The gates read peer Component status, which must be live:
		// cross-Component coordination against a cache-lagged peer would
		// admit a rollout the peer's real state forbids.
		input.UpdateGate = func(fromRevision string, strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
			allowed, gate, reason := coordination.EvaluateUpdateGate(ctx, r.liveReader(), parent, ir.Spec.Component, fromRevision, target, r.Recorder, coordDefaults, strategy, inFlightSurge, inFlightUnavail)
			return allowed, workloadtypes.RolloutHoldGate(gate), reason
		}
		// The pin alone, for the starts the workload admits without the
		// capacity consult: an Instance that serves nothing still rolls on
		// the plan.
		input.PlanGate = func() (bool, workloadtypes.RolloutHoldGate, string) {
			allowed, gate, reason := coordination.EvaluatePlanGate(parent, ir.Spec.Component)
			return allowed, workloadtypes.RolloutHoldGate(gate), reason
		}
		// The drain-time half of the pair floor: a surge update asks it with
		// the replacement already serving, over live pod state.
		input.DrainGate = func(sourcePods []string) (bool, workloadtypes.RolloutHoldGate, string) {
			allowed, gate, reason := coordination.EvaluatePairingDrain(ctx, r.liveReader(), parent, ir.Spec.Component, target, coordDefaults, sourcePods)
			return allowed, workloadtypes.RolloutHoldGate(gate), reason
		}
		// A held drain is reported through the same RolloutHold as a held
		// start; without a collector the gate would hold silently.
		input.DrainHolds = &workloadtypes.DrainHolds{}
	}
	if policy := ir.Spec.PlacementExecution; policy != nil {
		coordinationGate := input.UpdateGate
		input.UpdateGate = func(fromRevision string, strategy workloadtypes.UpdateStrategyType, surge, unavailable int32) (bool, workloadtypes.RolloutHoldGate, string) {
			if err := protocol.Validate(policy); err != nil {
				return false, workloadtypes.RolloutHoldGateBudget, err.Error()
			}
			if policy.PauseSurge && strategy == workloadtypes.UpdateStrategySurgeThenDrain {
				return false, workloadtypes.RolloutHoldGateBudget, "placement transition holds new member surge"
			}
			if coordinationGate != nil {
				return coordinationGate(fromRevision, strategy, surge, unavailable)
			}
			return true, "", ""
		}
	}
}

// instanceFailedMsg formats the operator-facing InstanceFailed Warning:
// (namespace, name, component, instance, pod, reason). podName is empty
// for the deadline backstop (no single pod to blame); reason carries the
// cause either way (kubelet waiting-state reason for the stuck-pod path,
// "DeadlineExceeded: <op>/<step> ..." for the deadline path).
const instanceFailedMsg = "InferenceReplica %s/%s component=%s instance=%d escalated to Phase=Failed (pod=%q): %s"

// warnInstanceFailedFunc returns the WarnInstanceFailed closure: mirror
// the escalation onto the controller log (operator-facing;
// log-aggregated views see it without the events stream), then emit the
// InstanceFailed Warning against eventTarget (user-facing).
func (r *Reconciler) warnInstanceFailedFunc(ir *v1beta1.InferenceReplica, eventTarget client.Object) func(idx int32, podName, reason string) {
	return func(idx int32, podName, reason string) {
		r.Log.Info("Instance escalated to Phase=Failed",
			"inferencereplica", client.ObjectKeyFromObject(ir),
			"component", ir.Spec.Component,
			"instance", idx,
			"pod", podName,
			"reason", reason)
		if r.Recorder == nil {
			return
		}
		r.Recorder.Eventf(eventTarget, corev1.EventTypeWarning, string(workloadtypes.EventReasonInstanceFailed),
			instanceFailedMsg, ir.Namespace, ir.Name, ir.Spec.Component, idx, podName, reason)
	}
}

// retryHeldMsg formats the operator-facing RetryHeld Warning. A Held block
// stays until a new target revision arrives or an operator releases it, so
// the message names both exits; raising the retry limits releases nothing.
const retryHeldMsg = "InferenceReplica %s/%s component=%s update to revision %s held after %d failed attempt(s) (last failure: %s); " +
	"publish a corrected revision, or fix the cause and set annotation %s=%s on the InferenceReplica to release the hold"

// warnRetryHeldFunc returns the WarnRetryHeld closure: surface the
// terminal same-target retry exhaustion (RetryBlock → Held) on the
// controller log and as a RetryHeld Warning against eventTarget. The
// workload writer invokes it exactly once, at the transition into Held.
func (r *Reconciler) warnRetryHeldFunc(ir *v1beta1.InferenceReplica, eventTarget client.Object) func(targetRevision string, attempts int32, reason string) {
	return func(targetRevision string, attempts int32, reason string) {
		r.Log.Info("Same-target update retries exhausted; revision Held",
			"inferencereplica", client.ObjectKeyFromObject(ir),
			"component", ir.Spec.Component,
			"targetRevision", targetRevision,
			"attempts", attempts,
			"reason", reason)
		if r.Recorder == nil {
			return
		}
		r.Recorder.Eventf(eventTarget, corev1.EventTypeWarning, string(workloadtypes.EventReasonRetryHeld), retryHeldMsg,
			ir.Namespace, ir.Name, ir.Spec.Component, targetRevision, attempts, reason,
			constants.ReleaseHeldRevisionAnnotationKey, targetRevision)
	}
}

// buildHeadlessServiceSpec adapts an InferenceReplica into the typed
// workload.PerComponentServiceSpec the workload renderer consumes.
// The IR owns its per-Component headless Service so deletion of the IR
// cascades to the Service; the IR controller is the Service's sole
// writer.
//
// OwnerName is ir.NamePrefix(): the parent InferenceService name for a
// projected replica, the replica's own name for a standalone one. The
// Service is named <prefix>-<component>-headless.
//
// Selector matches the labels the workload renderer stamps on pods
// (see render.go::podLabels): ome.io/inferenceservice + component +
// managed-by=OMENative.
func buildHeadlessServiceSpec(ir *v1beta1.InferenceReplica) workloadtypes.PerComponentServiceSpec {
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: ir.NamePrefix(),
		constants.OMEComponentLabel:           string(ir.Spec.Component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	return workloadtypes.PerComponentServiceSpec{
		Name:      query.HeadlessServiceName(ir.NamePrefix(), v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)),
		Namespace: ir.Namespace,
		Selector:  selector,
		Labels:    selector,
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(ir, irGVK),
		},
	}
}

// desiredFromIR projects the runners + IR.Spec.Lifecycle onto the
// workload.WorkloadDesiredSpec the workload dispatcher reads. runners are
// the replica's stored runners or the ones rendered from its references.
//
// PodSpec/WorkerPodSpec come from the Runner.Template's PodSpec.
// PodTemplateObjectMeta is derived from the Runner.Template's
// ObjectMeta so user-intent pod-template labels/annotations land on
// every emitted pod.
func desiredFromIR(ir *v1beta1.InferenceReplica, runners []v1beta1.Runner) workloadtypes.WorkloadDesiredSpec {
	replicas := int32(1)
	if ir.Spec.Replicas != nil && (*ir.Spec.Replicas > 0 || *ir.Spec.Replicas == 0 && protocol.HasZeroReplicaFloor(ir.Spec.PlacementExecution, ir.Spec.Component)) {
		replicas = *ir.Spec.Replicas
	}
	if policy := ir.Spec.PlacementExecution; policy != nil && policy.PauseSurge && ir.Spec.PlacementReplicaLimit != nil {
		replicas = min(replicas, *ir.Spec.PlacementReplicaLimit)
	}
	// Pacing.Partition is the rollout-control partition the engine's
	// update hold reads ahead of the user's Lifecycle partition;
	// Pacing.MaxUnavailable is copied but has no producer and no reader
	// (see WorkloadPacing). Pacing.RollbackToRevision is handled in
	// reconciler.go.
	desired := workloadtypes.WorkloadDesiredSpec{
		Replicas:          replicas,
		AllowZeroReplicas: protocol.HasZeroReplicaFloor(ir.Spec.PlacementExecution, ir.Spec.Component),
		MinReadySeconds:   ir.Spec.MinReadySeconds,
		Runners:           runnersFromIR(runners),
		Paused:            ir.Spec.Paused,
		PauseFreeze:       ir.Spec.Paused && ir.Spec.PauseMode == v1beta1.PauseModeFreeze,
	}
	if ir.Spec.TopologyKey != nil {
		desired.TopologyKey = *ir.Spec.TopologyKey
	}
	if ir.Spec.TopologySpread != nil {
		desired.TopologySpread = string(*ir.Spec.TopologySpread)
	}
	if ir.Spec.TopologySpreadKey != nil {
		desired.TopologySpreadKey = *ir.Spec.TopologySpreadKey
	}
	if ir.Spec.PairingProtocol != nil {
		desired.PairingProtocol = *ir.Spec.PairingProtocol
	}
	if ir.Spec.Pacing != nil {
		desired.Pacing = &workloadtypes.WorkloadPacing{
			Partition:      ir.Spec.Pacing.Partition,
			MaxUnavailable: ir.Spec.Pacing.MaxUnavailable,
		}
	}
	if ir.Spec.Lifecycle != nil {
		desired.Lifecycle = v1beta1convert.LifecycleSpecToWorkload(*ir.Spec.Lifecycle)
	}
	// The per-role PodSpec and ObjectMeta come off the runners through the
	// one reader the revision hash and the projector's template hash share.
	templates := v1beta1convert.PodTemplatesOf(runners)
	desired.PodSpec = templates.Primary
	desired.PodTemplateObjectMeta = templates.Meta
	desired.WorkerPodSpec = templates.Worker
	desired.MultiPod = templates.MultiPod
	return desired
}

// validatePlacementReplicaLimit runs before workload actions and generation
// acknowledgement. A pruned or unresolved limit cannot grant member growth.
func validatePlacementReplicaLimit(ir *v1beta1.InferenceReplica) error {
	policy := ir.Spec.PlacementExecution
	if policy == nil || !policy.PauseSurge {
		return nil
	}
	if err := protocol.Validate(policy); err != nil {
		return err
	}
	if ir.Spec.PlacementReplicaLimit == nil || *ir.Spec.PlacementReplicaLimit <= 0 {
		return fmt.Errorf("placement pause requires a persisted positive replica limit")
	}
	return nil
}

// runnersFromIR converts a Runner list to the workload.Runner
// projection. The names round-trip 1:1 (RunnerNameDefault → "default",
// RunnerNameLeader → "leader", RunnerNameWorker → "worker"); the size
// is the per-Instance pod count for that role.
func runnersFromIR(runners []v1beta1.Runner) []workloadtypes.Runner {
	if len(runners) == 0 {
		return nil
	}
	out := make([]workloadtypes.Runner, 0, len(runners))
	for _, r := range runners {
		out = append(out, workloadtypes.Runner{
			Name: string(r.Name),
			Size: r.Size,
		})
	}
	return out
}

// observedFromIR projects IR.Status onto the workload.WorkloadObservedState
// the dispatcher reads on each pass. Conditions are NOT propagated:
// the workload package writes back into the same shape via
// WriteAggregateCondition, so reading them in here would double-stamp
// transition timestamps.
func observedFromIR(ir *v1beta1.InferenceReplica) workloadtypes.WorkloadObservedState {
	return workloadtypes.WorkloadObservedState{
		ObservedGeneration: ir.Status.ObservedGeneration,
		CollisionCount:     ir.Status.CollisionCount,
		CurrentRevision:    ir.Status.CurrentRevision,
		UpdateRevision:     ir.Status.UpdateRevision,
		InstanceStatuses:   v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses),
		RetryBlocks:        retryBlocksFromIR(ir),
		Migrations:         migrationsFromIR(ir),
	}
}

// retryBlocksFromIR mirrors IR.Status.RetryBlocks onto the workload-side
// RetryBlock shape. Field-for-field; timestamps are deep-copied so the
// workload mirror never aliases the IR's status slice.
func retryBlocksFromIR(ir *v1beta1.InferenceReplica) []workloadtypes.RetryBlock {
	if len(ir.Status.RetryBlocks) == 0 {
		return nil
	}
	out := make([]workloadtypes.RetryBlock, len(ir.Status.RetryBlocks))
	for i := range ir.Status.RetryBlocks {
		out[i] = retryBlockToWorkload(ir.Status.RetryBlocks[i])
	}
	return out
}

// retryBlockToWorkload converts one v1beta1.RetryBlock to the workload
// mirror. Timestamps copy pointer-safely (DeepCopy) like the
// instance-status conversion does for its pointer fields.
func retryBlockToWorkload(v v1beta1.RetryBlock) workloadtypes.RetryBlock {
	return workloadtypes.RetryBlock{
		TargetRevision:  v.TargetRevision,
		State:           workloadtypes.RetryBlockState(v.State),
		AttemptsStarted: v.AttemptsStarted,
		NextRetryAt:     v.NextRetryAt.DeepCopy(),
		FirstFailureAt:  v.FirstFailureAt.DeepCopy(),
		LastFailureAt:   v.LastFailureAt.DeepCopy(),
		Reason:          v.Reason,
	}
}

// retryBlockFromWorkload is the inverse of retryBlockToWorkload.
func retryBlockFromWorkload(w workloadtypes.RetryBlock) v1beta1.RetryBlock {
	return v1beta1.RetryBlock{
		TargetRevision:  w.TargetRevision,
		State:           v1beta1.RetryBlockState(w.State),
		AttemptsStarted: w.AttemptsStarted,
		NextRetryAt:     w.NextRetryAt.DeepCopy(),
		FirstFailureAt:  w.FirstFailureAt.DeepCopy(),
		LastFailureAt:   w.LastFailureAt.DeepCopy(),
		Reason:          w.Reason,
	}
}

// buildKey is the IR-side workload.Key projection. The SelectorLabels
// are the OMENative trio (name prefix, component, managed-by) the
// workload renderer stamps on every pod it creates.
//
// OwnerName is ir.NamePrefix(): the parent InferenceService name for a
// projected replica, the replica's own name for a standalone one.
// query.PodName and query.HeadlessServiceName key off this field, and
// irRevisionKey derives revision names from the same prefix.
func buildKey(ir *v1beta1.InferenceReplica) workloadtypes.Key {
	return workloadtypes.Key{
		Namespace:   ir.Namespace,
		Component:   v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		OwnerName:   ir.NamePrefix(),
		OwnerLabels: ir.Labels,
		SelectorLabels: map[string]string{
			constants.InferenceServicePodLabelKey: ir.NamePrefix(),
			constants.OMEComponentLabel:           string(ir.Spec.Component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// buildMutateInstance wraps the workload-typed mutate callback with an
// IR-typed apiserver round-trip: re-read under retry.RetryOnConflict,
// locate/append the (idx) InstanceStatus, hand the workload-typed mirror
// to the caller, convert back to v1beta1 if the caller reported a real
// change, and persist via Status().Update().
//
// The boolean return lets the workload caller skip the apiserver
// round-trip when nothing changed, so per-op writers stay idempotent.
//
// Owner disappearance or replacement returns ErrStatusOwnerGone so callers
// stop before applying effects selected from the stale snapshot.
func buildMutateInstance(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	return func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		var committed *v1beta1.OMENativeInstanceStatus
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			committed = nil
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workloadtypes.ErrStatusOwnerGone
			}
			insts := fresh.Status.InstanceStatuses
			pos := -1
			for i, s := range insts {
				if s.Index == idx {
					pos = i
					break
				}
			}
			var slot *v1beta1.OMENativeInstanceStatus
			if pos == -1 {
				insts = append(insts, v1beta1.OMENativeInstanceStatus{Index: idx})
				slot = &insts[len(insts)-1]
			} else {
				slot = &insts[pos]
			}
			w := v1beta1convert.InstanceStatusToWorkload(*slot)
			read := w.Phase
			if !mutate(&w) {
				return nil
			}
			*slot = encodeMutatedInstance(slot.Phase, read, w)
			fresh.Status.InstanceStatuses = insts
			if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			for i := range fresh.Status.InstanceStatuses {
				if fresh.Status.InstanceStatuses[i].Index == idx {
					committed = fresh.Status.InstanceStatuses[i].DeepCopy()
					break
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if committed != nil {
			mirrorInstanceStatus(ir, *committed)
		}
		return nil
	}
}

// buildApplyInstanceMutations exposes the instance-only view of the atomic
// status mutation capability. The shared implementation keeps instance-only
// callers and callers that also transition a RetryBlock on the same conflict,
// no-op, and in-memory-mirror semantics.
func buildApplyInstanceMutations(writer statusWriter, ir *v1beta1.InferenceReplica) func(ctx context.Context, muts []workloadtypes.InstanceMutation) error {
	// Instance-only mutations never transition a RetryBlock, so the
	// retention prune is unreachable from here.
	return instanceOnlyMutationAdapter(buildApplyInstanceMutationsWithRetryBlock(writer, ir, nil))
}

func instanceOnlyMutationAdapter(apply func(context.Context, []workloadtypes.InstanceMutation, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error) func(context.Context, []workloadtypes.InstanceMutation) error {
	return func(ctx context.Context, muts []workloadtypes.InstanceMutation) error {
		return apply(ctx, muts, "", nil)
	}
}

type committedInstanceMutation struct {
	index    int32
	onCommit func(previous, current *workloadtypes.InstanceStatus)
	previous *workloadtypes.InstanceStatus
	current  *workloadtypes.InstanceStatus
}

// buildApplyInstanceMutationsWithRetryBlock applies a batch of InstanceStatus
// mutations and an optional RetryBlock mutation to one fresh IR snapshot and
// persists their combined result in one Status().Update. A conflict retries
// the complete mutation set. Missing InstanceStatus entries are appended only
// when their callback reports a change. Instance removals share the same O(N)
// indexed pass and cause the complete committed InstanceStatus slice to be
// mirrored in memory. Removing a missing InstanceStatus or RetryBlock and all
// other no-op combinations write nothing. A missing owner returns
// ErrStatusOwnerGone so callers can suppress the corresponding external
// effect. Committed values are mirrored onto the caller's IR so later work in
// the same reconcile observes exactly the persisted state.
func buildApplyInstanceMutationsWithRetryBlock(writer statusWriter, ir *v1beta1.InferenceReplica, historyLimit *int32) func(ctx context.Context, muts []workloadtypes.InstanceMutation, targetRevision string, mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
	return buildApplyInstanceMutationsWithRetryBlockFromReader(writer, irstatus.NewReader(writer.Client, writer.decoder), ir, historyLimit)
}

// buildApplyInstanceMutationsWithRetryBlockFromReader uses an authoritative
// reader for conflict retries and the client for status writes. The split is
// required for adjacent writes in one reconcile: the informer cache may not
// observe the first write before the next mutation starts.
//
// Each attempt reads the replica live, applies the batch to that copy
// through an instanceBatchWrite, writes the copy, and reads back the rows
// the apiserver stored for the caller's copy and the commit callbacks. The
// rows of the API object are read and written here alone; the record works
// on copies.
func buildApplyInstanceMutationsWithRetryBlockFromReader(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica, historyLimit *int32) func(ctx context.Context, muts []workloadtypes.InstanceMutation, targetRevision string, mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	return func(ctx context.Context, muts []workloadtypes.InstanceMutation, targetRevision string, mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		if err := validateInstanceMutations(muts); err != nil {
			return err
		}
		if len(muts) == 0 && mutateRetryBlock == nil {
			return nil
		}
		write := &instanceBatchWrite{mutations: muts, targetRevision: targetRevision, mutateRetryBlock: mutateRetryBlock, historyLimit: historyLimit}
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			write.beginAttempt()
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workloadtypes.ErrStatusOwnerGone
			}
			rows := indexInstanceRows(fresh.Status.InstanceStatuses, len(muts))
			if err := write.checkBatchPreconditions(fresh, rows); err != nil {
				return err
			}
			write.applyInstanceMutations(rows)
			if write.instanceChanged {
				fresh.Status.InstanceStatuses = rows.inStoredOrder()
			}
			write.applyRetryBlockMutation(&fresh.Status)
			if !write.instanceChanged && !write.retryBlockChanged {
				return nil
			}
			write.statusWriteAttempted = true
			if fresh, err = write.persist(ctx, writer, reads, key, fresh, source); err != nil {
				return err
			}
			// The rows the apiserver stored, by Instance: the caller's copy
			// and the commit callbacks read these, never the attempt's own.
			persisted := make(map[int32]v1beta1.OMENativeInstanceStatus, len(fresh.Status.InstanceStatuses))
			for i := range fresh.Status.InstanceStatuses {
				row := fresh.Status.InstanceStatuses[i]
				persisted[row.Index] = row
			}
			write.recordCommittedCallbacks(persisted)
			// A removal replaces the caller's whole row list, so every
			// stored row is copied; otherwise the changed rows alone.
			if write.removalApplied {
				write.replaceCommittedInstances = true
				write.committedInstances = make([]v1beta1.OMENativeInstanceStatus, len(fresh.Status.InstanceStatuses))
				for i := range fresh.Status.InstanceStatuses {
					write.committedInstances[i] = *fresh.Status.InstanceStatuses[i].DeepCopy()
				}
			} else {
				write.committedInstances = write.touchedRows(persisted)
			}
			write.recordCommittedRetryBlocks(fresh.Status.RetryBlocks)
			return nil
		})
		return write.finish(err, ir)
	}
}

// validateInstanceMutations rejects a batch whose shape the write cannot
// apply: a mutation either removes its row or changes it, and a commit
// callback needs its Instance once in the batch, so the callback has one
// previous and one current value.
func validateInstanceMutations(muts []workloadtypes.InstanceMutation) error {
	mutationCounts := make(map[int32]int, len(muts))
	for i := range muts {
		mutationCounts[muts[i].Index]++
	}
	for i := range muts {
		switch {
		case muts[i].Remove && muts[i].Mutate != nil:
			return fmt.Errorf("instance mutation %d for index %d sets both Remove and Mutate", i, muts[i].Index)
		case !muts[i].Remove && muts[i].Mutate == nil:
			return fmt.Errorf("instance mutation %d for index %d sets neither Remove nor Mutate", i, muts[i].Index)
		case muts[i].OnCommit != nil && mutationCounts[muts[i].Index] > 1:
			return fmt.Errorf("instance mutation %d for index %d uses OnCommit but the index appears %d times", i, muts[i].Index, mutationCounts[muts[i].Index])
		}
	}
	return nil
}

// instanceRows is one attempt's working copy of the stored rows: by
// Instance for the mutations, with the stored order kept for the write.
type instanceRows struct {
	byInstance map[int32]v1beta1.OMENativeInstanceStatus
	order      []int32
	ordered    map[int32]struct{}
}

// indexInstanceRows copies the stored rows into a working set with room for
// pending new Instances. An Instance the stored list names twice keeps its
// first position and its last row.
func indexInstanceRows(stored []v1beta1.OMENativeInstanceStatus, pending int) *instanceRows {
	rows := &instanceRows{
		byInstance: make(map[int32]v1beta1.OMENativeInstanceStatus, len(stored)+pending),
		order:      make([]int32, 0, len(stored)+pending),
		ordered:    make(map[int32]struct{}, len(stored)+pending),
	}
	for i := range stored {
		rows.set(stored[i].Index, stored[i])
	}
	return rows
}

// set stores a row; an Instance the set has not seen takes the next
// position after the rows already ordered.
func (r *instanceRows) set(index int32, row v1beta1.OMENativeInstanceStatus) {
	r.byInstance[index] = row
	if _, seen := r.ordered[index]; !seen {
		r.order = append(r.order, index)
		r.ordered[index] = struct{}{}
	}
}

// inStoredOrder lists the rows for the write: the stored order first, new
// Instances after it, removed Instances left out.
func (r *instanceRows) inStoredOrder() []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(r.byInstance))
	for _, index := range r.order {
		if row, found := r.byInstance[index]; found {
			rows = append(rows, row)
		}
	}
	return rows
}

// snapshot copies every row into the engine's shape for a batch
// precondition, beside the owner's identity and generation.
func (r *instanceRows) snapshot(owner *v1beta1.InferenceReplica) workloadtypes.InstanceMutationSnapshot {
	snapshot := workloadtypes.InstanceMutationSnapshot{
		OwnerUID:        owner.UID,
		OwnerGeneration: owner.Generation,
		Instances:       make(map[int32]workloadtypes.InstanceStatus, len(r.byInstance)),
	}
	for index, row := range r.byInstance {
		snapshot.Instances[index] = v1beta1convert.InstanceStatusToWorkload(*row.DeepCopy())
	}
	return snapshot
}

// instanceBatchWrite is one call of the batch status write: the row
// mutations and the retry record mutation it was asked for, what the
// attempt in progress changed on the fresh copy, and what the apiserver
// stored, which the caller's copy and the commit callbacks read once the
// write is in.
type instanceBatchWrite struct {
	mutations        []workloadtypes.InstanceMutation
	targetRevision   string
	mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition
	historyLimit     *int32

	// The attempt in progress; beginAttempt clears these for a retry.
	instanceChanged         bool
	removalApplied          bool
	touched                 []int32
	touchedSet              map[int32]struct{}
	retryBlockChanged       bool
	retryBlockPostcondition func([]v1beta1.RetryBlock) bool

	// Whether any attempt reached the apiserver. Kept across retries, so a
	// failure after an attempted write counts as a failed write.
	statusWriteAttempted bool

	// What the apiserver stored, read back after the write.
	committedInstances          []v1beta1.OMENativeInstanceStatus
	replaceCommittedInstances   bool
	committedRetryBlocks        []v1beta1.RetryBlock
	committedRetryBlockMutation bool
	committedMutationCallbacks  []committedInstanceMutation
}

// beginAttempt clears what an earlier attempt recorded, so a retry after a
// conflict rebuilds everything from the fresh copy it reads.
func (w *instanceBatchWrite) beginAttempt() {
	w.instanceChanged = false
	w.removalApplied = false
	w.touched = make([]int32, 0, len(w.mutations))
	w.touchedSet = make(map[int32]struct{}, len(w.mutations))
	w.retryBlockChanged = false
	w.retryBlockPostcondition = nil
	w.committedInstances = nil
	w.replaceCommittedInstances = false
	w.committedRetryBlocks = nil
	w.committedRetryBlockMutation = false
	w.committedMutationCallbacks = nil
}

// checkBatchPreconditions runs every batch precondition against the fresh
// rows before any mutation applies, so a rejected batch changes nothing.
// Each precondition reads its own copy of the rows.
func (w *instanceBatchWrite) checkBatchPreconditions(owner *v1beta1.InferenceReplica, rows *instanceRows) error {
	for _, mutation := range w.mutations {
		if mutation.BatchPrecondition == nil {
			continue
		}
		if !mutation.BatchPrecondition(rows.snapshot(owner)) {
			return workloadtypes.ErrStatusMutationPrecondition
		}
	}
	return nil
}

// applyInstanceMutations applies the mutations to the working rows in the
// order given. A mutation whose precondition fails, whose callback reports
// no change, or whose removal names no row leaves the rows as they are.
func (w *instanceBatchWrite) applyInstanceMutations(rows *instanceRows) {
	for i := range w.mutations {
		if w.mutations[i].Remove {
			w.removeInstance(w.mutations[i], rows)
			continue
		}
		w.mutateInstance(w.mutations[i], rows)
	}
}

// removeInstance drops one row when it exists and its precondition holds,
// and keeps the row's last value for the commit callback.
func (w *instanceBatchWrite) removeInstance(m workloadtypes.InstanceMutation, rows *instanceRows) {
	row, found := rows.byInstance[m.Index]
	if !found {
		return
	}
	previous := v1beta1convert.InstanceStatusToWorkload(*row.DeepCopy())
	if m.Precondition != nil && !m.Precondition(&previous) {
		return
	}
	delete(rows.byInstance, m.Index)
	w.instanceChanged = true
	w.removalApplied = true
	if m.OnCommit != nil {
		w.committedMutationCallbacks = append(w.committedMutationCallbacks, committedInstanceMutation{
			index:    m.Index,
			onCommit: m.OnCommit,
			previous: cloneWorkloadInstanceStatus(previous),
		})
	}
}

// mutateInstance hands one row, or an empty row for an Instance that has
// none, to the mutation's callback and stores the result when the callback
// reports a change. A new row joins the stored order at its end.
func (w *instanceBatchWrite) mutateInstance(m workloadtypes.InstanceMutation, rows *instanceRows) {
	row, found := rows.byInstance[m.Index]
	if !found {
		row = v1beta1.OMENativeInstanceStatus{Index: m.Index}
	}
	status := v1beta1convert.InstanceStatusToWorkload(row)
	if m.Precondition != nil && !m.Precondition(&status) {
		return
	}
	var previous *workloadtypes.InstanceStatus
	if found {
		previous = cloneWorkloadInstanceStatus(status)
	}
	read := status.Phase
	if !m.Mutate(&status) {
		return
	}
	rows.set(m.Index, encodeMutatedInstance(row.Phase, read, status))
	w.instanceChanged = true
	if _, seen := w.touchedSet[m.Index]; !seen {
		w.touched = append(w.touched, m.Index)
		w.touchedSet[m.Index] = struct{}{}
	}
	if m.OnCommit != nil {
		w.committedMutationCallbacks = append(w.committedMutationCallbacks, committedInstanceMutation{
			index:    m.Index,
			onCommit: m.OnCommit,
			previous: previous,
			current:  cloneWorkloadInstanceStatus(status),
		})
	}
}

// applyRetryBlockMutation hands the retry record of the target revision, or
// an empty one, to the caller's callback and keeps, removes or leaves the
// record as the callback decides. A changed list is pruned to the history
// limit, and the record's written shape is kept to confirm an ambiguous
// write.
func (w *instanceBatchWrite) applyRetryBlockMutation(status *v1beta1.InferenceReplicaStatus) {
	if w.mutateRetryBlock == nil {
		return
	}
	blocks := status.RetryBlocks
	pos := retryBlockPosition(blocks, w.targetRevision)
	block := workloadtypes.RetryBlock{TargetRevision: w.targetRevision}
	if pos != -1 {
		block = retryBlockToWorkload(blocks[pos])
	}
	switch w.mutateRetryBlock(&block) {
	case workloadtypes.RetryBlockPersist:
		next := retryBlockFromWorkload(block)
		next.TargetRevision = w.targetRevision
		if pos == -1 {
			blocks = append(blocks, next)
		} else {
			blocks[pos] = next
		}
		w.retryBlockChanged = true
	case workloadtypes.RetryBlockRemove:
		if pos != -1 {
			blocks = append(blocks[:pos], blocks[pos+1:]...)
			w.retryBlockChanged = true
		}
	}
	if !w.retryBlockChanged {
		return
	}
	status.RetryBlocks = pruneRetryBlocks(blocks, status.UpdateRevision, w.historyLimit)
	w.retryBlockPostcondition = retryBlockPostconditionFor(status.RetryBlocks, w.targetRevision)
}

// retryBlockPosition is the position of the revision's retry record in
// blocks, or -1 when the revision has none.
func retryBlockPosition(blocks []v1beta1.RetryBlock, revision string) int {
	for i := range blocks {
		if blocks[i].TargetRevision == revision {
			return i
		}
	}
	return -1
}

// retryBlockPostconditionFor recognizes the written retry record of one
// revision on a later read: present with the same content, or absent, as
// the write left it.
func retryBlockPostconditionFor(blocks []v1beta1.RetryBlock, revision string) func([]v1beta1.RetryBlock) bool {
	expected, expectedFound := retryBlockForRevision(blocks, revision)
	return func(blocks []v1beta1.RetryBlock) bool {
		actual, found := retryBlockForRevision(blocks, revision)
		return found == expectedFound && (!found || apiequality.Semantic.DeepEqual(actual, expected))
	}
}

// persist writes the fresh copy and returns the object as the apiserver
// stored it. An error that may still have committed the write is checked
// against a live read: the write counts when the live object confirms it,
// and the error stands otherwise.
func (w *instanceBatchWrite) persist(ctx context.Context, writer statusWriter, reads client.Reader, key client.ObjectKey, fresh *v1beta1.InferenceReplica, source irstatus.Encoding) (*v1beta1.InferenceReplica, error) {
	err := updateInferenceReplicaStatus(ctx, writer, fresh, source)
	if err == nil {
		return fresh, nil
	}
	if apierrors.IsNotFound(err) {
		return nil, workloadtypes.ErrStatusOwnerGone
	}
	updateErr := fmt.Errorf("update IR status: %w", err)
	confirmed := &v1beta1.InferenceReplica{}
	confirmedSource, readErr := irstatus.GetDecoded(ctx, reads, key, confirmed)
	if readErr != nil || confirmed.UID != fresh.UID || !w.confirms(confirmed) {
		return nil, updateErr
	}
	obsmetrics.RecordIRStatusWrite(encodingLabel(confirmedSource), obsmetrics.IRStatusWriteConfirmed)
	return confirmed, nil
}

// confirms reports whether a live object proves the write landed: every
// row mutation's postcondition holds there and the retry record reads as
// written. A batch with nothing to confirm never counts as landed.
func (w *instanceBatchWrite) confirms(confirmed *v1beta1.InferenceReplica) bool {
	if len(w.mutations) > 0 && !instanceMutationPostconditionsHold(confirmed, w.mutations) {
		return false
	}
	if w.retryBlockPostcondition != nil && !w.retryBlockPostcondition(confirmed.Status.RetryBlocks) {
		return false
	}
	return len(w.mutations) > 0 || w.retryBlockPostcondition != nil
}

// recordCommittedCallbacks gives each commit callback the row as the
// apiserver stored it, or nil for an Instance the write left without a
// row. A removal's callback keeps its nil current value.
func (w *instanceBatchWrite) recordCommittedCallbacks(persisted map[int32]v1beta1.OMENativeInstanceStatus) {
	for i := range w.committedMutationCallbacks {
		if w.committedMutationCallbacks[i].current == nil {
			continue
		}
		w.committedMutationCallbacks[i].current = nil
		if row, found := persisted[w.committedMutationCallbacks[i].index]; found {
			current := v1beta1convert.InstanceStatusToWorkload(*row.DeepCopy())
			w.committedMutationCallbacks[i].current = cloneWorkloadInstanceStatus(current)
		}
	}
}

// touchedRows copies the stored rows of the Instances this write changed,
// in the order the mutations named them.
func (w *instanceBatchWrite) touchedRows(persisted map[int32]v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(w.touched))
	for _, index := range w.touched {
		if row, found := persisted[index]; found {
			rows = append(rows, *row.DeepCopy())
		}
	}
	return rows
}

// recordCommittedRetryBlocks keeps a copy of the stored retry records when
// this write changed them.
func (w *instanceBatchWrite) recordCommittedRetryBlocks(blocks []v1beta1.RetryBlock) {
	if !w.retryBlockChanged {
		return
	}
	w.committedRetryBlockMutation = true
	w.committedRetryBlocks = make([]v1beta1.RetryBlock, len(blocks))
	for i := range blocks {
		w.committedRetryBlocks[i] = *blocks[i].DeepCopy()
	}
}

// finish records the write's outcome and, after a committed write, mirrors
// the stored rows and retry records onto the caller's copy and runs the
// commit callbacks in mutation order, so later work in the same pass reads
// the state the apiserver holds.
func (w *instanceBatchWrite) finish(err error, ir *v1beta1.InferenceReplica) error {
	if err != nil {
		w.recordFailure(err)
		return err
	}
	if w.statusWriteAttempted {
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultSuccess)
	}
	if w.replaceCommittedInstances {
		replaceInstanceStatuses(ir, w.committedInstances)
	} else {
		mirrorInstanceStatuses(ir, w.committedInstances)
	}
	if w.committedRetryBlockMutation && ir != nil {
		ir.Status.RetryBlocks = w.committedRetryBlocks
	}
	for _, committed := range w.committedMutationCallbacks {
		committed.onCommit(committed.previous, committed.current)
	}
	return nil
}

// recordFailure counts a failed write by its kind. A rejected batch
// precondition is a replan, not a write outcome, and an error before any
// attempt reached the apiserver counts nothing.
func (w *instanceBatchWrite) recordFailure(err error) {
	switch {
	case errors.Is(err, workloadtypes.ErrStatusMutationPrecondition):
		// A replan: no write outcome to count.
	case errors.Is(err, workloadtypes.ErrStatusOwnerGone):
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultNotFound)
	case apierrors.IsConflict(err):
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultConflict)
	case w.statusWriteAttempted:
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultError)
	}
}

func cloneWorkloadInstanceStatus(status workloadtypes.InstanceStatus) *workloadtypes.InstanceStatus {
	converted := v1beta1convert.InstanceStatusFromWorkload(status)
	copy := v1beta1convert.InstanceStatusToWorkload(*converted.DeepCopy())
	return &copy
}

// encodeMutatedInstance converts a mutated row back for the write. A
// mutation that leaves the phase as it read it keeps the published phase:
// the status publication writes Restarting over a settled Ready row whose
// pod set serves nothing, and Ready over a Failed row whose pods on its
// running revision serve beside an attempt at another, spent or still kept
// at its surge step; the engine reads those rows as Ready and Failed, and
// only a mutation that moves the phase overwrites the publication's
// reading.
func encodeMutatedInstance(published v1beta1.OMENativeInstancePhase, read workloadtypes.InstancePhase, w workloadtypes.InstanceStatus) v1beta1.OMENativeInstanceStatus {
	out := v1beta1convert.InstanceStatusFromWorkload(w)
	if w.Phase == read {
		out.Phase = published
	}
	return out
}

func instanceMutationPostconditionsHold(ir *v1beta1.InferenceReplica, mutations []workloadtypes.InstanceMutation) bool {
	if ir == nil || len(mutations) == 0 {
		return false
	}
	statuses := make(map[int32]*workloadtypes.InstanceStatus, len(ir.Status.InstanceStatuses))
	for i := range ir.Status.InstanceStatuses {
		status := v1beta1convert.InstanceStatusToWorkload(*ir.Status.InstanceStatuses[i].DeepCopy())
		statuses[status.Index] = &status
	}
	for _, mutation := range mutations {
		status, found := statuses[mutation.Index]
		if mutation.Remove {
			if found {
				return false
			}
			continue
		}
		if !found || mutation.Postcondition == nil || !mutation.Postcondition(status) {
			return false
		}
	}
	return true
}

func retryBlockForRevision(blocks []v1beta1.RetryBlock, revision string) (v1beta1.RetryBlock, bool) {
	for i := range blocks {
		if blocks[i].TargetRevision == revision {
			return *blocks[i].DeepCopy(), true
		}
	}
	return v1beta1.RetryBlock{}, false
}

func replaceInstanceStatuses(ir *v1beta1.InferenceReplica, statuses []v1beta1.OMENativeInstanceStatus) {
	if ir == nil {
		return
	}
	ir.Status.InstanceStatuses = statuses
}

// mirrorInstanceStatus copies the just-committed InstanceStatus onto
// the caller's in-memory IR snapshot. Subsequent ops in the same
// reconcile pass observe the post-write state instead of the stale
// pre-reconcile snapshot.
func mirrorInstanceStatus(ir *v1beta1.InferenceReplica, src v1beta1.OMENativeInstanceStatus) {
	mirrorInstanceStatuses(ir, []v1beta1.OMENativeInstanceStatus{src})
}

func mirrorInstanceStatuses(ir *v1beta1.InferenceReplica, statuses []v1beta1.OMENativeInstanceStatus) {
	if ir == nil || len(statuses) == 0 {
		return
	}
	positions := make(map[int32]int, len(ir.Status.InstanceStatuses)+len(statuses))
	for i := range ir.Status.InstanceStatuses {
		positions[ir.Status.InstanceStatuses[i].Index] = i
	}
	for _, src := range statuses {
		if pos, found := positions[src.Index]; found {
			ir.Status.InstanceStatuses[pos] = src
			continue
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, src)
		positions[src.Index] = len(ir.Status.InstanceStatuses) - 1
	}
}

// buildPromoteCurrentRevision returns the CurrentRevision rollup step.
// The IR controller owns the component-level revision pair: CurrentRevision
// and UpdateRevision are both stamped against the SPEC target (never the
// roll target — during a canary rollback the two diverge, and coordination
// reads their skew as RolloutInFlight, so promoting against the roll target
// would fabricate a permanent phantom-rollout skew; the canary rollback
// machinery also load-bears on "CurrentRevision names the last revision
// fully rolled forward onto"). The reconciler calls this from its deferred
// status tail on every return path, so the rollup's timing is uniform
// across success, early-requeue, and paused reconciles.
//
//   - authoritative Get the IR so the rollup observes per-Instance
//     updates committed earlier in the same reconcile;
//   - CurrentRevision follows status.CurrentRevisionFor: promoted to
//     targetName once every Instance is Ready on it and no RetryBlock
//     names it (a partition > 0 never satisfies that, so a staged rollout
//     does not promote — the Staged Ready reason derives downstream from
//     the CurrentRevision != UpdateRevision skew; a revision whose ladder
//     still records a failure has not landed, whatever the rows read
//     between crashes), withdrawn while it names targetName but an
//     Instance still runs another revision, the shape a rollback onto the
//     last promoted revision leaves behind, and restored to targetName
//     once every Instance is back on the revision it was withdrawn from,
//     which the CurrentRevisionWithdrawn condition records beside the
//     empty current and this write raises or clears with it;
//   - the rows are read beside the revision their pods carry (podReader,
//     the cached client); the write that moves current records targetName
//     as the update revision too, so the withdrawal names the pair written;
//   - an unchanged value short-circuits with no write (a converged steady
//     state performs ZERO writes);
//   - retry.RetryOnConflict mirrors buildMutateInstance.
//
// On a committed write the new revision pair is mirrored onto the caller's
// in-memory IR so the deferred aggregator's Ready computation and the
// reconciler's promotion log observe the post-write value.
func buildPromoteCurrentRevision(writer statusWriter, reads, podReader client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, targetName string) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	ownerGeneration := ir.Generation
	templateHash := templateHashOf(ir)
	return func(ctx context.Context, targetName string) error {
		if targetName == "" {
			return nil
		}
		pods, err := podRevisionsFor(ctx, podReader, ir)
		if err != nil {
			return err
		}
		var committed string
		wrote := false
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			committed, wrote = "", false
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workloadtypes.ErrStatusOwnerGone
			}
			if fresh.Generation != ownerGeneration && specChangedSincePlanned(ir, fresh) {
				return workloadtypes.ErrStatusMutationPrecondition
			}
			insts := v1beta1convert.InstanceStatusSliceToWorkload(fresh.Status.InstanceStatuses)
			withdrawn := withdrawnRevisionFor(&fresh.Status)
			recorded := fresh.Status.CurrentRevision
			want := workloadstatus.CurrentRevisionFor(insts, recorded, targetName, retryBlocksFromIR(fresh), withdrawn, pods)
			if want == recorded {
				return nil
			}
			fresh.Status.UpdateRevision = targetName
			fresh.Status.ObservedTemplateHash = templateHash
			fresh.Status.CurrentRevision = want
			recordCurrentRevisionWithdrawal(&fresh.Status, withdrawn, recorded)
			if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			committed, wrote = want, true
			return nil
		})
		if err != nil {
			return err
		}
		if wrote && ir != nil {
			ir.Status.UpdateRevision = targetName
			ir.Status.ObservedTemplateHash = templateHash
			ir.Status.CurrentRevision = committed
		}
		return nil
	}
}

// buildRecordUpdateRevision writes status.updateRevision ahead of the pass
// that first renders a revision: re-read under retry.RetryOnConflict,
// refuse a generation the pass did not plan against, write, and mirror the
// committed names onto the caller's in-memory IR. The publication at the end
// of the pass restates the same name beside the counters. The template hash
// the revision was computed from is written beside it, so a reader can tell
// the status describes the spec's current templates.
//
// The pair is written as one pair: the same write sets status.currentRevision
// from status.CurrentRevisionFor over the fresh rows and the revision their
// pods carry (podReader, the cached client), so no stored status reads
// current equal to update while an Instance runs, or is pinned to, another revision.
func buildRecordUpdateRevision(writer statusWriter, reads, podReader client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, targetName string) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	ownerGeneration := ir.Generation
	templateHash := templateHashOf(ir)
	return func(ctx context.Context, targetName string) error {
		if targetName == "" {
			return nil
		}
		pods, err := podRevisionsFor(ctx, podReader, ir)
		if err != nil {
			return err
		}
		var committedCurrent string
		wrote := false
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			committedCurrent, wrote = "", false
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workloadtypes.ErrStatusOwnerGone
			}
			if fresh.Generation != ownerGeneration && specChangedSincePlanned(ir, fresh) {
				return workloadtypes.ErrStatusMutationPrecondition
			}
			if fresh.Status.UpdateRevision == targetName && fresh.Status.ObservedTemplateHash == templateHash {
				return nil
			}
			insts := v1beta1convert.InstanceStatusSliceToWorkload(fresh.Status.InstanceStatuses)
			withdrawn := withdrawnRevisionFor(&fresh.Status)
			recorded := fresh.Status.CurrentRevision
			current := workloadstatus.CurrentRevisionFor(insts, recorded, targetName, retryBlocksFromIR(fresh), withdrawn, pods)
			fresh.Status.UpdateRevision = targetName
			fresh.Status.ObservedTemplateHash = templateHash
			fresh.Status.CurrentRevision = current
			recordCurrentRevisionWithdrawal(&fresh.Status, withdrawn, recorded)
			if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			committedCurrent, wrote = current, true
			return nil
		})
		if err != nil {
			return err
		}
		if ir != nil {
			ir.Status.UpdateRevision = targetName
			ir.Status.ObservedTemplateHash = templateHash
			if wrote {
				ir.Status.CurrentRevision = committedCurrent
			}
		}
		return nil
	}
}

// templateHashOf is the pod-template hash the projector wrote on a
// replica's spec, "" on a replica no projector writes.
func templateHashOf(ir *v1beta1.InferenceReplica) string {
	return ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]
}

// podRevisionsFor lists the Component's pods through the cache, as the
// publication does, and reads the revision each Instance's pods carry.
func podRevisionsFor(ctx context.Context, c client.Reader, ir *v1beta1.InferenceReplica) (workloadstatus.PodRevisions, error) {
	pods, err := query.ListOMENativePodsByName(ctx, c, ir.Namespace, ir.NamePrefix(), v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), true)
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	return workloadstatus.PodRevisionsByInstance(query.BucketPodsByInstanceIdx(pods)), nil
}

// buildMutateRetryBlock wraps the workload-typed RetryBlock mutate
// callback with an IR-typed apiserver round-trip — the
// ReconcileInput.MutateRetryBlock capability. Same shape as
// buildMutateInstance: re-read under retry.RetryOnConflict, locate the
// block for targetRevision (or hand the callback a zero block with
// TargetRevision set), convert to the workload mirror, apply the
// callback's disposition (Persist upserts, Remove deletes, Unchanged
// writes nothing), apply the retention prune, persist via
// Status().Update, and mirror the committed slice back onto the
// caller's in-memory IR so later ops in the same pass observe the
// post-write state.
//
// Owner disappearance or replacement returns ErrStatusOwnerGone.
func buildMutateRetryBlock(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica, historyLimit *int32) func(ctx context.Context, targetRevision string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	return func(ctx context.Context, targetRevision string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		var committed []v1beta1.RetryBlock
		wrote := false
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			wrote = false
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workloadtypes.ErrStatusOwnerGone
			}
			blocks := fresh.Status.RetryBlocks
			pos := -1
			for i := range blocks {
				if blocks[i].TargetRevision == targetRevision {
					pos = i
					break
				}
			}
			w := workloadtypes.RetryBlock{TargetRevision: targetRevision}
			if pos != -1 {
				w = retryBlockToWorkload(blocks[pos])
			}
			switch mutate(&w) {
			case workloadtypes.RetryBlockPersist:
				nb := retryBlockFromWorkload(w)
				// The subject key is fixed: a callback cannot re-scope the
				// block to a different revision.
				nb.TargetRevision = targetRevision
				if pos == -1 {
					blocks = append(blocks, nb)
				} else {
					blocks[pos] = nb
				}
			case workloadtypes.RetryBlockRemove:
				if pos == -1 {
					// Nothing to remove — no write.
					return nil
				}
				blocks = append(blocks[:pos], blocks[pos+1:]...)
			default: // RetryBlockUnchanged
				return nil
			}
			fresh.Status.RetryBlocks = pruneRetryBlocks(blocks, fresh.Status.UpdateRevision, historyLimit)
			if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			wrote = true
			committed = make([]v1beta1.RetryBlock, len(fresh.Status.RetryBlocks))
			for i := range fresh.Status.RetryBlocks {
				committed[i] = *fresh.Status.RetryBlocks[i].DeepCopy()
			}
			return nil
		})
		if err != nil {
			return err
		}
		if wrote {
			// Mirror the committed slice onto the caller's in-memory IR —
			// same pattern as buildMutateInstance's mirrorInstanceStatus,
			// wholesale because pruning can touch entries beyond the
			// mutated one.
			ir.Status.RetryBlocks = committed
		}
		return nil
	}
}

// pruneRetryBlocks applies the RetryBlock retention rule: the block
// whose TargetRevision == updateRevision (the current rollout target)
// is NEVER pruned; the remaining historical blocks are capped at
// historyLimit, oldest-pruned by LastFailureAt with a nil LastFailureAt
// sorting oldest. Survivors keep their original relative order so the
// listType=map patch surface stays minimal. A nil limit is
// unconfigured: every historical block is kept.
func pruneRetryBlocks(blocks []v1beta1.RetryBlock, updateRevision string, historyLimit *int32) []v1beta1.RetryBlock {
	if historyLimit == nil {
		return blocks
	}
	var otherIdx []int
	for i := range blocks {
		if updateRevision == "" || blocks[i].TargetRevision != updateRevision {
			otherIdx = append(otherIdx, i)
		}
	}
	excess := len(otherIdx) - int(*historyLimit)
	if excess <= 0 {
		return blocks
	}
	sort.SliceStable(otherIdx, func(a, b int) bool {
		return retryBlockOlder(blocks[otherIdx[a]], blocks[otherIdx[b]])
	})
	drop := make(map[int]bool, excess)
	for _, i := range otherIdx[:excess] {
		drop[i] = true
	}
	out := make([]v1beta1.RetryBlock, 0, len(blocks)-excess)
	for i := range blocks {
		if !drop[i] {
			out = append(out, blocks[i])
		}
	}
	return out
}

// retryBlockOlder orders blocks for pruning: nil LastFailureAt is
// oldest; otherwise earlier LastFailureAt is older.
func retryBlockOlder(a, b v1beta1.RetryBlock) bool {
	switch {
	case a.LastFailureAt == nil && b.LastFailureAt == nil:
		return false // tie — stable sort keeps original order
	case a.LastFailureAt == nil:
		return true
	case b.LastFailureAt == nil:
		return false
	default:
		return a.LastFailureAt.Before(b.LastFailureAt)
	}
}

// buildRemoveInstance wraps the IR-typed status removal flow as the
// workload.ReconcileInput.RemoveInstance callback. Drops the (idx)
// InstanceStatus, persists, and (on a real removal) forgets the
// expectation entry so a later index reuse doesn't inherit stale
// counters. The expectations bucket keys on Key.OwnerName, which is
// ir.NamePrefix() (buildKey), so Forget must use the same.
// Owner disappearance or replacement returns ErrStatusOwnerGone.
func buildRemoveInstance(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica, exp *workloadtypes.Expectations) func(ctx context.Context, idx int32) (bool, error) {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	return func(ctx context.Context, idx int32) (bool, error) {
		var hadEntry bool
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			hadEntry = false
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workloadtypes.ErrStatusOwnerGone
			}
			insts := fresh.Status.InstanceStatuses
			pos := -1
			for i, s := range insts {
				if s.Index == idx {
					pos = i
					break
				}
			}
			if pos == -1 {
				return nil
			}
			fresh.Status.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
			if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			hadEntry = true
			return nil
		})
		if err != nil {
			return false, err
		}
		if hadEntry {
			cache := exp
			if cache == nil {
				cache = workloadtypes.DefaultExpectations
			}
			cache.Forget(ir.Namespace, ir.NamePrefix(), v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), idx)
		}
		return hadEntry, nil
	}
}

// buildWriteAggregateCondition wraps the IR-side per-Component
// condition writer as the workload.ReconcileInput.WriteAggregateCondition
// callback. The closure merges cond into IR.Status.Conditions under
// retry.RetryOnConflict so LastTransitionTime only bumps on a real
// transition and concurrent reconciles can't lose the write to each
// other. Today this surface is exercised only by the workload-side
// gang reconciler (GangSchedulingUnavailable); the IR controller
// inherits it for free.
func buildWriteAggregateCondition(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, cond metav1.Condition) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	return func(ctx context.Context, cond metav1.Condition) error {
		if writer.Client == nil || ir == nil {
			return fmt.Errorf("buildWriteAggregateCondition: nil client or IR")
		}
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workloadtypes.ErrStatusOwnerGone
			}
			// No-op short-circuit so we don't bump ResourceVersion every
			// reconcile when nothing changed.
			if existing := apimeta.FindStatusCondition(fresh.Status.Conditions, cond.Type); existing != nil &&
				existing.Status == cond.Status &&
				existing.Reason == cond.Reason &&
				existing.ObservedGeneration == cond.ObservedGeneration &&
				existing.Message == cond.Message {
				return nil
			}
			apimeta.SetStatusCondition(&fresh.Status.Conditions, cond)
			if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
				if apierrors.IsNotFound(err) {
					return workloadtypes.ErrStatusOwnerGone
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			return nil
		})
	}
}
