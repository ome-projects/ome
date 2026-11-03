package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	isvcstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// InferenceReplicaConditionReady is the top-level condition type written
// onto IR.Status.Conditions. Operators waiting on the IR directly
// (`kubectl wait --for=condition=Ready irep/<name>`), the irprojector
// that mirrors IR conditions onto the parent ISVC, and future autoscaler
// scalers that read the IR all key off this condition.
const InferenceReplicaConditionReady = "Ready"

// InferenceReplicaConditionRolloutStalled is an ADVISORY condition that
// surfaces a rollout wedged on failing Instances (most commonly a new-revision
// pod stuck in CrashLoopBackOff). It is deliberately NOT a dependent of the
// Ready condition: old-revision pods keep serving during a failed surge
// update, so a healthy IR/ISVC stays Ready=True while this flags the otherwise-
// invisible wedge (rollout stuck at N/total) for operators + dashboards.
const InferenceReplicaConditionRolloutStalled = "RolloutStalled"

// InferenceReplicaConditionDrainOverdue is an ADVISORY condition that is
// True while at least one Instance on its way out is past the deadline
// of the delete operation draining it. Nothing about the scale-down
// changes while it is True — the drain keeps running and the wedged pod
// is removed only under a configured force-delete policy — so like
// RolloutStalled it is not a dependent of Ready: it exists to make a
// stalled drain visible to operators and dashboards instead of leaving
// the elapsed deadline unreported. It clears when no Instance is
// overdue.
const InferenceReplicaConditionDrainOverdue = "DrainOverdue"

// InferenceReplicaConditionCurrentRevisionWithdrawn is present, True, while
// status.currentRevision stands withdrawn; the rollup reads it to restore
// the current revision once every Instance is back on that revision.
const InferenceReplicaConditionCurrentRevisionWithdrawn = "CurrentRevisionWithdrawn"

// ReasonInstanceOffRevision is the CurrentRevisionWithdrawn reason: an
// Instance still runs, or is pinned to, another revision.
const ReasonInstanceOffRevision = "InstanceOffRevision"

const (
	// ReasonInstancesFailing — RolloutStalled=True: >=1 Instance recorded a
	// terminal failure while not yet on the target revision.
	ReasonInstancesFailing = "InstancesFailing"
	// ReasonRolloutProgressing — RolloutStalled=False: rollout in flight with
	// no failing Instances, or no rollout in flight.
	ReasonRolloutProgressing = "Progressing"
)

const (
	// ReasonDrainsOverdue — DrainOverdue=True: >=1 Instance is draining
	// past the deadline of the delete operation that owns it.
	ReasonDrainsOverdue = "DrainsOverdue"
	// ReasonDrainsWithinDeadline — DrainOverdue=False: every drain in
	// flight is inside its deadline, or none is in flight.
	ReasonDrainsWithinDeadline = "DrainsWithinDeadline"
)

// Reason strings for the InferenceReplicaConditionReady condition. Kept
// short and machine-readable so
// `kubectl get irep -o jsonpath='...conditions[?(@.type=="Ready")].reason'`
// is grep-friendly. Mirrors the omenative status-conditions vocabulary
// for cross-controller consistency.
const (
	// ReasonAllInstancesReady is stamped when every Instance is Ready AND
	// CurrentRevision == UpdateRevision (rollout fully converged).
	ReasonAllInstancesReady = "AllInstancesReady"
	// ReasonRolloutInProgress is stamped when CurrentRevision !=
	// UpdateRevision — explicit "churning, check back" signal. Maps to
	// Status=Unknown so `kubectl wait --for=condition=Ready` doesn't
	// short-circuit while a rollout is mid-flight.
	ReasonRolloutInProgress = "RolloutInProgress"
	// ReasonStaged is stamped when the IR rests at its staged shape under a
	// non-zero partition (a projected canary step or a user rollingUpdate
	// partition): every Instance Ready, the ones still on the prior revision
	// held by the partition, at most `partition` of them. A partition that
	// asks for more held Instances than still run the prior revision holds
	// every one of them, and the message says how many fewer. Ready=True:
	// a partitioned rollout is intentionally complete-and-holding, not
	// "in progress". Distinct from AllInstancesReady (fully rolled).
	ReasonStaged = "Staged"
	// ReasonInstanceFailed is stamped when at least one Instance has
	// terminally failed (stuck-pod escalation, InstanceReadyTimeout).
	// Maps to Status=False so operators paging on Ready=False catch the
	// failure immediately.
	ReasonInstanceFailed = "InstanceFailed"
	// ReasonReplicaCountMismatch is stamped when the serving Instance count is
	// below the lifecycle rolling-update availability floor with no in-flight
	// rollout. Maps to Status=False.
	ReasonReplicaCountMismatch = "ReplicaCountMismatch"
	// ReasonMinimumAvailable is stamped when ReadyReplicas < Replicas but the
	// serving Instance count is still at or above the availability floor —
	// Ready=True, distinct from AllInstancesReady so full readiness remains
	// distinguishable from partial availability.
	ReasonMinimumAvailable = "MinimumAvailable"
	// ReasonNoReplicas is stamped when Replicas == 0 (no desired
	// Instances). Distinct from ReplicaCountMismatch so operators can
	// tell "scaled-to-zero" from "something stuck".
	ReasonNoReplicas = "NoReplicas"
)

// Ready=False reasons stamped when a replica has no pod templates to run:
// it declares no template source, or it renders from modelRef/runtimeRef
// and the render is blocked. The workload pass does not run and the existing
// pods are left as they are; the runtime and model watches re-trigger the
// replica when the referenced objects change.
const (
	// ReasonTemplateSourceMissing: neither runners nor a model or runtime
	// reference is set.
	ReasonTemplateSourceMissing = "TemplateSourceMissing"
	// ReasonModelNotFound: no BaseModel or ClusterBaseModel carries the
	// referenced name, or a sharded model has not finished loading.
	ReasonModelNotFound = "ModelNotFound"
	// ReasonModelDisabled: the referenced model is disabled.
	ReasonModelDisabled = "ModelDisabled"
	// ReasonRuntimeNotFound: the named runtime does not exist in the
	// replica's namespace or at cluster scope.
	ReasonRuntimeNotFound = "RuntimeNotFound"
	// ReasonRuntimeDisabled: the named runtime is disabled.
	ReasonRuntimeDisabled = "RuntimeDisabled"
	// ReasonRuntimeSelectionFailed: no runtime serves the model, or the
	// named runtime cannot serve it.
	ReasonRuntimeSelectionFailed = "RuntimeSelectionFailed"
	// ReasonRuntimePieceMissing: the runtime declares no piece for the
	// replica's component.
	ReasonRuntimePieceMissing = "RuntimePieceMissing"
	// ReasonRenderFailed: the merge or the render of the pod templates
	// failed, or this controller is not configured to render from refs.
	ReasonRenderFailed = "RenderFailed"
)

// aggregateAndWriteStatus publishes cached Pod and EndpointSlice facts over
// freshly read lifecycle rows, then computes the IR summaries and conditions.
// Lifecycle fields and CurrentRevision retain their dedicated writers, with
// one exception the publication owns: the phase of a settled row follows
// the pods published beside it, so the phase never contradicts the
// counters. A Ready row (workloadstatus.DemotableReady) reads Pending with
// no pods and Restarting while its set serves nothing; a Failed row off the
// revision it runs, its attempt spent (workloadtypes.FailedOffRunningRevision)
// or still kept at its surge step
// (workloadtypes.FailedKeepingSurgeOffRunningRevision), reads Ready while
// the pods on that revision serve. A nil target leaves revision pointers
// untouched while counters still publish.
//
// holdObserved/hold carry this pass's Update-pass rollout-hold verdict
// (ReconcileInput.RecordRolloutHold): holdObserved is true when the Update
// pass ran at all this reconcile, and hold is its verdict (nil clears).
// When the Update pass did not run (holdObserved is false — the pass ended
// before it, nothing needed updating, or Plan selected no target), the
// same-target RetryBlock/Held state persisted in status is the hold (a
// RetryBlock-denied Instance is never selected for the Update pass and the
// pass reports the block only when it runs for other Instances), else the
// persisted hold stands until a pass that runs the Update pass replaces or
// clears it, unless this pass removed an Instance. instancesAtStart lists
// the Instances that had a row when the pass began; a recorded hold
// describes those Instances, so the write that removes one of them carries
// no hold the pass did not observe — see effectiveRolloutHold.
//
// The returned duration is how long until the earliest in-rotation pod still
// inside the spec.minReadySeconds window becomes Available (0 when none is
// pending). That raises the Available counters without any watch event, so
// the caller folds it into the reconcile result to publish them on time.
func (r *Reconciler) aggregateAndWriteStatus(ctx context.Context, ir *v1beta1.InferenceReplica, plan workloadtypes.ComponentPlan, target *appsv1.ControllerRevision, holdObserved bool, hold *workloadtypes.RolloutHold, instancesAtStart []int32) (time.Duration, error) {
	if r.Client == nil {
		return 0, fmt.Errorf("aggregateAndWriteStatus: nil client")
	}
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	ownerGeneration := ir.Generation
	desiredByIdx := workloadstatus.DesiredPodCountByInstance(plan)
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)

	// List pods + EndpointSlices via the CACHED client (r.Client), outside
	// the retry closure: both reads are idempotent (no apiserver-side
	// mutation), so on a status conflict we just re-stamp the same counters
	// onto the re-read IR.
	//
	// This aggregator is observability-only COUNTING — the manager cache is
	// scoped to OME pods, has the OMENative Pod field index registered, and
	// watches EndpointSlices, so cache-served counts are both correct and
	// cheap. Going live (r.APIReader) on every reconcile would be the dominant
	// hot-path cost: an uncached LIST per IR reconcile purely for counters,
	// plus a MatchingFields probe the live reader can't serve. r.APIReader
	// stays reserved for the destructive/correctness-critical paths
	// (drain/delete/recreate confirm) that genuinely need a live read.
	pods, err := query.ListOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.NamePrefix(), component, true)
	if err != nil {
		return 0, fmt.Errorf("aggregateAndWriteStatus: list pods: %w", err)
	}
	byIndex := query.BucketPodsByInstanceIdx(pods)
	podObservation := workload.NewCachedSelectorPodObservation(pods, byIndex)
	serviceName := query.HeadlessServiceName(ir.NamePrefix(), component)
	availableByPod, err := workloadstatus.AvailablePodSet(ctx, r.Client, ir.Namespace, serviceName)
	if err != nil {
		return 0, fmt.Errorf("aggregateAndWriteStatus: compute availability: %w", err)
	}

	ownerUnavailable := false
	var nextAvailableIn time.Duration
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		source, err := irstatus.GetDecoded(ctx, r.liveReader(), key, fresh)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// IR deleted under us; nothing to aggregate into.
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultNotFound)
				ownerUnavailable = true
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: re-read IR: %w", err)
		}
		if ownerUID == "" || fresh.UID != ownerUID {
			ownerUnavailable = true
			return nil
		}
		if fresh.Generation != ownerGeneration && specChangedSincePlanned(ir, fresh) {
			return workloadtypes.ErrStatusMutationPrecondition
		}

		// Snapshot the live status before recomputation so a no-op
		// reconcile (computed == live) skips the Status().Update below.
		// Every status write re-triggers this controller AND the
		// irprojector rollup onto the parent ISVC, so at steady state the
		// unconditional write amplified idle reconciles into a write
		// storm. apimeta.SetStatusCondition preserves LastTransitionTime
		// when the condition value is unchanged, so an unchanged status
		// DeepEquals its prior snapshot.
		before := fresh.Status.DeepCopy()

		observation, err := workload.NewOwnedPublicationObservation(
			v1beta1convert.InstanceStatusSliceToWorkload(fresh.Status.InstanceStatuses),
			podObservation,
			availableByPod,
			workloadstatus.AvailabilityWindow{MinReadySeconds: plan.MinReadySeconds, Now: r.now()},
		)
		if err != nil {
			return fmt.Errorf("aggregateAndWriteStatus: build publication observation: %w", err)
		}
		var targetName string
		if target != nil {
			targetName = target.Name
		}
		publication, counters, err := observation.TakeInlineV1Publication(desiredByIdx, targetName)
		if err != nil {
			return fmt.Errorf("aggregateAndWriteStatus: consume publication observation: %w", err)
		}
		nextAvailableIn = counters.NextAvailableIn
		for i := range fresh.Status.InstanceStatuses {
			fresh.Status.InstanceStatuses[i].Phase = v1beta1convert.InstancePhaseFromWorkload(publication[i].Phase)
			fresh.Status.InstanceStatuses[i].PodCount = publication[i].PodCount
			fresh.Status.InstanceStatuses[i].ReadyPodCount = publication[i].ReadyPodCount
			fresh.Status.InstanceStatuses[i].ServingPodCount = publication[i].ServingPodCount
			fresh.Status.InstanceStatuses[i].AvailablePodCount = publication[i].AvailablePodCount
			fresh.Status.InstanceStatuses[i].ScheduledPodCount = publication[i].ScheduledPodCount
			fresh.Status.InstanceStatuses[i].Admitted = publication[i].Admitted
			fresh.Status.InstanceStatuses[i].NodesOccupied = publication[i].NodesOccupied
		}

		// Component-level counters consume the same publication observation.
		fresh.Status.Replicas = counters.Replicas
		fresh.Status.ServingReplicas = counters.ServingReplicas
		// ReadyReplicas + AvailableReplicas use the publication
		// observation above. The surge-tolerant classifier
		// (InstanceMeetsThreshold) means an Instance with the canonical
		// pod ContainersReady is counted even mid-surge or mid-drain,
		// while availability follows EndpointSlice membership plus the
		// spec.minReadySeconds Ready-age window.
		fresh.Status.ReadyReplicas = counters.ReadyReplicas
		fresh.Status.AvailableReplicas = counters.AvailableReplicas
		if target != nil {
			fresh.Status.UpdatedReplicas = counters.UpdatedReplicas
			fresh.Status.UpdatedReadyReplicas = counters.UpdatedReadyReplicas
			withdrawn := withdrawnRevisionFor(&fresh.Status)
			fresh.Status.UpdateRevision = target.Name
			fresh.Status.ObservedTemplateHash = templateHashOf(ir)
			recordCurrentRevisionWithdrawal(&fresh.Status, withdrawn, fresh.Status.CurrentRevision)
		}
		irstatus.ClearPodDerivedObservations(fresh.Status.InstanceStatuses)

		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.PlacementObservedGeneration = 0
		if ir.Spec.PlacementExecution != nil {
			fresh.Status.PlacementObservedGeneration = fresh.Generation
		}
		fresh.Status.LabelSelector = irLabelSelectorString(fresh.NamePrefix(), fresh.Spec.Component)

		// Top-level Ready condition. Two-axis rule:
		//   Status=True    when ReadyReplicas == Replicas AND
		//                  CurrentRevision == UpdateRevision (fully
		//                  converged, no rollout in flight).
		//   Status=False   when any Instance has Phase=Failed OR the
		//                  serving Instance count is below the availability
		//                  floor (lifecycle rollingUpdate.maxUnavailable)
		//                  with no in-flight rollout.
		//   Status=Unknown when CurrentRevision != UpdateRevision —
		//                  explicit "churning, check back" so
		//                  `kubectl wait --for=condition=Ready` doesn't
		//                  short-circuit mid-rollout.
		apimeta.SetStatusCondition(&fresh.Status.Conditions, computeReadyCondition(&fresh.Status, fresh.Spec.Replicas, fresh.Spec.Lifecycle, fresh.Spec.Pacing))
		// Advisory RolloutStalled condition — surfaces a rollout wedged on
		// failing Instances (e.g. new-rev CrashLoopBackOff) without affecting
		// the Ready condition above (old-rev pods keep serving).
		apimeta.SetStatusCondition(&fresh.Status.Conditions, computeRolloutStalledCondition(&fresh.Status))
		// Advisory DrainOverdue condition — surfaces Instances stuck on
		// their way out past the deadline of the delete operation
		// draining them. Derived from the rows themselves, so it clears
		// on its own once no Instance is overdue.
		apimeta.SetStatusCondition(&fresh.Status.Conditions, computeDrainOverdueCondition(&fresh.Status))
		now := r.now()
		removed := anyInstanceRemoved(instancesAtStart, fresh.Status.InstanceStatuses)
		fresh.Status.RolloutHold = nextRolloutHold(fresh.Status.RolloutHold,
			effectiveRolloutHold(holdObserved, hold, &fresh.Status, removed, now), metav1.NewTime(now))

		// Skip the write when the recomputed status matches what's
		// already live: a no-op reconcile must perform ZERO writes so it
		// triggers nothing downstream. The in-memory mirror below still
		// runs so callers in this pass observe the fresh counters.
		if equality.Semantic.DeepEqual(*before, fresh.Status) {
			mirrorBack(ir, fresh, publication)
			return nil
		}

		if err := updateInferenceReplicaStatus(ctx, r.statusWriter(), fresh, source); err != nil {
			if apierrors.IsNotFound(err) {
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultNotFound)
				ownerUnavailable = true
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: update IR status: %w", err)
		}

		// Mirror committed Component fields and Conditions together with the
		// transient publication counters needed by the rest of this pass.
		mirrorBack(ir, fresh, publication)
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultSuccess)
		return nil
	}); err != nil {
		// A generation change replans without a write outcome. Other terminal
		// retry failures retain their status-write classification.
		if !errors.Is(err, workloadtypes.ErrStatusMutationPrecondition) {
			if apierrors.IsConflict(err) {
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultConflict)
			} else {
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultError)
			}
		}
		return 0, err
	}
	if ownerUnavailable {
		return 0, nil
	}

	// Park the InstanceReadyTimeout clock for any Instance whose pods are
	// held by a scheduling gate (queued for admission, e.g. by Kueue) or
	// by an operator pause: a gated/paused workload is queued, not stuck,
	// so the wait must not count against the deadline. The escalation
	// pass inside workload.Reconcile honors the parked (zeroed) deadline
	// and additionally skips gated Instances outright; once the pods
	// clear the gate the clock restarts from admission.
	if err := r.reconcileHeldDeadlines(ctx, ir, byIndex, plan.Paused, plan.InstanceReadyTimeout); err != nil {
		return 0, err
	}
	return nextAvailableIn, nil
}

// specChangedSincePlanned reports whether the live spec differs from the one
// the pass planned against in anything the pass reads. A generation that
// moved for the values of the inherited annotation keys the revision leaves
// out changes nothing the pass read: the hash ignores them and they reach
// only the pods rendered after the change. The plan, the counters and the
// revision pair a pass computed stay valid for such a spec, so the status
// writes land and name the live generation, and a writer that keeps
// rewriting a service annotation costs no status write.
func specChangedSincePlanned(planned, fresh *v1beta1.InferenceReplica) bool {
	excluded := v1beta1convert.ExcludedAnnotationKeys(fresh)
	return !equality.Semantic.DeepEqual(
		specWithoutExcludedAnnotations(&planned.Spec, excluded),
		specWithoutExcludedAnnotations(&fresh.Spec, excluded))
}

// specWithoutExcludedAnnotations is a copy of spec with the excluded
// annotation keys removed from every runner template; the spec itself when
// no key is excluded.
func specWithoutExcludedAnnotations(spec *v1beta1.InferenceReplicaSpec, excluded map[string]struct{}) *v1beta1.InferenceReplicaSpec {
	if len(excluded) == 0 {
		return spec
	}
	out := spec.DeepCopy()
	for i := range out.Runners {
		out.Runners[i].Template.ObjectMeta = *v1beta1convert.StripExcludedAnnotations(&out.Runners[i].Template.ObjectMeta, excluded)
	}
	return out
}

// mirrorBack copies Component fields and transient publication counters onto
// the caller's in-memory IR. Lifecycle fields remain owned by workload
// operations and are left untouched, except the phase, which comes along so
// the caller's snapshot shows the demotion the publication wrote.
func mirrorBack(ir, fresh *v1beta1.InferenceReplica, publication []workloadtypes.InstanceStatus) {
	ir.Status.Replicas = fresh.Status.Replicas
	ir.Status.ReadyReplicas = fresh.Status.ReadyReplicas
	ir.Status.ServingReplicas = fresh.Status.ServingReplicas
	ir.Status.AvailableReplicas = fresh.Status.AvailableReplicas
	ir.Status.UpdatedReplicas = fresh.Status.UpdatedReplicas
	ir.Status.UpdatedReadyReplicas = fresh.Status.UpdatedReadyReplicas
	ir.Status.UpdateRevision = fresh.Status.UpdateRevision
	ir.Status.ObservedTemplateHash = fresh.Status.ObservedTemplateHash
	ir.Status.CurrentRevision = fresh.Status.CurrentRevision
	ir.Status.ObservedGeneration = fresh.Status.ObservedGeneration
	ir.Status.PlacementObservedGeneration = fresh.Status.PlacementObservedGeneration
	ir.Status.LabelSelector = fresh.Status.LabelSelector
	ir.Status.RolloutHold = fresh.Status.RolloutHold.DeepCopy()
	// The transient publication view is the source for same-pass derived fields.
	mirrorInstanceCounters(&ir.Status, publication)
	// Conditions: deep-copy the just-computed slice so callers reading
	// IR.Status.Conditions in the same reconcile pass observe the
	// post-compute set without sharing a backing array.
	if len(fresh.Status.Conditions) > 0 {
		ir.Status.Conditions = make([]metav1.Condition, len(fresh.Status.Conditions))
		copy(ir.Status.Conditions, fresh.Status.Conditions)
	} else {
		ir.Status.Conditions = nil
	}
}

// reconcileHeldDeadlines parks/restarts InstanceReadyTimeout around either
// admission gating or an operator pause. Pausing holds every in-flight
// Instance and every open move record; unpausing rearms each parked deadline
// from that point unless its pods remain admission-gated.
func (r *Reconciler) reconcileHeldDeadlines(ctx context.Context, ir *v1beta1.InferenceReplica, byIndex map[int32][]*corev1.Pod, paused bool, timeout time.Duration) error {
	if ir == nil {
		return nil
	}
	held := make(map[int32]bool, len(byIndex))
	for idx, pods := range byIndex {
		for _, p := range pods {
			if workloadtypes.PodAdmissionGated(p) {
				held[idx] = true
				break
			}
		}
	}
	// A gang-surge source's attempt pods live in its Operation.SurgeIndex
	// bucket: while they queue for admission, the source's deadline must
	// park too, or it expires during a legitimate wait.
	for i := range ir.Status.InstanceStatuses {
		s := &ir.Status.InstanceStatuses[i]
		op := s.Operation
		if op == nil || op.Type != v1beta1.InstanceOperationUpdate || op.SurgeIndex == nil {
			continue
		}
		if held[*op.SurgeIndex] {
			held[s.Index] = true
		}
	}
	if paused {
		for i := range ir.Status.InstanceStatuses {
			held[ir.Status.InstanceStatuses[i].Index] = true
		}
	}
	input := r.buildDeadlineParkInput(ir)
	if err := escalation.ReconcileGatedDeadlines(ctx, input,
		v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses), held, timeout); err != nil {
		return err
	}
	// The move records' clocks follow the pause the same way: a paused
	// Component drives no record, so none of them runs out under it.
	return escalation.ReconcilePausedMigrationDeadlines(ctx, input, paused, timeout)
}

// buildDeadlineParkInput builds the minimal workload.ReconcileInput the
// InstanceReadyTimeout parking step (escalation.ReconcileGatedDeadlines and
// ReconcilePausedMigrationDeadlines) consumes: the deadline writes go
// through MutateInstance and MutateMigration (same retry +
// in-memory-mirror semantics as dispatch-time writes) and the clock seam
// supplies now. The remaining callbacks satisfy the ReconcileInput
// must-be-set contract as explicit no-ops — parking never removes
// instances, writes conditions, or emits events.
func (r *Reconciler) buildDeadlineParkInput(ir *v1beta1.InferenceReplica) workloadtypes.ReconcileInput {
	return workloadtypes.ReconcileInput{
		OwnerObject:             ir,
		OwnerGVK:                irGVK,
		Key:                     buildKey(ir),
		ObservedState:           observedFromIR(ir),
		Clock:                   r.Clock,
		MutateInstance:          buildMutateInstance(r.statusWriter(), r.liveReader(), ir),
		MutateMigration:         buildMutateMigration(r.statusWriter(), r.liveReader(), ir),
		RemoveInstance:          func(_ context.Context, _ int32) (bool, error) { return false, nil },
		WriteAggregateCondition: func(_ context.Context, _ metav1.Condition) error { return nil },
		WarnInstanceFailed:      func(_ int32, _, _ string) {},
	}
}

// mirrorInstanceCounters copies the published phase and the transient Pod
// counters by Instance index. Other lifecycle fields and admission remain
// untouched.
func mirrorInstanceCounters(status *v1beta1.InferenceReplicaStatus, publication []workloadtypes.InstanceStatus) {
	if status == nil {
		return
	}
	publicationByIdx := make(map[int32]workloadtypes.InstanceStatus, len(publication))
	for _, instance := range publication {
		publicationByIdx[instance.Index] = instance
	}
	for i := range status.InstanceStatuses {
		s := &status.InstanceStatuses[i]
		observed, ok := publicationByIdx[s.Index]
		if !ok {
			continue
		}
		s.Phase = v1beta1convert.InstancePhaseFromWorkload(observed.Phase)
		s.PodCount = observed.PodCount
		s.ReadyPodCount = observed.ReadyPodCount
		s.ServingPodCount = observed.ServingPodCount
		s.ScheduledPodCount = observed.ScheduledPodCount
		s.AvailablePodCount = observed.AvailablePodCount
		if observed.NodesOccupied != nil {
			s.NodesOccupied = append([]string(nil), observed.NodesOccupied...)
		} else {
			s.NodesOccupied = nil
		}
	}
}

// hasFailedInstance reports whether at least one Instance has been
// escalated to Phase=Failed (stuck-pod escalation, InstanceReadyTimeout).
// Used by computeReadyCondition to flip Ready=False when the rollout is
// permanently wedged.
func hasFailedInstance(insts []v1beta1.OMENativeInstanceStatus) bool {
	for _, s := range insts {
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			return true
		}
	}
	return false
}

// withdrawnRevisionFor names the revision the current revision was
// withdrawn from while the status still records the withdrawal: the
// recorded update revision, which the withdrawal left current equal to.
func withdrawnRevisionFor(st *v1beta1.InferenceReplicaStatus) string {
	if st.CurrentRevision != "" || !apimeta.IsStatusConditionTrue(st.Conditions, InferenceReplicaConditionCurrentRevisionWithdrawn) {
		return ""
	}
	return st.UpdateRevision
}

// recordCurrentRevisionWithdrawal keeps the CurrentRevisionWithdrawn
// condition in step with the revision pair a write is about to persist,
// given the withdrawal on record and the current revision it replaces.
func recordCurrentRevisionWithdrawal(st *v1beta1.InferenceReplicaStatus, before, oldCurrent string) {
	if workloadstatus.WithdrawnRevisionAfter(before, oldCurrent, st.CurrentRevision, st.UpdateRevision) == "" {
		apimeta.RemoveStatusCondition(&st.Conditions, InferenceReplicaConditionCurrentRevisionWithdrawn)
		return
	}
	apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:    InferenceReplicaConditionCurrentRevisionWithdrawn,
		Status:  metav1.ConditionTrue,
		Reason:  ReasonInstanceOffRevision,
		Message: "current revision withdrawn while an Instance still runs, or is pinned to, another revision",
	})
}

// computeReadyCondition derives the Ready condition for the IR from
// already-rolled-up status counters + per-Instance phases. Caller passes
// a pre-aggregated IR.Status snapshot (Replicas, ReadyReplicas,
// CurrentRevision, UpdateRevision, InstanceStatuses must all reflect the
// post-counter state). Reason precedence:
//
//  1. InstanceFailed                    — any Instance Phase=Failed (False)
//  2. Staged                            — converged static partition (True)
//  3. RolloutInProgress                 — revision rollout in flight (Unknown)
//  4. NoReplicas                        — Replicas == 0 (False)
//  5. AllInstancesReady                 — ReadyReplicas == Replicas (True)
//  6. MinimumAvailable/ReplicaMismatch  — lifecycle availability floor
//
// Returns a metav1.Condition with ObservedGeneration stamped off
// status.ObservedGeneration so consumers can correlate. The caller is
// responsible for invoking apimeta.SetStatusCondition to merge the
// returned condition into status.Conditions (which handles
// LastTransitionTime correctly).
// effectivePartition returns the partition the IR holds Instances at — the
// projected spec.pacing.partition when set, else the user's lifecycle
// rollingUpdate partition — or 0 when neither is set. It is the same source
// order the workload engine plans with, so the condition below describes the
// shape the engine actually converges to.
func effectivePartition(lifecycle *v1beta1.LifecycleSpec, pacing *v1beta1.InferenceReplicaPacing) int32 {
	p := irprojector.EffectivePartition(lifecycle, pacing)
	if p == nil {
		return 0
	}
	return *p
}

// stagedAtPartition reports whether the IR rests at its staged shape under
// a non-zero partition, and how many Instances that partition holds on the
// prior revision: every Instance Ready, the ones still off the update
// revision held, at least one and at most `partition` of them. A partition
// that asks for more held Instances than still run the prior revision
// (raised above that count mid-roll, or the count scaled below it) holds
// every one of them, so the IR rests staged at what the partition can hold
// rather than reading in progress for good. Zero partition (full rollout)
// is not staged — it converges via the normal promotion path.
func stagedAtPartition(status *v1beta1.InferenceReplicaStatus, lifecycle *v1beta1.LifecycleSpec, pacing *v1beta1.InferenceReplicaPacing) (staged bool, held int32) {
	part := effectivePartition(lifecycle, pacing)
	if part <= 0 {
		return false, 0
	}
	insts := v1beta1convert.InstanceStatusSliceToWorkload(status.InstanceStatuses)
	if !workloadstatus.ReachedDesiredShape(insts, status.UpdateRevision, part, status.Replicas) {
		return false, 0
	}
	return true, workloadstatus.HeldInstanceCount(insts, status.UpdateRevision)
}

// computeRolloutStalledCondition derives the advisory RolloutStalled
// condition (see InferenceReplicaConditionRolloutStalled). True when a rollout
// is in flight (UpdateRevision set and != CurrentRevision) AND one or more
// Instances carry a terminal LastFailure while not yet on the target revision
// — i.e. the rollout is wedged on failing Instances. Keyed on LastFailure (not
// live Phase) so it stays stable even when a stuck Instance oscillates
// Updating<->Failed each reconcile. Advisory only: not folded into Ready.
//
// Failures from before the current attempt started are ignored. LastFailure is
// kept across revision rolls on purpose, so an Instance that failed once would
// otherwise look wedged on every roll after it. That includes an Instance the
// roll has not reached yet: it still serves on its own revision with no
// attempt toward the target, so any failure it carries is from before. An
// attempt still held by the environment-caused condition that ended the last
// one is the exception: a scheduler that still finds no placement makes every
// retry repeat that failure.
func computeRolloutStalledCondition(status *v1beta1.InferenceReplicaStatus) metav1.Condition {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionRolloutStalled,
		ObservedGeneration: status.ObservedGeneration,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonRolloutProgressing,
	}
	if status.UpdateRevision == "" || status.CurrentRevision == status.UpdateRevision {
		cond.Message = "no rollout in flight"
		return cond
	}
	stalled := 0
	reasons := map[string]int{}
	for i := range status.InstanceStatuses {
		s := status.InstanceStatuses[i]
		// Already on the target revision → a stale prior failure isn't blocking.
		if s.LastFailure == nil || s.RunningRevision == status.UpdateRevision {
			continue
		}
		// An announced overdue drain is a visibility record on a row
		// leaving the Component, not a rollout failure; it carries its
		// own condition.
		if s.LastFailure.Reason == workloadtypes.DrainOverdueReason {
			continue
		}
		// It failed before this attempt began, so it is retrying, unless
		// this attempt is held by the condition that failed the last one.
		if op := s.Operation; op != nil && !op.StartedAt.Before(&s.LastFailure.Time) && !heldByLastFailure(op, s.LastFailure) {
			continue
		}
		// The roll has not reached it: it serves on its own revision and
		// nothing targets it yet, so its failure predates this roll.
		if s.Phase == v1beta1.OMENativeInstanceReady && s.Operation == nil && s.TargetRevision != status.UpdateRevision {
			continue
		}
		stalled++
		if r := s.LastFailure.Reason; r != "" {
			reasons[r]++
		}
	}
	if stalled == 0 {
		cond.Message = fmt.Sprintf("Rolling out %s (%d/%d Instances on target)",
			status.UpdateRevision, status.UpdatedReadyReplicas, status.Replicas)
		return cond
	}
	cond.Status = metav1.ConditionTrue
	cond.Reason = ReasonInstancesFailing
	cond.Message = fmt.Sprintf("%d/%d Instance(s) failing rollout to %s (%s)",
		stalled, status.Replicas, status.UpdateRevision, summarizeFailureReasons(reasons))
	return cond
}

// heldByLastFailure reports whether an open attempt is held by the
// environment-caused condition its Instance's last failure recorded. The
// roll opens the next attempt in the pass that ends the last one, so that
// failure's time can fall in the same second as this attempt's start, and
// the times alone cannot tell the two refusals apart; the hold can.
func heldByLastFailure(op *v1beta1.InstanceOperation, failure *v1beta1.InstanceTermination) bool {
	return op.Waiting != "" && op.Waiting == failure.Reason && workloadtypes.IsEnvironmentCausedReason(failure.Reason)
}

// computeDrainOverdueCondition derives the advisory DrainOverdue
// condition (see InferenceReplicaConditionDrainOverdue) from the
// announcement records the delete pass leaves on the rows it is still
// waiting on. True while any Instance is Deleting past the deadline of
// the delete operation draining it, counting them so an operator sees
// how much of the scale-down is wedged.
//
// Derived, not written: the row's own disappearance at the end of the
// drain clears the condition, and nothing has to remember to retract it.
func computeDrainOverdueCondition(status *v1beta1.InferenceReplicaStatus) metav1.Condition {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionDrainOverdue,
		ObservedGeneration: status.ObservedGeneration,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonDrainsWithinDeadline,
		Message:            "no drain is past its deadline",
	}
	overdue := make([]string, 0)
	for i := range status.InstanceStatuses {
		s := status.InstanceStatuses[i]
		if s.Phase != v1beta1.OMENativeInstanceDeleting || s.LastFailure == nil {
			continue
		}
		if s.LastFailure.Reason != workloadtypes.DrainOverdueReason {
			continue
		}
		overdue = append(overdue, strconv.Itoa(int(s.Index)))
	}
	if len(overdue) == 0 {
		return cond
	}
	cond.Status = metav1.ConditionTrue
	cond.Reason = ReasonDrainsOverdue
	cond.Message = fmt.Sprintf("%d Instance(s) draining past their deadline (%s)",
		len(overdue), strings.Join(overdue, ", "))
	return cond
}

// summarizeFailureReasons renders a stable, compact "Reason xN" summary
// (sorted by reason for byte-stability across reconciles).
func summarizeFailureReasons(reasons map[string]int) string {
	if len(reasons) == 0 {
		return "terminal failure"
	}
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s x%d", k, reasons[k]))
	}
	return strings.Join(parts, ", ")
}

// effectiveRolloutHold resolves this pass's RolloutHold observation for
// InferenceReplicaConditionRolloutStalled's sibling status.rolloutHold
// field. A Component with no rollout in flight (or already converged) has
// nothing to hold regardless of any stale signal.
//
// When the Update pass ran this reconcile (holdObserved), its verdict is
// authoritative: a RetryBlock-denied Instance never reaches the Update
// pass (see workloadops.evaluateUpdateTriggerFast), so a nil verdict here
// means genuine forward progress or nothing needed updating. When the
// Update pass did NOT run — the pass ended before it on a repair, a scale
// wave or a dispatch hold, or Plan selected no ActionUpdate because every
// candidate Instance was denied before reaching it, most commonly a
// same-target RetryBlock — the hold is a wait that has not ended: a
// same-target RetryBlock in Held or not-yet-due Backoff denies every
// fresh start and is the hold that stands; otherwise the persisted hold
// stands as written until a pass that runs the Update pass replaces or
// clears it.
//
// A recorded hold describes the Instances the pass that wrote it saw. A
// pass that removed an Instance (instanceRemoved: a row the pass began
// with is gone from the live object, as after a scale wave's last write)
// therefore carries no hold it did not observe: the next pass that reaches
// the update position records whatever stands over the Instances that
// remain, and a hold naming an Instance that is gone never outlives its
// row. The retry record is read first either way, since it is live state.
//
// A converged subject is read the same way: a roll with nothing left to
// do clears the hold with a nil verdict, which a pass that finds every
// row settled and serving reports, while one whose promoted sets crash
// after it completed stands behind the budget while a set is out of
// rotation, or behind the ladder while its block counts the crash, and
// its verdict says so. Only a subject with no update revision has
// nothing to hold.
func effectiveRolloutHold(holdObserved bool, hold *workloadtypes.RolloutHold, status *v1beta1.InferenceReplicaStatus, instanceRemoved bool, now time.Time) *v1beta1.RolloutHold {
	if status.UpdateRevision == "" {
		return nil
	}
	if holdObserved {
		if hold == nil {
			return nil
		}
		return &v1beta1.RolloutHold{
			Gate:   v1beta1.RolloutHoldGate(hold.Gate),
			Reason: hold.Reason,
			Target: hold.Target,
		}
	}
	if blocked := computeRetryBlockRolloutHold(status, now); blocked != nil {
		return blocked
	}
	if instanceRemoved {
		return nil
	}
	return status.RolloutHold
}

// instanceIndices lists the Instances the rows describe, in row order.
func instanceIndices(rows []v1beta1.OMENativeInstanceStatus) []int32 {
	out := make([]int32, 0, len(rows))
	for i := range rows {
		out = append(out, rows[i].Index)
	}
	return out
}

// anyInstanceRemoved reports whether an Instance that had a row when the
// pass began (atStart) has none among rows, the live rows the pass leaves.
func anyInstanceRemoved(atStart []int32, rows []v1beta1.OMENativeInstanceStatus) bool {
	present := make(map[int32]struct{}, len(rows))
	for i := range rows {
		present[rows[i].Index] = struct{}{}
	}
	for _, idx := range atStart {
		if _, ok := present[idx]; !ok {
			return true
		}
	}
	return false
}

// computeRetryBlockRolloutHold derives a RolloutHold from a same-target
// RetryBlock in Held or not-yet-due Backoff — the two RetryBlock states
// that stop every fresh Update attempt at the current target (see
// workloadops.evaluateRetryBlockGate). Returns nil when the target
// carries no block, the block is RetryInProgress (an attempt is
// authorized), or a Backoff whose NextRetryAt has already elapsed (about
// to retry, not currently held).
//
// The reason names the Instances on the block's revision that do not
// serve it, in the words the update pass uses for the same hold, so the
// account an operator reads does not change with the pass that wrote
// it: a record Held on the revision every Instance runs, as after a
// rollback onto it, names the Instance it keeps down and the failure
// that spent its attempts.
func computeRetryBlockRolloutHold(status *v1beta1.InferenceReplicaStatus, now time.Time) *v1beta1.RolloutHold {
	b, found := retryBlockForRevision(status.RetryBlocks, status.UpdateRevision)
	if !found {
		return nil
	}
	notServing := workloadtypes.InstancesNotServingClause(instancesNotServingOn(status.InstanceStatuses, b.TargetRevision), b.TargetRevision)
	switch b.State {
	case v1beta1.RetryBlockHeld:
		return &v1beta1.RolloutHold{
			Gate:   v1beta1.RolloutHoldGateHeld,
			Reason: irprojector.HeldUpdateReason(b) + notServing,
			Target: b.TargetRevision,
		}
	case v1beta1.RetryBlockBackoff:
		if b.NextRetryAt != nil && b.NextRetryAt.Time.After(now) {
			return &v1beta1.RolloutHold{
				Gate:   v1beta1.RolloutHoldGateRetryBlock,
				Reason: fmt.Sprintf("update to %s in backoff after %d failed attempt(s): %s%s", b.TargetRevision, b.AttemptsStarted, b.Reason, notServing),
				Target: b.TargetRevision,
			}
		}
	}
	return nil
}

// instancesNotServingOn lists, in row order, the Instances that run
// revision and do not serve it, as the published rows read: a row
// Failed on it, one whose pod set is being rebuilt (Restarting), or one
// left with no pod set (Pending). A Ready row serves. A row leaving the
// revision under an update, a move or a delete is another pass's to
// report, and a row that never ran the revision is not on it.
func instancesNotServingOn(rows []v1beta1.OMENativeInstanceStatus, revision string) []int32 {
	var out []int32
	for i := range rows {
		row := &rows[i]
		if revision == "" || row.RunningRevision != revision {
			continue
		}
		switch row.Phase {
		case v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstancePending:
			out = append(out, row.Index)
		}
	}
	return out
}

// nextRolloutHold merges this pass's observed RolloutHold onto the
// previously persisted value. Since is preserved when Gate and Target are
// unchanged — mirroring how apimeta.SetStatusCondition preserves
// LastTransitionTime while a condition's Status is unchanged — so a gate
// whose display Reason fluctuates (e.g. a countdown) without a real
// change of cause does not reset the "held since" anchor. observed == nil
// clears the hold outright (progressed or converged).
func nextRolloutHold(existing, observed *v1beta1.RolloutHold, now metav1.Time) *v1beta1.RolloutHold {
	if observed == nil {
		return nil
	}
	if existing != nil && existing.Gate == observed.Gate && existing.Target == observed.Target {
		observed.Since = existing.Since
		return observed
	}
	observed.Since = now
	return observed
}

func computeReadyCondition(status *v1beta1.InferenceReplicaStatus, desiredReplicas *int32, lifecycle *v1beta1.LifecycleSpec, pacing *v1beta1.InferenceReplicaPacing) metav1.Condition {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionReady,
		ObservedGeneration: status.ObservedGeneration,
	}
	staged, held := stagedAtPartition(status, lifecycle, pacing)
	switch {
	case hasFailedInstance(status.InstanceStatuses):
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonInstanceFailed
		cond.Message = "At least one Instance has Phase=Failed"
	case staged:
		// Converged to a static partition: intentionally holding old-revision
		// Instances, all Ready. Ready=True (not RolloutInProgress/Unknown) —
		// the rollout is complete for the configured partition. A partition
		// that asks for more held Instances than still run the prior revision
		// holds every one of them; the message names the shortfall.
		part := effectivePartition(lifecycle, pacing)
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonStaged
		cond.Message = fmt.Sprintf("Staged at partition %d: %d/%d Instances on %s, %d held on the prior revision",
			part, status.Replicas-held, status.Replicas, status.UpdateRevision, held)
		if held < part {
			cond.Message += fmt.Sprintf(" (%d fewer than the partition asks for)", part-held)
		}
	case status.UpdateRevision != "" && status.CurrentRevision != status.UpdateRevision:
		cond.Status = metav1.ConditionUnknown
		cond.Reason = ReasonRolloutInProgress
		cond.Message = fmt.Sprintf("Rolling out %s (%d/%d Instances on target)",
			status.UpdateRevision, status.UpdatedReadyReplicas, status.Replicas)
	case status.Replicas == 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonNoReplicas
		cond.Message = "InferenceReplica has no desired Instances"
	case status.ReadyReplicas == status.Replicas:
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonAllInstancesReady
		cond.Message = fmt.Sprintf("%d/%d Instances Ready", status.ReadyReplicas, status.Replicas)
	default:
		// Ordinary readiness uses the Instance lifecycle budget. Pacing controls
		// staged rollout coordination and does not relax this serving floor.
		floor := isvcstatus.InstanceAvailabilityFloor(status.Replicas, desiredReplicas, lifecycle)
		if status.ServingReplicas >= floor {
			cond.Status = metav1.ConditionTrue
			cond.Reason = ReasonMinimumAvailable
			cond.Message = fmt.Sprintf("%d/%d Instances serving (min %d)",
				status.ServingReplicas, status.Replicas, floor)
		} else {
			cond.Status = metav1.ConditionFalse
			cond.Reason = ReasonReplicaCountMismatch
			cond.Message = fmt.Sprintf("%d/%d Instances serving, need %d (no rollout in flight)",
				status.ServingReplicas, status.Replicas, floor)
		}
	}
	return cond
}

// irLabelSelectorString returns the selector the HPA scale subresource reads
// from IR.status.labelSelector: the name-prefix, component and managed-by
// labels, narrowed to the runners an Instance has exactly one of. The HPA
// multiplies a Value or Utilization ratio by the Ready pods this selects and
// writes the result as an Instance count, so it must select one pod per
// Instance: the "default" pod of a single-pod Instance, the leader of a
// multi-pod one.
func irLabelSelectorString(namePrefix string, component v1beta1.ComponentType) string {
	return labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: namePrefix,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}).String() + "," + query.LabelRunner + " in (" + workloadtypes.RunnerDefault + "," + workloadtypes.RunnerLeader + ")"
}
