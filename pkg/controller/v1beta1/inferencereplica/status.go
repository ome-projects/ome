package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	isvcstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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

const (
	// ReasonInstancesFailing — RolloutStalled=True: >=1 Instance recorded a
	// terminal failure while not yet on the target revision.
	ReasonInstancesFailing = "InstancesFailing"
	// ReasonRolloutProgressing — RolloutStalled=False: rollout in flight with
	// no failing Instances, or no rollout in flight.
	ReasonRolloutProgressing = "Progressing"
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
	// ReasonStaged is stamped when the IR has converged to a static
	// rollingUpdate.partition — (replicas-partition) Instances on the target
	// revision and `partition` held on the prior one, all Ready. Ready=True:
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

// aggregateAndWriteStatus publishes cached Pod and EndpointSlice facts over
// freshly read lifecycle rows, then computes the IR summaries and conditions.
// Lifecycle fields and CurrentRevision retain their dedicated writers. A nil
// target leaves revision pointers untouched while counters still publish.
//
// holdObserved/hold carry this pass's Update-pass rollout-hold verdict
// (ReconcileInput.RecordRolloutHold): holdObserved is true when the Update
// pass ran at all this reconcile, and hold is its verdict (nil clears).
// When the Update pass did not run (holdObserved is false — nothing needed
// updating this pass, or Plan selected no target), the RetryBlock/Held
// state persisted in status is the only signal available, since a
// RetryBlock-denied Instance never reaches the Update pass to be gated
// there — see computeRetryBlockRolloutHold.
//
// The returned duration is how long until the earliest in-rotation pod still
// inside the spec.minReadySeconds window becomes Available (0 when none is
// pending). That raises the Available counters without any watch event, so
// the caller folds it into the reconcile result to publish them on time.
func (r *Reconciler) aggregateAndWriteStatus(ctx context.Context, ir *v1beta1.InferenceReplica, plan workload.ComponentPlan, target *appsv1.ControllerRevision, holdObserved bool, hold *workload.RolloutHold) (time.Duration, error) {
	if r.Client == nil {
		return 0, fmt.Errorf("aggregateAndWriteStatus: nil client")
	}
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	ownerGeneration := ir.Generation
	desiredByIdx := workload.DesiredPodCountByInstance(plan)
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)

	// List pods + EndpointSlices via the CACHED client (r.Client), outside
	// the retry closure: both reads are idempotent (no apiserver-side
	// mutation), so on a status conflict we just re-stamp the same counters
	// onto the re-read IR. Mirrors the omenative direct path which reads pods
	// + availability once at the top of AggregateAndWriteStatus.
	//
	// This aggregator is observability-only COUNTING — the manager cache is
	// scoped to OME pods, has the OMENative Pod field index registered, and
	// watches EndpointSlices, so cache-served counts are both correct and
	// cheap. Going live (r.APIReader) on every reconcile would be the dominant
	// hot-path cost: an uncached LIST per IR reconcile purely for counters,
	// plus a MatchingFields probe the live reader can't serve. r.APIReader
	// stays reserved for the destructive/correctness-critical paths
	// (drain/delete/recreate confirm) that genuinely need a live read.
	pods, err := query.ListOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.Spec.ParentRef.Name, component, true)
	if err != nil {
		return 0, fmt.Errorf("aggregateAndWriteStatus: list pods: %w", err)
	}
	byIndex := query.BucketPodsByInstanceIdx(pods)
	podObservation := workload.NewCachedSelectorPodObservation(pods, byIndex)
	serviceName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, component)
	availableByPod, err := workload.AvailablePodSet(ctx, r.Client, ir.Namespace, serviceName)
	if err != nil {
		return 0, fmt.Errorf("aggregateAndWriteStatus: compute availability: %w", err)
	}

	ownerUnavailable := false
	var nextAvailableIn time.Duration
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.APIReader.Get(ctx, key, fresh); err != nil {
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
		if fresh.Generation != ownerGeneration {
			return workload.ErrStatusMutationPrecondition
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
			workload.AvailabilityWindow{MinReadySeconds: plan.MinReadySeconds, Now: r.now()},
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
			fresh.Status.UpdateRevision = target.Name
		}
		clearPodDerivedInstanceObservations(fresh)

		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.LabelSelector = irLabelSelectorString(fresh.Spec.ParentRef.Name, fresh.Spec.Component)

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
		now := r.now()
		fresh.Status.RolloutHold = nextRolloutHold(fresh.Status.RolloutHold,
			effectiveRolloutHold(holdObserved, hold, &fresh.Status, now), metav1.NewTime(now))

		// Skip the write when the recomputed status matches what's
		// already live: a no-op reconcile must perform ZERO writes so it
		// triggers nothing downstream. The in-memory mirror below still
		// runs so callers in this pass observe the fresh counters.
		if equality.Semantic.DeepEqual(*before, fresh.Status) {
			mirrorBack(ir, fresh, publication)
			return nil
		}

		if err := updateInferenceReplicaStatus(ctx, r.Client, fresh); err != nil {
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
		if !errors.Is(err, workload.ErrStatusMutationPrecondition) {
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

// mirrorBack copies Component fields and transient publication counters onto
// the caller's in-memory IR. Lifecycle fields remain owned by workload
// operations and are left untouched.
func mirrorBack(ir, fresh *v1beta1.InferenceReplica, publication []workload.InstanceStatus) {
	ir.Status.Replicas = fresh.Status.Replicas
	ir.Status.ReadyReplicas = fresh.Status.ReadyReplicas
	ir.Status.ServingReplicas = fresh.Status.ServingReplicas
	ir.Status.AvailableReplicas = fresh.Status.AvailableReplicas
	ir.Status.UpdatedReplicas = fresh.Status.UpdatedReplicas
	ir.Status.UpdatedReadyReplicas = fresh.Status.UpdatedReadyReplicas
	ir.Status.UpdateRevision = fresh.Status.UpdateRevision
	ir.Status.CurrentRevision = fresh.Status.CurrentRevision
	ir.Status.ObservedGeneration = fresh.Status.ObservedGeneration
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
// Instance; unpausing rearms each parked deadline from that point unless its
// pods remain admission-gated.
func (r *Reconciler) reconcileHeldDeadlines(ctx context.Context, ir *v1beta1.InferenceReplica, byIndex map[int32][]*corev1.Pod, paused bool, timeout time.Duration) error {
	if ir == nil {
		return nil
	}
	held := make(map[int32]bool, len(byIndex))
	for idx, pods := range byIndex {
		for _, p := range pods {
			if workload.PodAdmissionGated(p) {
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
	return workload.ReconcileGatedDeadlines(ctx, r.buildDeadlineParkInput(ir),
		v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses), held, timeout)
}

// buildDeadlineParkInput builds the minimal workload.ReconcileInput the
// InstanceReadyTimeout parking step (workload.ReconcileGatedDeadlines)
// consumes: the deadline write goes through MutateInstance (same retry +
// in-memory-mirror semantics as dispatch-time writes) and the clock seam
// supplies now. The remaining callbacks satisfy the ReconcileInput
// must-be-set contract as explicit no-ops — parking never removes
// instances, writes conditions, or emits events.
func (r *Reconciler) buildDeadlineParkInput(ir *v1beta1.InferenceReplica) workload.ReconcileInput {
	return workload.ReconcileInput{
		OwnerObject:             ir,
		OwnerGVK:                irGVK,
		Key:                     buildKey(ir),
		ObservedState:           observedFromIR(ir),
		Clock:                   r.Clock,
		MutateInstance:          buildMutateInstance(r.Client, r.APIReader, ir),
		RemoveInstance:          func(_ context.Context, _ int32) (bool, error) { return false, nil },
		WriteAggregateCondition: func(_ context.Context, _ metav1.Condition) error { return nil },
		WarnInstanceFailed:      func(_ int32, _, _ string) {},
	}
}

// mirrorInstanceCounters copies transient Pod counters by Instance index.
// Lifecycle and admission fields remain untouched.
func mirrorInstanceCounters(status *v1beta1.InferenceReplicaStatus, publication []workload.InstanceStatus) {
	if status == nil {
		return
	}
	publicationByIdx := make(map[int32]workload.InstanceStatus, len(publication))
	for _, instance := range publication {
		publicationByIdx[instance.Index] = instance
	}
	for i := range status.InstanceStatuses {
		s := &status.InstanceStatuses[i]
		observed, ok := publicationByIdx[s.Index]
		if !ok {
			continue
		}
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
// effectivePartition returns the IR's static rollingUpdate.partition, or 0
// when unset.
func effectivePartition(pacing *v1beta1.InferenceReplicaPacing) int32 {
	if pacing == nil || pacing.Partition == nil {
		return 0
	}
	return *pacing.Partition
}

// stagedAtPartition reports whether the IR has converged to a static,
// non-zero partition: (replicas-partition) Instances Ready on the target
// revision and `partition` Instances held Ready on the prior one. Zero
// partition (full rollout) is not staged — it converges via the normal
// promotion path.
func stagedAtPartition(status *v1beta1.InferenceReplicaStatus, pacing *v1beta1.InferenceReplicaPacing) bool {
	part := effectivePartition(pacing)
	if part <= 0 {
		return false
	}
	return workload.ReachedDesiredShape(
		v1beta1convert.InstanceStatusSliceToWorkload(status.InstanceStatuses),
		status.UpdateRevision, part, status.Replicas)
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
// otherwise look wedged on every roll after it.
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
		// It failed before this attempt began, so it is retrying, not stuck.
		if op := s.Operation; op != nil && !op.StartedAt.Before(&s.LastFailure.Time) {
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
// Update pass did NOT run — Plan selected no ActionUpdate because every
// candidate Instance was denied before reaching it, most commonly a
// same-target RetryBlock — the persisted RetryBlock/Held state is the
// only available signal.
func effectiveRolloutHold(holdObserved bool, hold *workload.RolloutHold, status *v1beta1.InferenceReplicaStatus, now time.Time) *v1beta1.RolloutHold {
	if status.UpdateRevision == "" || status.CurrentRevision == status.UpdateRevision {
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
	return computeRetryBlockRolloutHold(status, now)
}

// computeRetryBlockRolloutHold derives a RolloutHold from a same-target
// RetryBlock in Held or not-yet-due Backoff — the two RetryBlock states
// that stop every fresh Update attempt at the current target (see
// workloadops.evaluateRetryBlockGate). Returns nil when the target
// carries no block, the block is RetryInProgress (an attempt is
// authorized), or a Backoff whose NextRetryAt has already elapsed (about
// to retry, not currently held).
func computeRetryBlockRolloutHold(status *v1beta1.InferenceReplicaStatus, now time.Time) *v1beta1.RolloutHold {
	b, found := retryBlockForRevision(status.RetryBlocks, status.UpdateRevision)
	if !found {
		return nil
	}
	switch b.State {
	case v1beta1.RetryBlockHeld:
		return &v1beta1.RolloutHold{
			Gate:   v1beta1.RolloutHoldGateHeld,
			Reason: fmt.Sprintf("update to %s held after %d failed attempt(s): %s", b.TargetRevision, b.AttemptsStarted, b.Reason),
			Target: b.TargetRevision,
		}
	case v1beta1.RetryBlockBackoff:
		if b.NextRetryAt != nil && b.NextRetryAt.Time.After(now) {
			return &v1beta1.RolloutHold{
				Gate:   v1beta1.RolloutHoldGateRetryBlock,
				Reason: fmt.Sprintf("update to %s in backoff after %d failed attempt(s): %s", b.TargetRevision, b.AttemptsStarted, b.Reason),
				Target: b.TargetRevision,
			}
		}
	}
	return nil
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
	switch {
	case hasFailedInstance(status.InstanceStatuses):
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonInstanceFailed
		cond.Message = "At least one Instance has Phase=Failed"
	case stagedAtPartition(status, pacing):
		// Converged to a static partition: intentionally holding old-revision
		// Instances, all Ready. Ready=True (not RolloutInProgress/Unknown) —
		// the rollout is complete for the configured partition.
		part := effectivePartition(pacing)
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonStaged
		cond.Message = fmt.Sprintf("Staged at partition %d: %d/%d Instances on %s, %d held on the prior revision",
			part, status.Replicas-part, status.Replicas, status.UpdateRevision, part)
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

// irLabelSelectorString returns the canonical "k=v,k=v" selector
// string the HPA scale subresource consumes via
// IR.status.labelSelector. Formula matches the legacy ISVC-side
// componentLabelSelectorString byte-for-byte so existing HPAs
// continue to resolve unchanged after a workload migrates from
// ISVC-direct to IR-driven.
func irLabelSelectorString(isvc string, component v1beta1.ComponentType) string {
	return labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}).String()
}
