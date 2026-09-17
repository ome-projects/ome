package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// MigrateRequeueInterval is the wait between Migrate passes while a
// surge migration is in flight. Exported so the dispatcher's pacing
// stays in lockstep.
const MigrateRequeueInterval = 5 * time.Second

// ledgerOwnerObject / ledgerOwnerGVK resolve the object that owns the
// migration audit-ledger ConfigMap. The IR caller sets input.LedgerOwner
// to the parent ISVC (so the ledger + the operator's migration-request
// annotation share the user-facing resource while the IR owns the pods);
// every other caller leaves it nil and the ledger lives on OwnerObject.
func ledgerOwnerObject(input workload.ReconcileInput) client.Object {
	if input.LedgerOwner != nil {
		return input.LedgerOwner
	}
	return input.OwnerObject
}

func ledgerOwnerGVK(input workload.ReconcileInput) schema.GroupVersionKind {
	if input.LedgerOwner != nil {
		return input.LedgerOwnerGVK
	}
	return input.OwnerGVK
}

// Migrate drives one surge migration toward completion. Work comes
// from the owner's status.migrations record for requestUUID (the
// single source of truth — the dispatcher selects the record, the
// executor resumes from its SurgeInstance + Phase and advances the
// phase through ReconcileInput.MutateMigration). Multi-pass and
// crash-safe: the record is the resume anchor; the audit ConfigMap is
// history only.
//
// Sequence (surge-only):
//
//  1. Terminal record → done. Fresh record (SurgeInstance unset):
//     run the fresh-request guards (steady-Ready source,
//     validateFromNode, capacity over status.migrations, overlay
//     pre-check); rejections mark the record Failed.
//  2. Allocate surge index = lowest unused; write it back to the
//     record (SurgeInstance + Phase=SurgePending) FIRST, then stamp
//     source Phase=Migrating + surge Phase=Creating and update the
//     ledger Started row with the real surge index. The record-first
//     order makes the record the crash anchor; the pair stamps are
//     re-ensured on resume while the record is still <= SurgePending.
//  3. Reuse source's RunningRevision as the surge template — the surge
//     mirrors the source's full Runner layout (a single "default" pod,
//     or leader + workers for a gang); create surge pods with the
//     migration anti-affinity overlay.
//  4. Wait surge ContainersReady + in-rotation + Available; flip
//     serving; record Phase=SurgeReady.
//  5. Drain source (serving=False, wait drain.IsPodDrained), delete;
//     record Phase=Draining at drain start.
//  6. Promote surge Ready+RunningRevision, drop source InstanceStatus,
//     persist Completed ledger entry, then record Phase=Completed +
//     CompletedAt — in that order. A Draining record whose source status
//     and pods are already absent resumes at resource finalization and
//     re-runs the idempotent completion tail.
//
// Multi-pod (gang) Instances migrate as a whole: the surge copies the
// source's Runner layout and worker template from its RunningRevision;
// a gang surge requeues one pass after stamping so EnsurePodGroups
// creates the surge PodGroup before the gang's pods render;
// validateFromNode is gang-aware (FromNode must host at least one
// member); the rotation/drain gates assert the routable leader only —
// workers are never routed.
//
// Returns (done, accepted, err):
//   - done=true: migration finished (success or terminal failure);
//     caller continues to the next reconcile pass without requeue from
//     the migration branch.
//   - done=false, accepted=true: migration is mid-flight (status
//     stamped, ledger Started, or driving an existing in-flight pair);
//     caller MUST requeue and SHOULD NOT fall through to Update/Create
//     (DetectUpdateTrigger suppresses Migrate-owned status anyway).
//   - done=false, accepted=false: migration deferred without taking
//     ownership (fresh request, source not yet in a steady Ready state
//     because of an in-flight Update/Restart/Create). Caller SHOULD
//     fall through to Update/Create/etc. so the in-flight op converges;
//     otherwise the dispatcher loops indefinitely in the Migrate-defer
//     branch and the source never reaches Ready (silent deadlock).
func Migrate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx int32, requestUUID string, req *audit.MigrationRequest) (done bool, accepted bool, err error) {
	if deps.Client == nil {
		return false, false, fmt.Errorf("Migrate: nil client")
	}
	if req == nil {
		return false, false, fmt.Errorf("Migrate: nil request (instance=%d)", sourceIdx)
	}
	if input.MutateMigration == nil {
		return false, false, fmt.Errorf("Migrate: MutateMigration not wired (uuid=%s)", requestUUID)
	}
	entry := workload.FindMigrationRecord(input.ObservedState.Migrations, requestUUID)
	if entry == nil {
		return false, false, fmt.Errorf("Migrate: no status.migrations record for uuid=%s", requestUUID)
	}
	if entry.Phase.Terminal() {
		// Already terminal — caller can continue. accepted=true so the
		// caller knows the request was already taken and is fully
		// handled (no fall-through needed).
		return true, true, nil
	}

	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), ledgerOwnerObject(input))
	if err != nil {
		return false, false, fmt.Errorf("Migrate: load audit ledger: %w", err)
	}

	// Multi-pod (gang) Instances migrate the whole gang to the surge
	// index — see the gang-shaped surgeInst + WorkerPodSpec wiring + the
	// surge-gang PodGroup below.
	gang := isMultiPodInstance(plan, sourceIdx)

	// Resume from the record's allocated surge index; allocate on a
	// fresh record (SurgeInstance unset, or the pre-allocation -1
	// sentinel imported from a legacy ledger row).
	source := findInstanceStatus(input.ObservedState.InstanceStatuses, sourceIdx)
	var surge *workload.InstanceStatus
	var surgeIdx int32
	pairConfirmedForEffects := false
	// freshStamp records that THIS pass allocated + stamped the surge
	// (the default case below). A gang surge requeues right after, so the
	// next pass's EnsurePodGroups creates the surge PodGroup before any
	// surge pod is rendered.
	freshStamp := false
	if entry.SurgeInstance != nil && *entry.SurgeInstance >= 0 {
		// In-flight — accepted on a prior pass; the record is the anchor.
		surgeIdx = *entry.SurgeInstance
		surge = findInstanceStatus(input.ObservedState.InstanceStatuses, surgeIdx)
		accepted = true
		// Crash window between the record's SurgeInstance write and the
		// pair stamps: while the record is still <= SurgePending the
		// source has not begun draining, so re-ensuring the (idempotent)
		// stamps here closes the window without ever resurrecting a
		// source status the completion tail already removed.
		if !workload.MigrationPhaseAtOrPast(entry.Phase, workload.MigrationPhaseSurgeReady) {
			// A surge slot absent from this pass's ObservedState means
			// EnsurePodGroups ran without the surge index — treat the
			// re-ensured stamp like a fresh one so a gang surge requeues
			// below and gets its PodGroup before its pods render.
			freshStamp = surge == nil
			if input.ApplyInstanceMutationsWithRetryBlock != nil {
				confirmed, cerr := establishMigrationPair(
					ctx, input, source, surge, requestUUID, surgeIdx, plan.InstanceReadyTimeout,
				)
				if cerr != nil {
					return false, accepted, fmt.Errorf("Migrate: ensure migration pair: %w", cerr)
				}
				if !confirmed {
					return false, accepted, nil
				}
				pairConfirmedForEffects = true
			} else {
				if err := patchInstanceStatusMigrating(ctx, input, sourceIdx, surgeIdx, requestUUID, plan.InstanceReadyTimeout); err != nil {
					return false, accepted, fmt.Errorf("Migrate: ensure source stamp (instance=%d): %w", sourceIdx, err)
				}
				if err := patchInstanceStatusMigrationSurge(ctx, input, surgeIdx, sourceIdx, requestUUID, plan.InstanceReadyTimeout); err != nil {
					return false, accepted, fmt.Errorf("Migrate: ensure surge stamp (instance=%d): %w", surgeIdx, err)
				}
			}
		}
	} else {
		// Fresh record: refuse to stamp Migrating unless source is a
		// steady Ready with a recorded revision. Without these guards a
		// mid-Create stamp deadlocks Create and a mid-Update / Restart
		// stamp silently steamrolls the in-flight op.
		if source == nil {
			d, ferr := failMigration(ctx, deps, input, ledger, req, requestUUID, "source InstanceStatus missing")
			return d, true, ferr
		}
		if source.Phase != workload.InstancePhaseReady || source.Operation != nil || source.RunningRevision == "" {
			// Defer without taking ownership — signal to caller that
			// fall-through is safe so the in-flight op (Update/Restart/
			// Create) can converge. accepted=false is load-bearing here;
			// the record stays Accepted and retries next pass.
			return false, false, nil
		}

		// Validate FromNode against where source pods actually run. The
		// requester's view may be stale; if source has since moved, the
		// surge's NotIn[FromNode] could land on the SAME node as the
		// post-move source — silently no-op'ing the migration.
		switch mismatch, defer_, err := validateFromNode(ctx, deps, input, plan, sourceIdx, req.FromNode); {
		case err != nil:
			return false, false, fmt.Errorf("Migrate: validate from-node (instance=%d): %w", sourceIdx, err)
		case defer_:
			// Transient (unscheduled source pod). Defer without ownership;
			// caller can fall through. validateFromNode's defer_ path is a
			// fresh-request guard and must not block other ops.
			return false, false, nil
		case mismatch != "":
			recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationFromNodeMismatch,
				"OMENative migration uuid=%s rejected: %s", requestUUID, mismatch)
			d, ferr := failMigration(ctx, deps, input, ledger, req, requestUUID, mismatch)
			return d, true, ferr
		}

		// Capacity / rate-limit gate over the owner's migration records —
		// EXECUTION semantics (excluding this request's own record):
		// in-flight = non-terminal records with an allocated surge,
		// per-hour = AllocatedAt inside the trailing window. Queued
		// Accepted records are unbounded by design (serial dispatch;
		// they hold nothing).
		if ok, reason := audit.ValidateCapacity(input.ObservedState.Migrations, requestUUID, deps.Now()); !ok {
			recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonRateLimited,
				"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
			d, ferr := failMigration(ctx, deps, input, ledger, req, requestUUID, reason)
			return d, true, ferr
		}

		// Resolve the source's RunningRevision and check the overlay BEFORE
		// stamping — against the leader AND (for a gang) the worker
		// template, the same two-spec check the resume path runs: a
		// worker-pinned gang that passed a leader-only check would stamp
		// the pair and then terminally fail on resume, orphaning the
		// stamped pair until the legacy deadline. A post-stamp rejection
		// leaves source Phase=Migrating for ~instanceReadyTimeout until
		// the timeout cleanup fires — operators see a wedged status and
		// the requester can't observe the rejection within its SLA.
		// (nil, nil) means the CR was GC'd or RunningRevision isn't set
		// yet — defer without ownership so other ops can run.
		_, preRevSpec, preWorkerSpec, err := surgeRevisionAndSpec(ctx, deps, input, sourceIdx)
		if err != nil {
			return false, false, fmt.Errorf("Migrate: resolve surge revision: %w", err)
		}
		if preRevSpec == nil {
			return false, false, nil
		}
		preOverlay := &workload.MigrationOverlay{
			FromNode:        req.FromNode,
			HintTargetNodes: req.HintTargetNodes,
		}
		if WouldOverlayConflictWithNodeAffinity(preRevSpec, preOverlay) ||
			(preWorkerSpec != nil && WouldOverlayConflictWithNodeAffinity(preWorkerSpec, preOverlay)) {
			reason := fmt.Sprintf("source PodSpec NodeAffinity requires kubernetes.io/hostname=%s; overlay's NotIn[%s] would make scheduling impossible", req.FromNode, req.FromNode)
			recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationNodeAffinityConflict,
				"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
			d, ferr := failMigration(ctx, deps, input, ledger, req, requestUUID, reason)
			return d, true, ferr
		}

		surgeIdx = workload.AllocateSurgeIndex(input.ObservedState.InstanceStatuses)
		// Record FIRST — it is the resume anchor. A crash after this
		// write resumes with the allocated index and re-ensures the pair
		// stamps above; the reverse order would strand a stamped source
		// behind the fresh-request guards forever.
		allocated := surgeIdx
		if err := input.MutateMigration(ctx, requestUUID, func(m *workload.MigrationRecord) bool {
			if m.SurgeInstance != nil && *m.SurgeInstance == allocated &&
				workload.MigrationPhaseAtOrPast(m.Phase, workload.MigrationPhaseSurgePending) {
				return false
			}
			m.SurgeInstance = &allocated
			// Execution starts here — the capacity gate counts from
			// AllocatedAt, stamped in the same write as the index.
			if m.AllocatedAt == nil {
				now := metav1.NewTime(deps.Now())
				m.AllocatedAt = &now
			}
			if !workload.MigrationPhaseAtOrPast(m.Phase, workload.MigrationPhaseSurgePending) {
				m.Phase = workload.MigrationPhaseSurgePending
			}
			m.Message = "surge allocated; waiting for surge pods"
			return true
		}); err != nil {
			return false, false, fmt.Errorf("Migrate: record surge allocation (uuid=%s): %w", requestUUID, err)
		}
		accepted = true
		if input.ApplyInstanceMutationsWithRetryBlock != nil {
			confirmed, cerr := establishMigrationPair(
				ctx, input, source, nil, requestUUID, surgeIdx, plan.InstanceReadyTimeout,
			)
			if cerr != nil {
				return false, accepted, fmt.Errorf("Migrate: stamp migration pair: %w", cerr)
			}
			if !confirmed {
				return false, accepted, nil
			}
			pairConfirmedForEffects = true
		} else {
			if err := patchInstanceStatusMigrating(ctx, input, sourceIdx, surgeIdx, requestUUID, plan.InstanceReadyTimeout); err != nil {
				return false, false, fmt.Errorf("Migrate: stamp source status (instance=%d): %w", sourceIdx, err)
			}
			if err := patchInstanceStatusMigrationSurge(ctx, input, surgeIdx, sourceIdx, requestUUID, plan.InstanceReadyTimeout); err != nil {
				return false, false, fmt.Errorf("Migrate: stamp surge status (instance=%d): %w", surgeIdx, err)
			}
		}
		// Audit: replace the accept-time Started row (surge index -1
		// sentinel) with the real surge index — same UUID upserts in place.
		ledger.UpsertEntry(audit.NewStartedEntry(req, requestUUID, surgeIdx))
		if err := audit.PersistLedgerForOwner(ctx, deps.Client, ledgerOwnerObject(input), ledgerOwnerGVK(input), ledger); err != nil {
			return false, false, fmt.Errorf("Migrate: persist Started ledger: %w", err)
		}
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonMigrationRequestAccepted,
			"OMENative %s migration accepted (uuid=%s, source-node=%s, surge-index=%d)",
			instanceKey(input.Key.Component, sourceIdx), requestUUID, req.FromNode, surgeIdx)
		// Record + stamps + ledger persisted — Migrate has taken ownership.
		freshStamp = true
	}

	// The plan releases the source index once the surge is promoted
	// Ready, so a completion-tail pass may no longer find the source's
	// plan entry. The surge entry carries the identical Runner layout
	// (the surge mirrors the source by construction), so either entry
	// proves the gang shape.
	gang = gang || isMultiPodInstance(plan, surgeIdx)

	// A gang surge stamped its surge status THIS pass, but the IR
	// reconciler's EnsurePodGroups already ran with the pre-stamp plan (no
	// surge index yet) — so the surge PodGroup doesn't exist. Requeue so
	// the next pass pins the surge index, EnsurePodGroups creates its
	// PodGroup, and only then are the surge gang's pods rendered (the
	// coscheduler needs the PodGroup before its pods). Single-pod surges
	// and gangs without gang scheduling skip this and proceed in one pass.
	if freshStamp && gang && input.DesiredSpec.GangSchedulingAvailable {
		return false, accepted, nil
	}

	// From this point onward Migrate has taken ownership of the request
	// (accepted=true above via the switch). All remaining returns
	// propagate accepted=true so the dispatcher knows to requeue rather
	// than fall through.
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return false, accepted, fmt.Errorf("Migrate: list source pods: %w", err)
	}
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, surgeIdx)
	if err != nil {
		return false, accepted, fmt.Errorf("Migrate: list surge pods: %w", err)
	}
	if migrationCompletionTailRecoverable(entry, sourcePods, surge) {
		// The source status is the normal ownership token for terminal
		// cleanup. The promoted surge may become the steady plan member while
		// source resource finalization still retains that token, so completion
		// resumes from the persisted pair instead of rendering pods from the
		// post-promotion plan.
		if input.ApplyInstanceMutationsWithRetryBlock == nil {
			return false, accepted, nil
		}
		confirmed := false
		var cerr error
		if source == nil {
			confirmed, cerr = confirmMigrationCompletionPair(ctx, input, sourceIdx, surge)
		} else {
			confirmed, cerr = confirmMigrationDrainPair(
				ctx, input, source, surge, requestUUID, surgeIdx, source.RunningRevision, true,
			)
		}
		if cerr != nil {
			return false, accepted, fmt.Errorf("Migrate: confirm completion pair: %w", cerr)
		}
		if !confirmed {
			return false, accepted, nil
		}
		finalized, ferr := finalizeAndRemoveInstance(ctx, deps, input, sourceIdx, source)
		if ferr != nil {
			return false, accepted, fmt.Errorf("Migrate: finalize source Instance in completion tail: %w", ferr)
		}
		if !finalized {
			return false, accepted, nil
		}
		confirmed, cerr = confirmMigrationCompletionPair(ctx, input, sourceIdx, surge)
		if cerr != nil {
			return false, accepted, fmt.Errorf("Migrate: recheck completion pair: %w", cerr)
		}
		if !confirmed {
			return false, accepted, nil
		}
		if err := completeMigrationTail(ctx, deps, input, ledger, req, requestUUID, sourceIdx, surgeIdx, surge.RunningRevision); err != nil {
			return false, accepted, err
		}
		return true, accepted, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil && !pairConfirmedForEffects {
		targetRevision := ""
		if source != nil {
			targetRevision = source.RunningRevision
		}
		confirmed, cerr := confirmMigrationDrainPair(
			ctx, input, source, surge, requestUUID, surgeIdx, targetRevision, len(sourcePods) == 0,
		)
		if cerr != nil {
			return false, accepted, fmt.Errorf("Migrate: confirm migration pair before effects: %w", cerr)
		}
		if !confirmed {
			return false, accepted, nil
		}
	}

	// Migration moves placement, not template — surge runs at the source's
	// RunningRevision (leader + worker templates for a gang).
	surgeRev, surgeRevSpec, surgeWorkerSpec, err := surgeRevisionAndSpec(ctx, deps, input, sourceIdx)
	if err != nil {
		return false, accepted, fmt.Errorf("Migrate: resolve surge revision: %w", err)
	}
	if surgeRev == nil || surgeRevSpec == nil {
		// Source not yet promoted with a RunningRevision — retry.
		return false, accepted, nil
	}

	// The surge mirrors the source's Runner layout: a single "default"
	// pod, or leader + workers for a gang.
	surgeRunners := []workload.RunnerPlan{{Name: "default", Size: 1}}
	var sourceExcludedNodes []string
	if gang {
		mirrored := false
		for i := range plan.Instances {
			if plan.Instances[i].Index == sourceIdx {
				surgeRunners = append([]workload.RunnerPlan(nil), plan.Instances[i].Runners...)
				sourceExcludedNodes = plan.Instances[i].ExcludedNodes
				mirrored = true
				break
			}
		}
		if !mirrored {
			// The source's plan entry is gone (released after surge
			// promotion); mirror the layout from the surge's own entry so
			// the tail keeps computing the gang-shaped desired pod set
			// instead of collapsing to the single-pod fallback above.
			for i := range plan.Instances {
				if plan.Instances[i].Index == surgeIdx {
					surgeRunners = append([]workload.RunnerPlan(nil), plan.Instances[i].Runners...)
					break
				}
			}
		}
	} else {
		// Single-pod: ExcludedNodes from the source instance plan.
		for i := range plan.Instances {
			if plan.Instances[i].Index == sourceIdx {
				sourceExcludedNodes = plan.Instances[i].ExcludedNodes
				break
			}
		}
	}

	// Migration overlay tells Render to stamp anti-affinity vs FromNode.
	surgeInst := workload.InstancePlan{
		Index:         surgeIdx,
		Incarnation:   1,
		Runners:       surgeRunners,
		ExcludedNodes: sourceExcludedNodes, // Exclusion memory follows the instance through surge replacement.
		MigrationOverlay: &workload.MigrationOverlay{
			FromNode:        req.FromNode,
			HintTargetNodes: req.HintTargetNodes,
		},
	}

	// Refuse a surge whose hard NodeAffinity would collapse to
	// "hostname=FromNode AND hostname!=FromNode". Check the leader and
	// (for a gang) the worker template. Without this gate the surge sits
	// Pending until timeout — silent rejection vs. early fail.
	if WouldOverlayConflictWithNodeAffinity(surgeRevSpec, surgeInst.MigrationOverlay) ||
		(surgeWorkerSpec != nil && WouldOverlayConflictWithNodeAffinity(surgeWorkerSpec, surgeInst.MigrationOverlay)) {
		reason := fmt.Sprintf("source PodSpec NodeAffinity requires kubernetes.io/hostname=%s; overlay's NotIn[%s] would make scheduling impossible", req.FromNode, req.FromNode)
		recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationNodeAffinityConflict,
			"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
		d, ferr := failMigration(ctx, deps, input, ledger, req, requestUUID, reason)
		return d, accepted, ferr
	}

	// A gang surge needs its PodGroup before its pods so the coscheduler
	// gangs them. The surge index is pinned in the plan (its Migrate-op
	// status lands it in migrationInFlightIndices), so the IR reconciler's
	// EnsurePodGroups — which runs BEFORE this dispatch — creates the
	// PodGroup. The fresh-stamp pass requeues (see freshStamp below) so
	// that ordering holds: stamp surge status → next pass EnsurePodGroups
	// makes the PodGroup → then the surge gang's pods are created here.

	desired := expectedPodNamesForInstance(input, plan, surgeInst)
	existingByName := query.IndexPodsByName(surgePods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, surgeIdx) {
			return false, accepted, nil
		}
		// Render against the surge revision's leader + worker PodSpecs (not
		// input.DesiredSpec) so renderHook + per-Component metadata stay
		// intact. Expectations bucket on surgeIdx (not sourceIdx) so surge
		// creates track separately from source-side deletes.
		surgeInput := input
		surgeInput.DesiredSpec.PodSpec = surgeRevSpec
		surgeInput.DesiredSpec.WorkerPodSpec = surgeWorkerSpec
		if _, cerr := createMissingPods(ctx, deps, surgeInput, plan, surgeInst, surgeIdx, missing, query.RevisionOf(surgeRev)); cerr != nil {
			rejection, classified := asPodRejection(cerr)
			// A permanently rejected surge can never be created, so the
			// request is closed now rather than idling to its deadline —
			// the same early-fail the node-affinity conflict gate applies.
			if classified && rejection.disposed {
				reason := fmt.Sprintf("surge pod %s rejected by the apiserver (%s): %s",
					rejection.podName, rejection.rejection.Reason, rejection.rejection.Message)
				d, ferr := failMigration(ctx, deps, input, ledger, req, requestUUID, reason)
				return d, accepted, ferr
			}
			// A classified-but-transient rejection (out of quota, throttled)
			// is handled as the blocked-create wait below.
			blockedPod := ""
			var createErr *podCreateError
			switch {
			case classified:
				blockedPod = rejection.podName
			case errors.As(cerr, &createErr):
				blockedPod = createErr.podName
			}
			if blockedPod != "" &&
				!entry.Deadline.IsZero() &&
				!errors.Is(cerr, context.Canceled) &&
				!errors.Is(cerr, context.DeadlineExceeded) {
				recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationSurgeCreateBlocked,
					"OMENative migration uuid=%s waiting to create surge pod %s before its deadline",
					requestUUID, blockedPod)
				logf.FromContext(ctx).V(1).Info("migration surge pod creation blocked",
					"uuid", requestUUID, "pod", blockedPod, "error", cerr.Error())
				return false, accepted, nil
			}
			return false, accepted, fmt.Errorf("Migrate: create surge pod set: %w", cerr)
		}
		return false, accepted, nil
	}

	if !query.AllPodsRuntimeReady(surgePods) {
		return false, accepted, nil
	}
	for _, pod := range surgePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, accepted, fmt.Errorf("Migrate: serving=True on surge pod %s: %w", pod.Name, err)
		}
	}

	// Wait for the surge's rotation + availability gates (shared with
	// the expiry pass's Draining carve-out — see surgeTailGatesPassed).
	tailOK, terr := surgeTailGatesPassed(ctx, deps, input, plan, surgeIdx, surgePods)
	if terr != nil {
		return false, accepted, fmt.Errorf("Migrate: %w", terr)
	}
	if !tailOK {
		return false, accepted, nil
	}
	pairConfirmed, perr := confirmMigrationDrainPair(
		ctx, input, source, surge, requestUUID, surgeIdx, surgeRev.Name, len(sourcePods) == 0,
	)
	if perr != nil {
		return false, accepted, fmt.Errorf("Migrate: confirm migration pair before drain: %w", perr)
	}
	if !pairConfirmed {
		return false, accepted, nil
	}

	// Every surge gate passed (exists + runtime-ready + in-rotation +
	// available) — the record advances to SurgeReady.
	if err := advanceMigrationPhase(ctx, input, requestUUID, workload.MigrationPhaseSurgeReady, "surge in rotation and available"); err != nil {
		return false, accepted, fmt.Errorf("Migrate: record Phase=SurgeReady (uuid=%s): %w", requestUUID, err)
	}

	for _, pod := range sourcePods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		// Migrate-source-drain key; cleared by pod deletion at the end.
		if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterMigrateSourceDrain, requestUUID); err != nil {
			return false, accepted, fmt.Errorf("Migrate: serving=False on source pod %s: %w", pod.Name, err)
		}
	}
	// Source drain has begun (serving=False requested on every source
	// pod) — the record advances to Draining.
	if err := advanceMigrationPhase(ctx, input, requestUUID, workload.MigrationPhaseDraining, "source draining"); err != nil {
		return false, accepted, fmt.Errorf("Migrate: record Phase=Draining (uuid=%s): %w", requestUUID, err)
	}
	for _, pod := range sourcePods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		drained, derr := drain.IsPodDrained(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
		if derr != nil {
			return false, accepted, fmt.Errorf("Migrate: check drain on source pod %s: %w", pod.Name, derr)
		}
		if !drained {
			return false, accepted, nil
		}
	}

	// Live read before deletes — a stale-empty cache view would skip the
	// delete loop and orphan source pods at source-status removal.
	sourcePods, err = query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return false, accepted, fmt.Errorf("Migrate: live-list source pods: %w", err)
	}

	if len(sourcePods) > 0 {
		// Stuck-Terminating escalation BEFORE the expectations gate — a
		// source pod wedged Terminating on a dead node (the reason the
		// migration was requested) never emits the watch DELETE that
		// satisfies its expectation, and the delete loop below skips
		// Terminating pods, so without this the migration can never
		// reach a terminal phase. This uses the scale-down escalation's
		// ordering and advisory-error semantics.
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp == nil {
				continue
			}
			if escErr := escalateStuckTerminating(ctx, deps, input, pod, sourceIdx); escErr != nil {
				logf.FromContext(ctx).V(1).Info("stuck-Terminating escalation deferred",
					"pod", pod.Name, "error", escErr.Error())
			}
		}
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, sourceIdx) {
			return false, accepted, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, sourceIdx, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, sourceIdx)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, accepted, fmt.Errorf("Migrate: delete source pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, accepted, nil
	}

	// Terminal order: surge promote, source resource finalization, and guarded
	// source-status removal; then the Completed ledger row (audit); then the
	// record's terminal stamp. The record write is LAST so a crash in this tail
	// leaves it non-terminal and the next pass re-runs the idempotent
	// steps to completion; a record already Completed would never be
	// picked again and cleanup would strand.
	promoted, perr := promoteMigrationSurge(ctx, input, source, surge, requestUUID, surgeRev.Name)
	if perr != nil {
		return false, accepted, fmt.Errorf("Migrate: promote surge to Ready: %w", perr)
	}
	if !promoted {
		return false, accepted, nil
	}
	if source != nil && !migrationSourceOwnsRemoval(source, requestUUID, surgeIdx) {
		return false, accepted, nil
	}
	removed, rerr := finalizeAndRemoveInstance(ctx, deps, input, sourceIdx, source)
	if rerr != nil {
		return false, accepted, fmt.Errorf("Migrate: finalize source Instance: %w", rerr)
	}
	if !removed {
		return false, accepted, nil
	}
	promotedSurge := *surge
	markReadyTransition(&promotedSurge, input.Now())
	promotedSurge.RunningRevision = surgeRev.Name
	promotedSurge.TargetRevision = ""
	promotedSurge.Operation = nil
	confirmed, cerr := confirmMigrationCompletionPair(ctx, input, sourceIdx, &promotedSurge)
	if cerr != nil {
		return false, accepted, fmt.Errorf("Migrate: confirm promoted migration pair: %w", cerr)
	}
	if !confirmed {
		return false, accepted, nil
	}
	if err := completeMigrationTail(ctx, deps, input, ledger, req, requestUUID, sourceIdx, surgeIdx, surgeRev.Name); err != nil {
		return false, accepted, err
	}
	return true, accepted, nil
}

func migrationCompletionTailRecoverable(
	record *workload.MigrationRecord,
	sourcePods []*corev1.Pod,
	surge *workload.InstanceStatus,
) bool {
	return record.Phase == workload.MigrationPhaseDraining &&
		len(sourcePods) == 0 &&
		surge != nil && surge.Phase == workload.InstancePhaseReady &&
		surge.Operation == nil && surge.RunningRevision != ""
}

func completeMigrationTail(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	ledger *audit.Ledger,
	req *audit.MigrationRequest,
	requestUUID string,
	sourceIdx, surgeIdx int32,
	runningRevision string,
) error {
	ledger.UpsertEntry(audit.NewTerminalEntry(*ledger.InFlightEntryOrSeed(requestUUID, req, surgeIdx), audit.PhaseCompleted, "migrated"))
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, ledgerOwnerObject(input), ledgerOwnerGVK(input), ledger); err != nil {
		return fmt.Errorf("Migrate: persist Completed ledger: %w", err)
	}
	completedMsg := fmt.Sprintf("migrated to instance=%d", surgeIdx)
	if err := input.MutateMigration(ctx, requestUUID, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() {
			return false
		}
		m.Phase = workload.MigrationPhaseCompleted
		m.Message = completedMsg
		now := metav1.NewTime(deps.Now())
		m.CompletedAt = &now
		return true
	}); err != nil {
		return fmt.Errorf("Migrate: record Phase=Completed (uuid=%s): %w", requestUUID, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonMigrationCompleted,
		"OMENative migration uuid=%s complete: %s -> instance=%d (revision=%s)",
		requestUUID, instanceKey(input.Key.Component, sourceIdx), surgeIdx, runningRevision)
	return nil
}

func establishMigrationPair(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	surgeIdx int32,
	timeout time.Duration,
) (bool, error) {
	if !migrationPairStampable(source, surge, requestUUID, surgeIdx) {
		return false, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}
	ownerUID := input.OwnerObject.GetUID()
	sourceGuard, sourceState := terminalIdentityGuard(input, source.Index, source)
	surgeGuard, surgeState := terminalIdentityGuard(input, surgeIdx, surge)
	confirmed := false
	batchGuard := func(snapshot workload.InstanceMutationSnapshot) bool {
		if snapshot.OwnerUID != ownerUID {
			confirmed = false
			return false
		}
		currentSource, sourceFound := snapshot.Instances[source.Index]
		currentSurge, surgeFound := snapshot.Instances[surgeIdx]
		currentSourceOwned := sourceFound && migrationSourceOwnsRemoval(&currentSource, requestUUID, surgeIdx)
		currentSurgeOwned := surgeFound && migrationSurgeOwnsPromotion(&currentSurge, requestUUID, source.Index)
		if (currentSourceOwned && currentSurgeOwned) ||
			(surge == nil && currentSourceOwned && !surgeFound) {
			confirmed = true
			return true
		}
		confirmed = sourceGuard(snapshot) && sourceState.matched && surgeGuard(snapshot)
		if surge == nil {
			confirmed = confirmed && surgeState.absent
		} else {
			confirmed = confirmed && surgeState.matched
		}
		return confirmed
	}

	sourceMutation := migrationStatusMutation(input, source.Index, surgeIdx, migrationRoleSource, requestUUID, timeout)
	sourceMutation.BatchPrecondition = batchGuard
	sourceMutation.Postcondition = func(status *workload.InstanceStatus) bool {
		return migrationSourceOwnsRemoval(status, requestUUID, surgeIdx)
	}
	surgeMutation := migrationStatusMutation(input, surgeIdx, source.Index, migrationRoleSurge, requestUUID, timeout)
	surgeMutation.Postcondition = func(status *workload.InstanceStatus) bool {
		return migrationSurgeOwnsPromotion(status, requestUUID, source.Index)
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{sourceMutation, surgeMutation})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	return confirmed, err
}

func migrationPairStampable(
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	surgeIdx int32,
) bool {
	if source == nil {
		return false
	}
	sourceOwned := migrationSourceOwnsRemoval(source, requestUUID, surgeIdx)
	surgeOwned := migrationSurgeOwnsPromotion(surge, requestUUID, source.Index)
	if sourceOwned {
		return surge == nil || surgeOwned
	}
	return source.Phase == workload.InstancePhaseReady && source.Operation == nil &&
		source.RunningRevision != "" && surge == nil
}

func confirmMigrationCompletionPair(
	ctx context.Context,
	input workload.ReconcileInput,
	sourceIdx int32,
	surge *workload.InstanceStatus,
) (bool, error) {
	if surge == nil || surge.RunningRevision == "" ||
		!migrationPromotedSurgeMatches(surge, surge.RunningRevision) {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return true, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := terminalIdentityGuard(input, sourceIdx, nil)
	surgeGuard, surgeState := terminalIdentityGuard(input, surge.Index, surge)
	confirmed := false
	preflight := workload.InstanceMutation{
		Index:  sourceIdx,
		Mutate: func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			confirmed = sourceGuard(snapshot) && sourceState.absent &&
				surgeGuard(snapshot) && surgeState.matched
			if !confirmed {
				return false
			}
			currentSurge := snapshot.Instances[surge.Index]
			confirmed = migrationPromotedSurgeMatches(&currentSurge, surge.RunningRevision)
			return confirmed
		},
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{preflight})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	return confirmed, err
}

func confirmMigrationDrainPair(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	surgeIdx int32,
	targetRevision string,
	allowPromoted bool,
) (bool, error) {
	if !migrationSourceOwnsRemoval(source, requestUUID, surgeIdx) {
		return false, nil
	}
	targetMatches := func(status *workload.InstanceStatus) bool {
		return migrationSurgeOwnsPromotion(status, requestUUID, source.Index)
	}
	if !targetMatches(surge) {
		if !allowPromoted || !migrationPromotedSurgeMatches(surge, targetRevision) {
			return false, nil
		}
		targetMatches = func(status *workload.InstanceStatus) bool {
			return migrationPromotedSurgeMatches(status, targetRevision)
		}
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return true, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := terminalIdentityGuard(input, source.Index, source)
	surgeGuard, surgeState := terminalIdentityGuard(input, surge.Index, surge)
	confirmed := false
	preflight := workload.InstanceMutation{
		Index:  source.Index,
		Mutate: func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			if !sourceGuard(snapshot) || !sourceState.matched ||
				!surgeGuard(snapshot) || !surgeState.matched {
				confirmed = false
				return false
			}
			currentSurge := snapshot.Instances[surge.Index]
			confirmed = targetMatches(&currentSurge)
			return confirmed
		},
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{preflight})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	return confirmed, err
}

func promoteMigrationSurge(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	targetRevision string,
) (bool, error) {
	if source == nil || surge == nil || !migrationSourceOwnsRemoval(source, requestUUID, surge.Index) {
		return false, nil
	}
	active := migrationSurgeOwnsPromotion(surge, requestUUID, source.Index)
	alreadyPromoted := migrationPromotedSurgeMatches(surge, targetRevision)
	if !active && !alreadyPromoted {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		if err := patchInstanceStatusReadyOnRevision(ctx, input, surge.Index, targetRevision); err != nil {
			if errors.Is(err, workload.ErrStatusOwnerGone) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if alreadyPromoted {
		confirmed, err := confirmMigrationDrainPair(
			ctx, input, source, surge, requestUUID, surge.Index, targetRevision, true,
		)
		if err != nil || !confirmed {
			return false, err
		}
		if err := pruneRetryBlockOnPromote(ctx, input, targetRevision); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := terminalIdentityGuard(input, source.Index, source)
	surgeGuard, surgeState := terminalIdentityGuard(input, surge.Index, surge)
	mutation := createStatusReadyOnRevisionMutation(surge.Index, targetRevision, input.Now())
	promoted := false
	mutation.BatchPrecondition = func(snapshot workload.InstanceMutationSnapshot) bool {
		if !sourceGuard(snapshot) || !sourceState.matched ||
			!surgeGuard(snapshot) || !surgeState.matched {
			return false
		}
		currentSource := snapshot.Instances[source.Index]
		currentSurge := snapshot.Instances[surge.Index]
		return migrationSourceOwnsRemoval(&currentSource, requestUUID, surge.Index) &&
			migrationSurgeOwnsPromotion(&currentSurge, requestUUID, source.Index)
	}
	mutation.Postcondition = func(status *workload.InstanceStatus) bool {
		return status != nil && status.Index == surge.Index &&
			status.Incarnation == surge.Incarnation && status.ActiveOrdinal == surge.ActiveOrdinal &&
			migrationPromotedSurgeMatches(status, targetRevision)
	}
	mutation.OnCommit = func(_, _ *workload.InstanceStatus) {
		promoted = true
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{mutation})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil || !promoted {
		return false, err
	}

	if err := pruneRetryBlockOnPromote(ctx, input, targetRevision); err != nil {
		return false, err
	}
	return true, nil
}

func migrationSourceOwnsRemoval(status *workload.InstanceStatus, requestUUID string, surgeIdx int32) bool {
	return status != nil && status.Phase == workload.InstancePhaseMigrating &&
		status.RunningRevision != "" && status.TargetRevision == "" && status.Operation != nil &&
		status.Operation.Type == workload.InstanceOperationMigrate &&
		status.Operation.RequestUUID == requestUUID &&
		status.Operation.SurgeIndex != nil && *status.Operation.SurgeIndex == surgeIdx
}

func migrationSurgeOwnsPromotion(status *workload.InstanceStatus, requestUUID string, sourceIdx int32) bool {
	return status != nil && status.Incarnation == 1 && status.ActiveOrdinal == 0 &&
		status.Phase == workload.InstancePhaseCreating && status.RunningRevision == "" &&
		status.TargetRevision == "" && status.Operation != nil &&
		status.Operation.Type == workload.InstanceOperationMigrate &&
		status.Operation.RequestUUID == requestUUID &&
		status.Operation.SurgeIndex != nil && *status.Operation.SurgeIndex == sourceIdx
}

func migrationPromotedSurgeMatches(status *workload.InstanceStatus, targetRevision string) bool {
	return status != nil && status.Incarnation == 1 && status.ActiveOrdinal == 0 &&
		status.Phase == workload.InstancePhaseReady &&
		status.RunningRevision == targetRevision && status.TargetRevision == "" && status.Operation == nil
}

// surgeTailGatesPassed reports whether the surge Instance passes the
// drive pass's pre-drain completion gates:
//
//   - every routable surge pod (the leader; workers are never routed —
//     drainServiceForPod returns "" for them) is in rotation in its
//     per-revision Service. Rotation reads the live reader so a cold
//     informer doesn't falsely report zero slices, and without it the
//     source swap can hit a zero-routable-endpoint window.
//   - the surge InstanceStatus reports full availability: its
//     AvailablePodCount covers every pod, which counts a pod only once it
//     is in rotation and, under a positive minReadySeconds window, has
//     been Ready for at least that long.
//
// SHARED between the Migrate drive pass and the expiry pass's Draining
// carve-out (migrationTailReady): the carve-out defers expiry exactly
// when the drive can finish the tail, so the two must evaluate the SAME
// gates — a weaker expiry-side copy would defer forever on a surge the
// drive blocks on (e.g., ready-but-never-in-rotation), stranding the
// record non-terminal past its deadline.
func surgeTailGatesPassed(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, surgeIdx int32, surgePods []*corev1.Pod) (bool, error) {
	for _, pod := range surgePods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		inRotation, err := drain.IsPodInRotation(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check rotation on surge pod %s: %w", pod.Name, err)
		}
		if !inRotation {
			return false, nil
		}
	}
	surgeStatus := findInstanceStatus(input.ObservedState.InstanceStatuses, surgeIdx)
	if surgeStatus == nil || surgeStatus.PodCount == 0 ||
		surgeStatus.AvailablePodCount < surgeStatus.PodCount {
		return false, nil
	}
	return true, nil
}

// advanceMigrationPhase moves the migration record for uuid forward to
// phase (forward-only: an already-at-or-past record is untouched, so a
// stale pass can never regress the phase). message replaces the
// record's current-blocker text.
func advanceMigrationPhase(ctx context.Context, input workload.ReconcileInput, uuid string, phase workload.MigrationPhase, message string) error {
	return input.MutateMigration(ctx, uuid, func(m *workload.MigrationRecord) bool {
		if workload.MigrationPhaseAtOrPast(m.Phase, phase) {
			return false
		}
		m.Phase = phase
		m.Message = message
		return true
	})
}

// validateFromNode live-reads source pods, returning three states:
//
//   - rejectionReason="" defer_=false err=nil — valid, proceed
//   - defer_=true — transient (pod not scheduled yet); caller requeues
//   - rejectionReason != "" — permanent mismatch; caller fails
//
// The check is gang-aware. A multi-node gang Instance (leader + workers)
// spans nodes BY DESIGN, so the whole-gang surge only needs req.FromNode
// to host at least one of the source pods — that pod is the one the surge's
// NotIn[FromNode] anti-affinity relocates off. Rejecting a gang merely for
// spanning nodes (the single-pod assumption) wrongly failed every gang
// migration whose pods didn't happen to co-locate. A single-pod Instance is
// the degenerate case: its one pod must sit on FromNode, and "pods on
// multiple nodes" stays a rejection because a single-pod migration can't
// legitimately span nodes. Either way an unscheduled pod defers so the next
// reconcile re-validates once scheduling settles.
func validateFromNode(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx int32, fromNode string) (string, bool, error) {
	listed, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return "", false, fmt.Errorf("list source pods: %w", err)
	}
	if len(listed) == 0 {
		return "source instance has no live pods", false, nil
	}
	// Terminating pods are seconds from gone (recreate churn leaves the
	// old pod Terminating beside its replacement) — validating against
	// them would permanently fail a legitimate request. All-Terminating
	// is transient (replacements pending): defer, don't reject.
	pods := make([]*corev1.Pod, 0, len(listed))
	for _, pod := range listed {
		if pod.DeletionTimestamp != nil {
			continue
		}
		pods = append(pods, pod)
	}
	if len(pods) == 0 {
		return "", true, nil
	}

	if isMultiPodInstance(plan, sourceIdx) {
		// Gang: the surge relocates the whole Instance, but FromNode names a
		// single source node to evacuate. Defer first while any member is
		// still unscheduled (the fresh-request guard — surge sizing and the
		// gang readiness gate need the full layout settled, and an
		// as-yet-unscheduled member could still land on FromNode). Once the
		// gang is fully scheduled, accept iff FromNode hosts a member; reject
		// only when none of them landed there (stale request — the gang has
		// since moved off that node).
		observed := make([]string, 0, len(pods))
		onFromNode := false
		for _, pod := range pods {
			node := pod.Spec.NodeName
			if node == "" {
				// Unscheduled gang member — defer; next reconcile re-validates.
				return "", true, nil
			}
			if node == fromNode {
				onFromNode = true
			}
			observed = append(observed, node)
		}
		if onFromNode {
			return "", false, nil
		}
		return "request.FromNode=" + fromNode + " hosts no source pod; gang spans nodes " + strings.Join(observed, ", "), false, nil
	}

	var observed string
	for _, pod := range pods {
		node := pod.Spec.NodeName
		if node == "" {
			// Unscheduled — defer; next reconcile re-validates.
			return "", true, nil
		}
		if observed == "" {
			observed = node
			continue
		}
		if observed != node {
			return "source pods span multiple nodes (" + observed + ", " + node + ")", false, nil
		}
	}
	if observed != fromNode {
		return "request.FromNode=" + fromNode + " does not match observed source node=" + observed, false, nil
	}
	return "", false, nil
}

// failMigration terminates the request: a terminal Failed audit row
// (history), then the migration record's Phase=Failed + Message +
// CompletedAt (authority — the dispatcher stops picking it and the
// capacity slot frees structurally). Ledger-first: if the record write
// crashes, the still-non-terminal record retries next pass, hits the
// same rejection, and the ledger upsert is idempotent. Returns
// done=true so the reconciler stops. Emits a Warning event so
// operators see the rejection in `kubectl describe`. The trigger
// annotation is not touched here — the adapter consumes it at
// accept/reject time (the executor never touches annotations).
func failMigration(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, ledger *audit.Ledger, req *audit.MigrationRequest, uuid, reason string) (bool, error) {
	now := metav1.NewTime(deps.Now()).UTC().Format(time.RFC3339)
	entry := audit.Entry{
		RequestUUID:     uuid,
		Phase:           audit.PhaseFailed,
		Component:       req.Component,
		FromNode:        req.FromNode,
		HintTargetNodes: append([]string(nil), req.HintTargetNodes...),
		StartedAt:       now,
		CompletedAt:     now,
		Outcome:         reason,
	}
	ledger.UpsertEntry(entry)
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, ledgerOwnerObject(input), ledgerOwnerGVK(input), ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Failed ledger (%s): %w", reason, err)
	}
	if err := input.MutateMigration(ctx, uuid, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() {
			return false
		}
		m.Phase = workload.MigrationPhaseFailed
		m.Message = reason
		completed := metav1.NewTime(deps.Now())
		m.CompletedAt = &completed
		return true
	}); err != nil {
		return false, fmt.Errorf("Migrate: record Phase=Failed (%s): %w", reason, err)
	}
	recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationRequestRejected,
		"OMENative migration uuid=%s rejected: %s", uuid, reason)
	return true, nil
}

// isMultiPodInstance reports whether the Instance at sourceIdx in plan
// is multi-pod (TotalPods > 1). Migrate uses it to select the gang
// surge shape: copy the source's Runner layout, carry the worker
// PodSpec from the RunningRevision, and requeue the fresh-stamp pass
// so EnsurePodGroups creates the surge PodGroup before the gang's
// pods render.
//
// Returns false when no Instance with that index exists in the plan —
// the rest of Migrate handles missing-source via failMigration("source
// InstanceStatus missing").
func isMultiPodInstance(plan workload.ComponentPlan, sourceIdx int32) bool {
	for _, inst := range plan.Instances {
		if inst.Index == sourceIdx {
			return inst.TotalPods() > 1
		}
	}
	return false
}

// surgeRevisionAndSpec returns the source's RunningRevision CR + leader
// (single-pod) PodSpec + worker PodSpec. (nil, nil, nil, nil) means no
// recorded RunningRevision yet OR the CR was deleted — caller retries.
// workerSpec is nil for single-pod Instances; for a gang it carries the
// worker template so the surge gang's worker pods render correctly.
//
// Reads the source's running revision from input.ObservedState; fetches
// the CR via deps.Reader() (live read) so a stale cache doesn't return
// a deleted CR as still-present.
func surgeRevisionAndSpec(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, sourceIdx int32) (*appsv1.ControllerRevision, *corev1.PodSpec, *corev1.PodSpec, error) {
	source := findInstanceStatus(input.ObservedState.InstanceStatuses, sourceIdx)
	if source == nil || source.RunningRevision == "" {
		return nil, nil, nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	if err := deps.Reader().Get(ctx, client.ObjectKey{Namespace: input.Key.Namespace, Name: source.RunningRevision}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("get source CR %s: %w", source.RunningRevision, err)
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, nil, nil, fmt.Errorf("unmarshal source CR data: %w", err)
	}
	if payload.PodSpec == nil {
		return nil, nil, nil, fmt.Errorf("source CR %s missing podSpec payload", source.RunningRevision)
	}
	return cr, payload.PodSpec, payload.WorkerPodSpec, nil
}

// migrationRole picks which side of the pair stampMigrationStatus is
// writing — source goes to Phase=Migrating with sibling=surge; surge
// goes to Phase=Creating with sibling=source and Incarnation seeded.
type migrationRole int

const (
	migrationRoleSource migrationRole = iota
	migrationRoleSurge
)

// stampMigrationStatus writes one side of the migration pair. Idempotent
// on (Phase, Operation.Type, RequestUUID). The Operation is a pin —
// Type + RequestUUID (+ SurgeIndex for pair correlation) plus the
// timing fields the deadline machinery reads; migration facts
// (FromNode, hints, reason) live on the owner's status.migrations
// record, not here.
func stampMigrationStatus(ctx context.Context, input workload.ReconcileInput, idx, siblingIdx int32, role migrationRole, uuid string, timeout time.Duration) error {
	mutation := migrationStatusMutation(input, idx, siblingIdx, role, uuid, timeout)
	return input.MutateInstance(ctx, mutation.Index, mutation.Mutate)
}

func migrationStatusMutation(input workload.ReconcileInput, idx, siblingIdx int32, role migrationRole, uuid string, timeout time.Duration) workload.InstanceMutation {
	now := metav1.NewTime(input.Now())
	siblingPtr := siblingIdx
	wantPhase := workload.InstancePhaseMigrating
	idSuffix := ""
	if role == migrationRoleSurge {
		wantPhase = workload.InstancePhaseCreating
		idSuffix = "-surge"
	}
	return workload.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
		if role == migrationRoleSource && s.Phase == "" {
			// Fresh-empty slot from the append path: the source status
			// was removed out from under us — don't resurrect it. The
			// surge role legitimately seeds a fresh slot.
			return false
		}
		if s.Phase == wantPhase &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationMigrate &&
			s.Operation.RequestUUID == uuid {
			return false
		}
		if role == migrationRoleSurge && s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Phase = wantPhase
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("migrate-%s%s-%d", uuid, idSuffix, now.Unix()),
			Type:           workload.InstanceOperationMigrate,
			Step:           "CreateSurge",
			RequestUUID:    uuid,
			SurgeIndex:     &siblingPtr,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	}}
}

// patchInstanceStatusMigrating stamps source Phase=Migrating; SurgeIndex
// on Operation points forward at the surge.
func patchInstanceStatusMigrating(ctx context.Context, input workload.ReconcileInput, idx, surgeIdx int32, uuid string, timeout time.Duration) error {
	return stampMigrationStatus(ctx, input, idx, surgeIdx, migrationRoleSource, uuid, timeout)
}

// patchInstanceStatusMigrationSurge stamps surge Phase=Creating; SurgeIndex
// points back at source (field reused as a sibling pointer so observers
// can correlate the pair from either side).
func patchInstanceStatusMigrationSurge(ctx context.Context, input workload.ReconcileInput, surgeIdx, sourceIdx int32, uuid string, timeout time.Duration) error {
	return stampMigrationStatus(ctx, input, surgeIdx, sourceIdx, migrationRoleSurge, uuid, timeout)
}

// drainServiceForPod returns the per-revision *routed* Service name to
// gate drain against for pod. Read from the pod's ome.io/revision-hash
// label (stamped by Render). Empty string when the label is missing —
// caller treats that as "no routed Service to check" and proceeds
// straight to delete.
//
// We deliberately do NOT use the Component's headless Service here.
// That Service sets PublishNotReadyAddresses=true so peer-discovery DNS
// resolves before pods are Ready; kube-proxy then publishes the
// endpoint with Conditions.Ready=true regardless of the pod's actual
// Ready state, which would make drain.IsPodDrained wait forever. The
// per-revision routed Service (created by the coordination layer with
// PublishNotReadyAddresses=false) reflects the controller-owned
// ome.io/serving gate correctly.
//
// plan.Component type flows from the caller so migrate.go can address
// per-revision Services without importing v1beta1 itself.
func drainServiceForPod(input workload.ReconcileInput, plan workload.ComponentPlan, pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" {
		return ""
	}
	// Workers are never members of the per-revision ROUTING Service: for
	// multi-pod (gang) Components that Service pins runner=leader,pod-ordinal=0
	// (coordination.BuildPerRevisionRoutingService) because workers run
	// distributed-init peers and never serve customer traffic. Returning a
	// routing-Service name for a worker would wedge the Migrate surge
	// in-rotation gate forever — IsPodInRotation(worker) can never become true
	// since the worker is not an endpoint — and is a no-op for the source
	// drain gate (a worker is never routed, so it's trivially drained). Skip it
	// so the gates assert only the routable leader; worker readiness is already
	// covered upstream by AllPodsRuntimeReady, and worker availability by the
	// headless-Service-based AvailablePodCount.
	if pod.Labels[query.LabelRunner] == "worker" {
		return ""
	}
	return query.PerRevisionServiceName(input.Key.OwnerName, plan.Component, hash)
}
