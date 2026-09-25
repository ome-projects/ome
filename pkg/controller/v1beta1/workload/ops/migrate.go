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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// MigrateRequeueInterval is the wait between Migrate passes while a
// surge migration is in flight, from the operator's
// lifecycle.requeue.operation. Exported so the dispatcher's pacing
// stays in lockstep. Zero means unconfigured: the caller requeues on
// the controller's rate-limited backoff instead.
func MigrateRequeueInterval(input workload.ReconcileInput) time.Duration {
	return input.Requeue.Operation
}

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
	source := input.ObservedState.Instance(sourceIdx)
	var surge *workload.InstanceStatus
	var surgeIdx int32
	pairConfirmedForEffects := false
	// freshStamp records that THIS pass allocated + stamped the surge
	// (the default case below). A gang surge requeues right after, so the
	// next pass's EnsurePodGroups creates the surge PodGroup before any
	// surge pod is rendered.
	freshStamp := false
	if entry.SurgeAllocated() {
		// In-flight — accepted on a prior pass; the record is the anchor.
		surgeIdx = *entry.SurgeInstance
		surge = input.ObservedState.Instance(surgeIdx)
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
		if !migrationSourceSteady(source) {
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
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationFromNodeMismatch,
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
		// An unconfigured policy is not a cap breach: there is no bound to
		// judge the request against, so the pass holds it WITHOUT taking
		// ownership. The record stays Accepted and a later pass admits it
		// once the operator supplies the caps.
		if input.MigrationAudit == nil {
			if err := holdMigrationForUnconfiguredCapacity(ctx, deps, input, entry, requestUUID); err != nil {
				return false, false, fmt.Errorf("Migrate: hold for unconfigured capacity (uuid=%s): %w", requestUUID, err)
			}
			// The operator writing the missing key raises no event the
			// controller watches, so the pass owes itself a wake-up: the
			// record would otherwise sit until unrelated work or the
			// resync happens by.
			input.PassWake.Observe(MigrateRequeueInterval(input))
			return false, false, nil
		}
		if err := writeMigrationCapacityCondition(ctx, input, false); err != nil {
			return false, false, fmt.Errorf("Migrate: clear capacity condition (uuid=%s): %w", requestUUID, err)
		}
		if ok, reason := audit.ValidateCapacity(input.MigrationAudit, input.ObservedState.Migrations, requestUUID, deps.Now()); !ok {
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonRateLimited,
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
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationNodeAffinityConflict,
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
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationRequestAccepted,
			"OMENative %s migration accepted (uuid=%s, source-node=%s, surge-index=%d)",
			workload.InstanceKey(input.Key.Component, sourceIdx), requestUUID, req.FromNode, surgeIdx)
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
		finalized, ferr := status.FinalizeAndRemove(ctx, deps, input, sourceIdx, source)
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
	// A surge wedged in a terminal kubelet waiting reason can never take
	// over, so the move ends on that evidence rather than idling to the
	// record's Deadline. It ends THROUGH THE RECORD — the same close an
	// expiry drives — so the source is restored from observation instead
	// of being stamped from here, and a record already terminal is not
	// reopened (the terminal check at the top of this pass).
	//
	// Ahead of the pair confirmation on purpose: the close needs only the
	// record and the surge pods, and its own writes are identity-guarded,
	// so a crash part-way through the unwind — which leaves the pair
	// unconfirmable — is re-derived on the next pass instead of waiting
	// out the Deadline. Live source pods are required because the restore
	// puts the source back in rotation: a source already drained and
	// deleted has nothing to restore, so that shape stays with the
	// Deadline backstop.
	if entry.SurgeAllocated() && len(sourcePods) > 0 {
		if blocker, wedged := surgeWedgeBlocker(input, surgePods); wedged {
			if err := failMigrationOnWedgedSurge(ctx, deps, input, plan, entry, surgeIdx, blocker); err != nil {
				return false, accepted, fmt.Errorf("Migrate: fail on wedged surge (uuid=%s): %w", requestUUID, err)
			}
			return true, accepted, nil
		}
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
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationNodeAffinityConflict,
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
	// A surge pod the kubelet refused to admit never ran and never will,
	// yet it still holds its name, so the migration can never hand over.
	// Free it so the surge is placed again before the record's deadline.
	// The bookkeeping belongs to the SOURCE, which owns the Migrate
	// operation; the pods and their expectations bucket on the surge.
	if recycling, rerr := recycleAdmissionRejectedTargets(ctx, deps, input, sourceIdx, surgeIdx,
		workload.InstanceOperationMigrate, surgePods, desired); rerr != nil {
		return false, accepted, fmt.Errorf("recycle rejected migration surge pod (instance=%d): %w", surgeIdx, rerr)
	} else if recycling {
		return false, accepted, nil
	}
	// A surge pod whose kubelet has stopped reporting is not dead
	// evidence: its name is freed only by the force-delete sweep, on
	// proven node death, and the source keeps serving meanwhile. The
	// record's Deadline stays the bound while the name is held; the
	// sweep reads and events under the surge index, where the pods live.
	if holding, _, herr := recoverUnknownPhaseTargets(ctx, deps, input, surgeIdx, surgePods, desired); herr != nil {
		return false, accepted, fmt.Errorf("recover unknown-phase migration surge pod (instance=%d): %w", surgeIdx, herr)
	} else if holding {
		return false, accepted, nil
	}
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
				workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationSurgeCreateBlocked,
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
	if source != nil && !status.MigrationSourceOwnsRemoval(source, requestUUID, surgeIdx) {
		return false, accepted, nil
	}
	removed, rerr := status.FinalizeAndRemove(ctx, deps, input, sourceIdx, source)
	if rerr != nil {
		return false, accepted, fmt.Errorf("Migrate: finalize source Instance: %w", rerr)
	}
	if !removed {
		return false, accepted, nil
	}
	promotedSurge := *surge
	status.EnterReady(&promotedSurge, input.Now())
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
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationCompleted,
		"OMENative migration uuid=%s complete: %s -> instance=%d (revision=%s)",
		requestUUID, workload.InstanceKey(input.Key.Component, sourceIdx), surgeIdx, runningRevision)
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
	return status.StartMigration(ctx, input, source, surge, requestUUID, surgeIdx, timeout)
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
	sourceOwned := status.MigrationSourceOwnsRemoval(source, requestUUID, surgeIdx)
	surgeOwned := status.MigrationSurgeOwnsPromotion(surge, requestUUID, source.Index)
	if sourceOwned {
		return surge == nil || surgeOwned
	}
	return migrationSourceSteady(source) && surge == nil
}

// migrationSourceSteady reports whether a source row is one a fresh
// record may claim: Ready on a recorded revision with no operation at
// all. An operation of any kind, recognized or not, is a claim on the
// row, and the revision is the migration's own requirement, since the
// surge is created at whatever the source is running.
func migrationSourceSteady(source *workload.InstanceStatus) bool {
	return source != nil && source.Phase == workload.InstancePhaseReady &&
		source.Operation == nil && source.RunningRevision != ""
}

func confirmMigrationCompletionPair(
	ctx context.Context,
	input workload.ReconcileInput,
	sourceIdx int32,
	surge *workload.InstanceStatus,
) (bool, error) {
	if surge == nil || surge.RunningRevision == "" ||
		!status.MigrationPromotedSurgeMatches(surge, surge.RunningRevision) {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return true, nil
	}
	if err := status.RequireOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := status.Guard(input, sourceIdx, nil)
	surgeGuard, surgeState := status.Guard(input, surge.Index, surge)
	confirmed := false
	preflight := workload.InstanceMutation{
		Index:  sourceIdx,
		Mutate: func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			confirmed = sourceGuard(snapshot) && sourceState.Absent &&
				surgeGuard(snapshot) && surgeState.Matched
			if !confirmed {
				return false
			}
			currentSurge := snapshot.Instances[surge.Index]
			confirmed = status.MigrationPromotedSurgeMatches(&currentSurge, surge.RunningRevision)
			return confirmed
		},
	}
	err := status.Apply(ctx, input, []workload.InstanceMutation{preflight})
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
	if !status.MigrationSourceOwnsRemoval(source, requestUUID, surgeIdx) {
		return false, nil
	}
	promoted := false
	if !status.MigrationSurgeOwnsPromotion(surge, requestUUID, source.Index) {
		if !allowPromoted || !status.MigrationPromotedSurgeMatches(surge, targetRevision) {
			return false, nil
		}
		promoted = true
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return true, nil
	}
	if err := status.RequireOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := status.Guard(input, source.Index, source)
	surgeGuard, surgeState := status.Guard(input, surge.Index, surge)
	confirmed := false
	preflight := workload.InstanceMutation{
		Index:  source.Index,
		Mutate: func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			if !sourceGuard(snapshot) || !sourceState.Matched ||
				!surgeGuard(snapshot) || !surgeState.Matched {
				confirmed = false
				return false
			}
			currentSurge := snapshot.Instances[surge.Index]
			confirmed = migrationDrainTargetMatches(&currentSurge, promoted, requestUUID, source.Index, targetRevision)
			return confirmed
		},
	}
	err := status.Apply(ctx, input, []workload.InstanceMutation{preflight})
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
	if source == nil || surge == nil || !status.MigrationSourceOwnsRemoval(source, requestUUID, surge.Index) {
		return false, nil
	}
	active := status.MigrationSurgeOwnsPromotion(surge, requestUUID, source.Index)
	alreadyPromoted := status.MigrationPromotedSurgeMatches(surge, targetRevision)
	if !active && !alreadyPromoted {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		if err := status.StampReadyOnRevision(ctx, input, surge.Index, targetRevision); err != nil {
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
		if err := status.RetryBlockPruneOnPromote(ctx, input, targetRevision); err != nil {
			return false, err
		}
		return true, nil
	}
	return status.StampMigrationSurgeReady(ctx, input, source, surge, requestUUID, targetRevision)
}

// migrationDrainTargetMatches reads the surge side of a drain pair as the
// pass classified it: still pinned to the request, or already promoted
// to targetRevision.
func migrationDrainTargetMatches(row *workload.InstanceStatus, promoted bool, requestUUID string, sourceIdx int32, targetRevision string) bool {
	if promoted {
		return status.MigrationPromotedSurgeMatches(row, targetRevision)
	}
	return status.MigrationSurgeOwnsPromotion(row, requestUUID, sourceIdx)
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
	surgeStatus := input.ObservedState.Instance(surgeIdx)
	publishedPods, publishedAvailable := workload.AdapterPublished(surgeStatus)
	if publishedPods == 0 || publishedAvailable < publishedPods {
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

// migrationCapacityUnconfiguredMessage is what a held request carries on
// its record while it waits for the operator's caps. Persisting it makes
// the hold edge-triggered: the Warning fires on the pass that starts the
// hold, not on every pass of it.
const migrationCapacityUnconfiguredMessage = "waiting for migration capacity policy (no lifecycle.audit configured)"

// holdMigrationForUnconfiguredCapacity records that the request is
// waiting for caps the operator has not supplied: the record keeps its
// Accepted phase and carries the reason, a Warning fires once per hold,
// and the Component condition makes the hold visible without events.
func holdMigrationForUnconfiguredCapacity(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, entry *workload.MigrationRecord, uuid string) error {
	if err := writeMigrationCapacityCondition(ctx, input, true); err != nil {
		return err
	}
	if entry != nil && entry.Message == migrationCapacityUnconfiguredMessage {
		return nil
	}
	// The closure's own answer is the edge: a record that already reads
	// as held under a stale observation must not re-warn.
	opened := false
	if err := input.MutateMigration(ctx, uuid, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() || m.Message == migrationCapacityUnconfiguredMessage {
			return false
		}
		m.Message = migrationCapacityUnconfiguredMessage
		opened = true
		return true
	}); err != nil {
		return fmt.Errorf("record capacity hold: %w", err)
	}
	if !opened {
		return nil
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationPolicyUnconfigured,
		"OMENative migration uuid=%s held: %s", uuid, migrationCapacityUnconfiguredMessage)
	return nil
}

// writeMigrationCapacityCondition stamps the Component-scoped
// MigrationPolicyUnconfigured condition with the pass's capacity
// verdict: True while a request waits for caps nobody configured, False
// once a pass judges one against the operator's caps.
func writeMigrationCapacityCondition(ctx context.Context, input workload.ReconcileInput, unconfigured bool) error {
	var generation int64
	if input.OwnerObject != nil {
		generation = input.OwnerObject.GetGeneration()
	}
	cond := metav1.Condition{
		Type:               string(workload.ConditionMigrationPolicyUnconfigured),
		Status:             metav1.ConditionFalse,
		Reason:             string(workload.ReasonMigrationCapacityConfigured),
		Message:            "Migration capacity policy is configured",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: generation,
	}
	if unconfigured {
		cond.Status = metav1.ConditionTrue
		cond.Reason = string(workload.ReasonMigrationCapacityUnconfigured)
		cond.Message = "lifecycle.audit is not configured; migration requests wait instead of executing"
	}
	if input.WriteAggregateCondition == nil {
		return nil
	}
	return input.WriteAggregateCondition(ctx, cond)
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
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationRequestRejected,
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
	source := input.ObservedState.Instance(sourceIdx)
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

// patchInstanceStatusMigrating stamps source Phase=Migrating; SurgeIndex
// on Operation points forward at the surge.
func patchInstanceStatusMigrating(ctx context.Context, input workload.ReconcileInput, idx, surgeIdx int32, uuid string, timeout time.Duration) error {
	return status.StampMigrationPin(ctx, input, idx, surgeIdx, status.MigrationRoleSource, uuid, timeout)
}

// patchInstanceStatusMigrationSurge stamps surge Phase=Creating; SurgeIndex
// points back at source (field reused as a sibling pointer so observers
// can correlate the pair from either side).
func patchInstanceStatusMigrationSurge(ctx context.Context, input workload.ReconcileInput, surgeIdx, sourceIdx int32, uuid string, timeout time.Duration) error {
	return status.StampMigrationPin(ctx, input, surgeIdx, sourceIdx, status.MigrationRoleSurge, uuid, timeout)
}

// drainServiceForPod is query.RoutedServiceForPod under the name the
// Update and Migrate drain gates read by.
func drainServiceForPod(input workload.ReconcileInput, plan workload.ComponentPlan, pod *corev1.Pod) string {
	return query.RoutedServiceForPod(input.Key.OwnerName, plan.Component, pod)
}

// podsDrainedFromRouting reports whether every pod in pods has left the
// EndpointSlices of the per-revision routed Service it belongs to. Pods with
// no routed Service — workers, or pods missing the revision-hash label — are
// trivially drained (drainServiceForPod returns "" for them). The caller is a
// destructive step: deleting a pod the data plane is still routing to sheds
// the requests in flight to it, so the whole set gates on one answer.
func podsDrainedFromRouting(
	ctx context.Context,
	drainer *drain.Batcher,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	idx int32,
	pods []*corev1.Pod,
) (bool, error) {
	for _, pod := range pods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		drained, err := drainer.IsPodDrained(ctx, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", idx, pod.Name, err)
		}
		if !drained {
			return false, nil
		}
	}
	return true, nil
}

// Terminal kubelet evidence on a migration's surge ends the move before
// the record's Deadline.
//
// A surge pod parked in a waiting reason it cannot get itself out of
// never runs, so the handover it exists for can never happen. Both
// escalation paths skip a Migrate-pinned row on purpose — a serving
// source must not be failed for its replacement's fate — which leaves
// the record as the only consumer of this evidence. The read therefore
// lives in the migration pass, which already holds the surge pods, and
// drives the SAME record close an expiry drives: the record is the
// authority, its Deadline is the backstop.

// migrationSourceUnhealthyReason is the LastFailure.Reason a source
// records when its migration is closed and observation finds the source
// itself not runtime-ready. It names the source's own condition, not
// the close that exposed it — the surge's wedge is what the record and
// the event carry.
const migrationSourceUnhealthyReason = "MigrationClosedSourceUnhealthy"

// surgeWedgeBlocker names the first surge pod parked in a terminal
// kubelet waiting reason past the configured stuck-pod grace, in the
// phrasing the record carries as its outcome.
//
// The reason set and the grace are the ones every other reader of this
// evidence uses, so a wedge one pass acts on is a wedge the others
// recognize. An unconfigured grace disables the read exactly as it
// disables the fast escalation. A pod already Terminating is on its way
// out — the recycle or the scale-down pipeline owns it — so it proves
// nothing about the migration.
func surgeWedgeBlocker(input workload.ReconcileInput, surgePods []*corev1.Pod) (string, bool) {
	if input.StuckPodGrace <= 0 {
		return "", false
	}
	now := input.Now()
	for _, pod := range surgePods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		reason, stuck := evidence.PodStuckInTerminalWaiting(pod, now, input.StuckPodGrace)
		if !stuck {
			continue
		}
		return fmt.Sprintf("surge pod %s wedged in %s past the stuck-pod grace", pod.Name, reason), true
	}
	return "", false
}

// failMigrationOnWedgedSurge closes the record Failed on blocker through
// the shared record path — surge unpinned for the bounded scale-down
// pipeline, source restored to what observation finds with its drain
// hold released — and emits the one Warning that names the pod and the
// reason. No RetryBlock is charged: the wedge indicts the image or the
// node the surge landed on, not an attempt the source is allowed to make
// on its own revision.
func failMigrationOnWedgedSurge(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	rec *workload.MigrationRecord,
	surgeIdx int32,
	blocker string,
) error {
	if err := failMigrationThroughRecord(ctx, deps, input, plan, rec, blocker,
		migrationSourceUnhealthyReason, blocker+"; source pods not runtime-ready"); err != nil {
		return err
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationSurgeWedged,
		"OMENative migration uuid=%s failed: %s; tearing down surge instance=%d",
		rec.RequestUUID, blocker, surgeIdx)
	return nil
}
