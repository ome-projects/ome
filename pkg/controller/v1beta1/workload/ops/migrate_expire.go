package ops

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// migrationExpiredReason is the LastFailure.Reason stamped on a source
// Instance whose pods are observed unhealthy when its migration record
// expires. Distinct from the instance-op DeadlineExceeded reason: the
// timeout authority here is the record's Deadline, not an
// Operation.Deadline.
const migrationExpiredReason = "MigrationExpired"

// ExpireMigrations is the deadline consumer for Manual migration
// records: every non-terminal Manual record in ObservedState.Migrations
// whose Deadline has passed is consumed — not just the one the
// dispatcher would drive, so a queued record can never sit expired
// behind an executing one. Returns how many records reached a terminal
// phase this pass; the dispatcher requeues when > 0 so the next pass
// rebuilds plan + ObservedState from the post-expiry status.
//
// Precedence — a migration that is actually completing wins over
// expiry. Two carve-outs, both decided from live observation:
//
//   - Completed-tail crash window: the completion tail already promoted
//     the surge and removed the source status, but died before resource
//     finalization or the terminal record write. Expiry finishes that
//     idempotent tail and leaves the promoted surge untouched.
//   - Draining with the source fully drained AND the surge passing the
//     drive pass's full completion gates (runtime-ready + in-rotation +
//     Available — the SAME gates, via surgeTailGatesPassed): completion
//     is one idempotent drive tail away (delete drained source pods →
//     promote → remove → Completed), so the drive pass runs instead.
//     This cannot strand the record: the moment any of those
//     observations regresses — drain incomplete, or any surge gate the
//     drive itself would block on forever — the next expiry pass fires.
//
// Everything else past Deadline expires in one transition:
//
//  1. Clear the Migrate op on the surge (it KEEPS its status slot;
//     unpinned, the next pass's plan drops it and the bounded
//     scale-down pipeline tears it down — drain gate,
//     escalation, atomic status removal — no bespoke teardown).
//  2. Clear the source's Migrate op, remove the drive's source-drain
//     serving key from every live source pod (expiry must undo every
//     reversible mutation the drive made — the key's normal removal is
//     pod deletion, which an expired-but-kept source never gets), and
//     restore the source's Phase from OBSERVATION: live source pods
//     all runtime-ready → Ready (RunningRevision untouched); else
//     Failed with a LastFailure. Never a blind stamp. The source stays
//     in the plan (its index is within the replica budget) so it can
//     never become a scale-down extra.
//  3. Mirror a terminal Failed row into the audit ledger — a HARD
//     step: a mirror failure aborts the pass before the terminal
//     record write, so the still-non-terminal record retries next pass
//     until the mirror lands. Without the row, the status entry's
//     eventual trim would let the upgrade import re-synthesize the
//     UUID from its Started row as fresh Accepted work.
//  4. Terminal record write LAST — the crash anchor: a crash anywhere
//     above leaves the record non-terminal and the next pass re-runs
//     the idempotent steps. (Record-first would strand: a terminal
//     record with still-pinned ops has no consumer.) Because this pass
//     runs BEFORE the drive pass, the drive can never re-stamp ops
//     between a partial expiry and its retry.
//  5. Warning event naming the uuid + blocker + surge teardown.
//
// An Accepted record (surge never allocated — SurgeInstance nil or the
// legacy -1 sentinel) has no instance ops to clear and no surge to tear
// down: op stamps only ever happen after the allocation write, so the
// expiry is record + ledger + event only and the source is not touched.
func ExpireMigrations(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan) (int, error) {
	now := input.Now()
	expired := 0
	for i := range input.ObservedState.Migrations {
		rec := &input.ObservedState.Migrations[i]
		if !isExpiredMigrationCandidate(rec, now) {
			continue
		}
		if input.MutateMigration == nil {
			return expired, fmt.Errorf("ExpireMigrations: MutateMigration not wired (uuid=%s)", rec.RequestUUID)
		}
		terminal, err := expireMigrationRecord(ctx, deps, input, plan, rec)
		if err != nil {
			return expired, fmt.Errorf("ExpireMigrations: expire uuid=%s: %w", rec.RequestUUID, err)
		}
		if terminal {
			expired++
		}
	}
	return expired, nil
}

// isExpiredMigrationCandidate reports whether rec is an expiry
// candidate: a non-terminal Manual record whose Deadline has passed. A
// zero Deadline never expires (mirrors the instance-op rule).
func isExpiredMigrationCandidate(rec *workload.MigrationRecord, now time.Time) bool {
	if rec.Trigger != workload.MigrationTriggerManual || rec.Phase.Terminal() {
		return false
	}
	return !rec.Deadline.IsZero() && now.After(rec.Deadline.Time)
}

// HasExpiredMigrationCandidate reports whether any record is an expiry
// candidate ExpireMigrations would consume. The dispatcher's decision
// layer consults this so the (pure) selection and the (effectful)
// consumption share one filter.
func HasExpiredMigrationCandidate(records []workload.MigrationRecord, now time.Time) bool {
	for i := range records {
		if isExpiredMigrationCandidate(&records[i], now) {
			return true
		}
	}
	return false
}

// expireMigrationRecord consumes one expired record. Returns
// terminal=true when the record was closed (Failed, or Completed via
// the completed-tail edge); false when the drive pass should finish it
// instead (Draining, one idempotent tail away).
func expireMigrationRecord(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, rec *workload.MigrationRecord) (bool, error) {
	uuid := rec.RequestUUID
	source := input.ObservedState.Instance(rec.SourceInstance)
	allocated := rec.SurgeAllocated()
	var surge *workload.InstanceStatus
	if allocated {
		surge = input.ObservedState.Instance(*rec.SurgeInstance)
	}

	// The promoted surge and absent source are the durable completion
	// evidence. Residual per-Instance resources must disappear before the
	// record becomes terminal, because terminal records are not driven again.
	if allocated && source == nil && surge != nil && surge.Phase == workload.InstancePhaseReady {
		confirmed, cerr := confirmMigrationCompletionPair(ctx, input, rec.SourceInstance, surge)
		if cerr != nil {
			return false, fmt.Errorf("confirm completed migration pair: %w", cerr)
		}
		if !confirmed {
			return false, nil
		}
		finalized, ferr := status.FinalizeAndRemove(ctx, deps, input, rec.SourceInstance, nil)
		if ferr != nil {
			return false, fmt.Errorf("finalize completed migration source: %w", ferr)
		}
		if !finalized {
			return false, nil
		}
		confirmed, cerr = confirmMigrationCompletionPair(ctx, input, rec.SourceInstance, surge)
		if cerr != nil {
			return false, fmt.Errorf("recheck completed migration pair: %w", cerr)
		}
		if !confirmed {
			return false, nil
		}
		msg := fmt.Sprintf("migrated to instance=%d", *rec.SurgeInstance)
		if err := mirrorTerminalMigrationLedger(ctx, deps, input, plan, rec, audit.PhaseCompleted, "migrated"); err != nil {
			return false, err
		}
		if err := closeMigrationRecord(ctx, input, uuid, workload.MigrationPhaseCompleted, msg); err != nil {
			return false, err
		}
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationCompleted,
			"OMENative migration uuid=%s complete: %s -> instance=%d (record closed at expiry after the completion tail)",
			uuid, workload.InstanceKey(plan.Component, rec.SourceInstance), *rec.SurgeInstance)
		return true, nil
	}

	// Draining precedence: when the source is fully drained AND the
	// surge is fully runtime-ready, completion is one idempotent drive
	// tail away — prefer driving to completion over expiry.
	if allocated && rec.Phase == workload.MigrationPhaseDraining {
		tailReady, err := migrationTailReady(ctx, deps, input, plan, rec.SourceInstance, *rec.SurgeInstance)
		if err != nil {
			return false, err
		}
		if tailReady {
			return false, nil
		}
	}

	blocker := migrationExpiryBlocker(rec.Phase)

	if err := failMigrationThroughRecord(ctx, deps, input, plan, rec, blocker,
		migrationExpiredReason, "migration expired; source pods unhealthy"); err != nil {
		return false, err
	}

	if allocated {
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationExpired,
			"OMENative migration uuid=%s expired: %s; tearing down surge instance=%d",
			uuid, blocker, *rec.SurgeInstance)
	} else {
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationExpired,
			"OMENative migration uuid=%s expired: %s", uuid, blocker)
	}
	return true, nil
}

// failMigrationThroughRecord closes rec Failed THROUGH THE RECORD, the
// single terminal-failure path for a migration whose pair is already
// stamped:
//
//  1. the surge pin is cleared so the next pass's plan drops the index
//     and the bounded scale-down pipeline tears it down,
//  2. the source is restored from live observation with the drive's
//     drain hold released — never a direct Failed stamp,
//  3. the terminal audit row is mirrored (HARD: a mirror failure aborts
//     before the record write so the pass retries), and
//  4. the record's terminal write lands LAST, the crash anchor that
//     makes every step above re-runnable.
//
// blocker is both the record's outcome message and the ledger outcome.
// sourceFailureReason / sourceFailureMessage describe the source only
// when observation finds it unhealthy. An Accepted record with no
// allocated surge has no pair to unwind, so only the ledger and the
// record are written.
func failMigrationThroughRecord(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	rec *workload.MigrationRecord,
	blocker, sourceFailureReason, sourceFailureMessage string,
) error {
	uuid := rec.RequestUUID
	if rec.SurgeAllocated() {
		if err := status.ClearMigrationPin(ctx, input, *rec.SurgeInstance, uuid); err != nil {
			return fmt.Errorf("clear surge Migrate op (instance=%d): %w", *rec.SurgeInstance, err)
		}
		if input.ObservedState.Instance(rec.SourceInstance) != nil {
			if err := restoreSourceFromObservation(ctx, deps, input, plan, rec.SourceInstance, uuid,
				sourceFailureReason, sourceFailureMessage); err != nil {
				return fmt.Errorf("restore source (instance=%d): %w", rec.SourceInstance, err)
			}
		}
	}
	if err := mirrorTerminalMigrationLedger(ctx, deps, input, plan, rec, audit.PhaseFailed, blocker); err != nil {
		return err
	}
	return closeMigrationRecord(ctx, input, uuid, workload.MigrationPhaseFailed, blocker)
}

// migrationTailReady reports whether an expired Draining record is one
// idempotent drive tail away from Completed: every live source pod is
// drained out of its routed Service AND the surge passes the drive
// pass's OWN completion gates — runtime-ready plus the shared
// rotation + availability gates (surgeTailGatesPassed). The surge
// gates must be exactly the drive's: any state the drive blocks on
// (dead surge pod, ready-but-never-in-rotation, not Available) must
// expire, not defer — a weaker check here would park the record
// non-terminal past its deadline forever.
//
// One deliberate exception: a source pod wedged Terminating with NO
// ForceDeletePolicy configured defers (see the wedge check below) —
// the record parks non-terminal rather than failing a migration whose
// surge is serving.
func migrationTailReady(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx, surgeIdx int32) (bool, error) {
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list surge pods: %w", err)
	}
	if !query.AllPodsRuntimeReady(surgePods) {
		return false, nil
	}
	if ok, gerr := surgeTailGatesPassed(ctx, deps, input, plan, surgeIdx, surgePods); gerr != nil || !ok {
		return false, gerr
	}
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("list source pods: %w", err)
	}
	for _, pod := range sourcePods {
		// A source pod wedged past its own deletion deadline (dead
		// node / kubelet gone) means the drive tail — which skips
		// Terminating pods — can never finish. Expiring here tears
		// down a serving surge, so it takes operator authority: only
		// a configured ForceDeletePolicy — whose escalation normally
		// force-deletes the wedged pod and lets the drive complete
		// first, leaving expiry for genuinely uncompletable wedges —
		// and only past its OverdueSlack beyond the pod's deletion
		// deadline (merely grazing it is routine on loaded nodes with
		// large-grace pods). Unconfigured, the record parks
		// non-terminal and the surge keeps serving.
		if input.ForceDelete != nil && pod.DeletionTimestamp != nil &&
			input.Now().After(pod.DeletionTimestamp.Add(input.ForceDelete.OverdueSlack)) {
			return false, nil
		}
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		drained, derr := drain.IsPodDrained(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
		if derr != nil {
			return false, fmt.Errorf("check drain on source pod %s: %w", pod.Name, derr)
		}
		if !drained {
			return false, nil
		}
	}
	return true, nil
}

// restoreSourceFromObservation clears the source's Migrate pin,
// removes the drive's source-drain serving key from every live source
// pod, and sets the source's Phase from what actually runs: live
// source pods all runtime-ready → Ready (RunningRevision untouched —
// it served throughout); else Failed with a LastFailure carrying
// failureReason / failureMessage, so the source's own escalation
// machinery takes over. Never a blind stamp.
//
// The un-drain is unconditional — keyed on live pod state, not the
// record's phase, and applied whichever Phase the observation decides.
// Expiry must undo every reversible mutation the drive made: the
// drain key's normal removal is source-pod deletion at the end of a
// SUCCESSFUL migration, which an expired-but-kept source never gets.
// Without it a Draining-expired source (or one caught in the
// SurgeReady→Draining crash window, keys applied but phase not yet
// advanced) reads Ready while every pod carries serving=False —
// permanently out of the routed Service and invisible to the
// availability counters. A Failed-observed source is un-drained too,
// so a later recovery serves. The removal is idempotent (no-op when
// the key is absent) and tolerates deleted pods — this is a drain-hold
// release, not a promotion: the hold dies with the pod, and nothing
// downstream assumes the pod is in rotation.
func restoreSourceFromObservation(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx int32, uuid, failureReason, failureMessage string) error {
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return fmt.Errorf("list source pods: %w", err)
	}
	for _, pod := range sourcePods {
		if err := podreadiness.RemoveNotReadyKeyIgnoreNotFound(ctx, deps.Client, deps.Reader(), pod, podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}); err != nil {
			return fmt.Errorf("remove source-drain key from pod %s: %w", pod.Name, err)
		}
	}
	healthy := query.AllPodsRuntimeReady(sourcePods)
	return status.StampMigrationSourceRestored(ctx, input, sourceIdx, uuid, healthy, failureReason, failureMessage)
}

// closeMigrationRecord writes the record's terminal phase + outcome
// message + CompletedAt. Idempotent: an already-terminal record is
// untouched.
func closeMigrationRecord(ctx context.Context, input workload.ReconcileInput, uuid string, phase workload.MigrationPhase, message string) error {
	return input.MutateMigration(ctx, uuid, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() {
			return false
		}
		m.Phase = phase
		m.Message = message
		now := metav1.NewTime(input.Now())
		m.CompletedAt = &now
		return true
	})
}

// mirrorTerminalMigrationLedger upserts the terminal audit row for rec.
// A HARD step on the expiry path: callers return the error BEFORE the
// record's terminal write, so the still-non-terminal record simply
// retries next pass until the mirror lands (the crash-anchor ordering
// makes re-entry idempotent — same discipline as the drive path's
// terminal ledger writes). The row is what blocks the trim + upgrade
// import from resurrecting the UUID after the status entry ages out.
func mirrorTerminalMigrationLedger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, rec *workload.MigrationRecord, phase, outcome string) error {
	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), ledgerOwnerObject(input))
	if err != nil {
		return fmt.Errorf("load ledger for terminal mirror (uuid=%s): %w", rec.RequestUUID, err)
	}
	req := &audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(plan.Component),
		Instance:        rec.SourceInstance,
		FromNode:        rec.FromNode,
		HintTargetNodes: append([]string(nil), rec.HintTargetNodes...),
		Reason:          rec.Reason,
	}
	surgeIdx := int32(-1)
	if rec.SurgeInstance != nil {
		surgeIdx = *rec.SurgeInstance
	}
	ledger.UpsertEntry(audit.NewTerminalEntry(*ledger.InFlightEntryOrSeed(rec.RequestUUID, req, surgeIdx), phase, outcome))
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, ledgerOwnerObject(input), ledgerOwnerGVK(input), ledger); err != nil {
		return fmt.Errorf("persist terminal ledger mirror (uuid=%s): %w", rec.RequestUUID, err)
	}
	return nil
}

// migrationExpiryBlocker names what the migration was stuck on when its
// Deadline passed, derived deterministically from the record's phase.
func migrationExpiryBlocker(p workload.MigrationPhase) string {
	switch p {
	case workload.MigrationPhaseAccepted:
		return "deadline exceeded in phase Accepted: surge never allocated"
	case workload.MigrationPhaseSurgePending:
		return "deadline exceeded in phase SurgePending: surge pods never became ready"
	case workload.MigrationPhaseSurgeReady:
		return "deadline exceeded in phase SurgeReady: source drain never started"
	case workload.MigrationPhaseDraining:
		return "deadline exceeded in phase Draining: source drain incomplete"
	default:
		return fmt.Sprintf("deadline exceeded in phase %s", p)
	}
}
