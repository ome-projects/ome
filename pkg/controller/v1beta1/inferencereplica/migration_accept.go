package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

// unallocatedSurgeIndex marks a ledger Started row whose surge index
// is not yet allocated. Accept only admits the request; the executor
// stamps the real index on the row when it allocates the surge. -1 is
// used (not 0) because 0 is a valid Instance index.
const unallocatedSurgeIndex = int32(-1)

// consumeMigrationRequests is the migration-request mailbox consumer:
// each migration-request annotation on the parent ISVC is either
// consumed into ir.Status.Migrations (validated accept), recorded as a
// Failed ledger row (invalid), or deleted as a re-delivery of an
// already-known UUID. Requests addressed to a sibling Component the
// parent declares are left for that Component's IR pass. At most ONE
// new Manual entry is accepted per pass (deterministic: lowest-sorted
// annotation key first), matching the one-migration-dispatch-per-pass
// pacing.
//
// mode is the Component's effective MigrationMode (the same value the
// dispatcher reads from ComponentPlan.MigrationMode). Under Never a
// valid request is consumed as a BORN-TERMINAL Failed entry: Never
// means no migrations, and the mailbox contract means every consumed
// request gets an immediate, visible answer — parking it Accepted
// until the Deadline would answer late and lie about why it failed.
//
// Write order per accepted request: status entry commit first, then
// the ledger Started row, then the annotation delete — a crash between
// the commit and the delete re-enters the dedup branch next pass and
// the annotation is cleaned up idempotently.
//
// All annotation deletions batch into ONE parent Update at pass end,
// against a fresh parent read under conflict retry; exhausted retries
// return requeue=true and the annotations retry next pass.
func (r *Reconciler) consumeMigrationRequests(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, mode workload.MigrationMode, instanceReadyTimeout time.Duration) (requeue bool, err error) {
	if parent == nil || len(parent.Annotations) == 0 {
		return false, nil
	}
	keys := make([]string, 0, len(parent.Annotations))
	for k := range parent.Annotations {
		if strings.HasPrefix(k, audit.MigrationRequestAnnotationPrefix) {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return false, nil
	}
	sort.Strings(keys)

	// The ledger is parent-owned on this path (same rule as
	// buildReconcileInput's LedgerOwner). Live read: sibling IRs of the
	// same parent write this ledger concurrently, and a cache-lagged
	// snapshot persisted wholesale below would drop their rows.
	ledger, err := audit.LoadLedgerForOwner(ctx, r.APIReader, parent)
	if err != nil {
		return false, fmt.Errorf("load migration audit ledger: %w", err)
	}

	var toDelete []string
	ledgerDirty := false
	accepted := false
	for _, k := range keys {
		uuid := audit.ExtractRequestUUID(k)
		raw := parent.Annotations[k]

		// (a) Re-delivery of an already-known UUID: an entry exists on
		// this IR (any phase) or the ledger records it terminal. Delete
		// the annotation; nothing else to do.
		if uuid != "" && (findMigrationStatus(ir, uuid) != nil || ledger.HasCompletedOrFailedRequest(uuid)) {
			toDelete = append(toDelete, k)
			continue
		}

		// (b) Parse + validate (defense-in-depth behind the webhook —
		// pre-webhook annotations and direct-write paths still arrive
		// here).
		req, perr := audit.ParseMigrationRequest(raw)
		reason := ""
		switch {
		case uuid == "":
			reason = "annotation key carries no request UUID"
		case perr != nil:
			reason = perr.Error()
		}
		if reason == "" {
			switch {
			case req.Component == string(ir.Spec.Component):
				if req.Instance < 0 {
					reason = fmt.Sprintf("instance must be >= 0, got %d", req.Instance)
				}
			case parentHasComponent(parent, req.Component):
				// A sibling Component's request; its own IR pass consumes it.
				continue
			default:
				reason = fmt.Sprintf("component %q does not exist on this InferenceService", req.Component)
			}
		}
		if reason != "" {
			// Invalid: terminal Failed ledger row (keyed by UUID; a
			// UUID-less key gets no row — there is nothing to dedup on),
			// Warning event on the ISVC, annotation consumed.
			if uuid != "" {
				now := metav1.NewTime(r.now()).UTC().Format(time.RFC3339)
				entry := audit.Entry{
					RequestUUID: uuid,
					Phase:       audit.PhaseFailed,
					StartedAt:   now,
					CompletedAt: now,
					Outcome:     reason,
				}
				if req != nil {
					entry.Component = req.Component
					entry.SourceInstance = req.Instance
					entry.FromNode = req.FromNode
				}
				ledger.UpsertEntry(entry)
				ledgerDirty = true
			}
			if r.Recorder != nil {
				// Unknown schemaVersion keeps its own event reason —
				// dashboards alert on version skew specifically.
				eventReason := workload.EventReasonMigrationRequestRejected
				if errors.Is(perr, audit.ErrUnsupportedSchemaVersion) {
					eventReason = workload.EventReasonUnsupportedSchemaVersion
				}
				r.Recorder.Eventf(parent, corev1.EventTypeWarning, string(eventReason),
					"OMENative migration uuid=%s rejected: %s", uuid, reason)
			}
			toDelete = append(toDelete, k)
			continue
		}

		// (c) Valid request, but migration is disabled for this
		// Component: consume as a born-terminal Failed entry (status
		// entry + ledger Failed row + Warning, annotation deleted) —
		// same visibility surfaces as an accept, answered immediately.
		// No pacing gate: rejections are records, never work, so every
		// Never-mode request resolves in one pass.
		if mode == workload.MigrationModeNever {
			now := metav1.NewTime(r.now())
			msg := "migrations disabled by MigrationPolicy Mode=Never"
			if aerr := appendMigrationStatus(ctx, r.statusWriter(), r.liveReader(), ir, v1beta1.MigrationStatus{
				RequestUUID:    uuid,
				Trigger:        v1beta1.MigrationTriggerManual,
				SourceInstance: req.Instance,
				FromNode:       req.FromNode,
				Reason:         req.Reason,
				Phase:          v1beta1.MigrationPhaseFailed,
				Message:        msg,
				StartedAt:      now,
				CompletedAt:    &now,
				Deadline:       now,
			}); aerr != nil {
				return false, fmt.Errorf("append migration status entry (uuid=%s): %w", uuid, aerr)
			}
			ts := now.UTC().Format(time.RFC3339)
			ledger.UpsertEntry(audit.Entry{
				RequestUUID:    uuid,
				Component:      req.Component,
				SourceInstance: req.Instance,
				FromNode:       req.FromNode,
				Phase:          audit.PhaseFailed,
				StartedAt:      ts,
				CompletedAt:    ts,
				Outcome:        msg,
			})
			ledgerDirty = true
			if r.Recorder != nil {
				r.Recorder.Eventf(parent, corev1.EventTypeWarning, string(workload.EventReasonMigrationRequestRejected),
					"OMENative migration uuid=%s rejected: %s", uuid, msg)
			}
			toDelete = append(toDelete, k)
			log.V(1).Info("Migration request rejected: MigrationPolicy Mode=Never",
				"uuid", uuid, "sourceInstance", req.Instance, "fromNode", req.FromNode)
			continue
		}

		// (d) Valid request for this Component. One accept per pass;
		// later keys stay in the mailbox for subsequent passes.
		if accepted {
			continue
		}
		now := metav1.NewTime(r.now())
		if aerr := appendMigrationStatus(ctx, r.statusWriter(), r.liveReader(), ir, v1beta1.MigrationStatus{
			RequestUUID:     uuid,
			Trigger:         v1beta1.MigrationTriggerManual,
			SourceInstance:  req.Instance,
			FromNode:        req.FromNode,
			HintTargetNodes: append([]string(nil), req.HintTargetNodes...),
			Reason:          req.Reason,
			Phase:           v1beta1.MigrationPhaseAccepted,
			StartedAt:       now,
			Deadline:        metav1.NewTime(now.Add(instanceReadyTimeout)),
		}); aerr != nil {
			return false, fmt.Errorf("append migration status entry (uuid=%s): %w", uuid, aerr)
		}
		ledger.UpsertEntry(audit.NewStartedEntry(req, uuid, unallocatedSurgeIndex))
		ledgerDirty = true
		toDelete = append(toDelete, k)
		accepted = true
		log.V(1).Info("Migration request accepted into status.migrations",
			"uuid", uuid, "sourceInstance", req.Instance, "fromNode", req.FromNode)
	}

	if ledgerDirty {
		if perr := audit.PersistLedgerForOwner(ctx, r.Client, parent, isvcGVK, ledger); perr != nil {
			return false, fmt.Errorf("persist migration audit ledger: %w", perr)
		}
	}
	if len(toDelete) == 0 {
		return false, nil
	}
	// Delete against a FRESH parent read under conflict retry — the
	// pass-start snapshot's RV routinely goes stale on a hot parent, and
	// a short-circuited delete would defer dispatch a whole pass.
	parentKey := client.ObjectKeyFromObject(parent)
	uerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		freshParent := &v1beta1.InferenceService{}
		if err := r.APIReader.Get(ctx, parentKey, freshParent); err != nil {
			return err
		}
		changed := false
		for _, k := range toDelete {
			if _, ok := freshParent.Annotations[k]; ok {
				delete(freshParent.Annotations, k)
				changed = true
			}
		}
		if !changed {
			return nil
		}
		return r.Update(ctx, freshParent)
	})
	if uerr != nil {
		if apierrors.IsConflict(uerr) {
			// Retries exhausted on a hot parent; the annotations retry
			// next pass (every branch above is idempotent on re-delivery).
			return true, nil
		}
		if apierrors.IsNotFound(uerr) {
			return false, nil
		}
		return false, fmt.Errorf("delete consumed migration-request annotations: %w", uerr)
	}
	// Mirror the deletions onto the caller's in-memory parent so
	// downstream consumers in this pass observe the consumed mailbox.
	for _, k := range toDelete {
		delete(parent.Annotations, k)
	}
	return false, nil
}

// appendMigrationStatus appends one MigrationStatus entry to
// ir.Status.Migrations under retry.RetryOnConflict, then mirrors the
// committed slice onto the caller's in-memory IR — same persistence
// discipline as buildMutateRetryBlock. Idempotent: an entry with the
// same RequestUUID already present writes nothing.
func appendMigrationStatus(ctx context.Context, writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica, entry v1beta1.MigrationStatus) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	var committed []v1beta1.MigrationStatus
	wrote := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wrote = false
		fresh := &v1beta1.InferenceReplica{}
		source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return workload.ErrStatusOwnerGone
			}
			return fmt.Errorf("re-read IR: %w", err)
		}
		if ownerUID == "" || fresh.UID != ownerUID {
			return workload.ErrStatusOwnerGone
		}
		for i := range fresh.Status.Migrations {
			if fresh.Status.Migrations[i].RequestUUID == entry.RequestUUID {
				committed = fresh.Status.Migrations
				wrote = true
				return nil
			}
		}
		fresh.Status.Migrations = append(fresh.Status.Migrations, entry)
		if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
			if apierrors.IsNotFound(err) {
				return workload.ErrStatusOwnerGone
			}
			return fmt.Errorf("update IR status: %w", err)
		}
		wrote = true
		committed = fresh.Status.Migrations
		return nil
	})
	if err != nil {
		return err
	}
	if wrote {
		out := make([]v1beta1.MigrationStatus, len(committed))
		for i := range committed {
			out[i] = *committed[i].DeepCopy()
		}
		ir.Status.Migrations = out
	}
	return nil
}

// migrationToWorkload converts one v1beta1.MigrationStatus to the
// workload mirror. Field-for-field; pointers deep-copy so the workload
// mirror never aliases the IR's status slice (same discipline as
// retryBlockToWorkload).
func migrationToWorkload(v v1beta1.MigrationStatus) workload.MigrationRecord {
	out := workload.MigrationRecord{
		RequestUUID:    v.RequestUUID,
		Trigger:        workload.MigrationTrigger(v.Trigger),
		SourceInstance: v.SourceInstance,
		FromNode:       v.FromNode,
		Phase:          workload.MigrationPhase(v.Phase),
		Attempt:        v.Attempt,
		Reason:         v.Reason,
		Message:        v.Message,
		StartedAt:      *v.StartedAt.DeepCopy(),
		AllocatedAt:    v.AllocatedAt.DeepCopy(),
		Deadline:       *v.Deadline.DeepCopy(),
		CompletedAt:    v.CompletedAt.DeepCopy(),
	}
	if v.SurgeInstance != nil {
		s := *v.SurgeInstance
		out.SurgeInstance = &s
	}
	if v.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), v.HintTargetNodes...)
	}
	if v.Succeeded != nil {
		b := *v.Succeeded
		out.Succeeded = &b
	}
	return out
}

// migrationFromWorkload is the inverse of migrationToWorkload.
func migrationFromWorkload(w workload.MigrationRecord) v1beta1.MigrationStatus {
	out := v1beta1.MigrationStatus{
		RequestUUID:    w.RequestUUID,
		Trigger:        v1beta1.MigrationTrigger(w.Trigger),
		SourceInstance: w.SourceInstance,
		FromNode:       w.FromNode,
		Phase:          v1beta1.MigrationPhase(w.Phase),
		Attempt:        w.Attempt,
		Reason:         w.Reason,
		Message:        w.Message,
		StartedAt:      *w.StartedAt.DeepCopy(),
		AllocatedAt:    w.AllocatedAt.DeepCopy(),
		Deadline:       *w.Deadline.DeepCopy(),
		CompletedAt:    w.CompletedAt.DeepCopy(),
	}
	if w.SurgeInstance != nil {
		s := *w.SurgeInstance
		out.SurgeInstance = &s
	}
	if w.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), w.HintTargetNodes...)
	}
	if w.Succeeded != nil {
		b := *w.Succeeded
		out.Succeeded = &b
	}
	return out
}

// migrationsFromIR mirrors IR.Status.Migrations onto the workload-side
// MigrationRecord shape for ObservedState.Migrations (the shape the
// dispatcher selects work from). Same mirror discipline as
// retryBlocksFromIR.
func migrationsFromIR(ir *v1beta1.InferenceReplica) []workload.MigrationRecord {
	if len(ir.Status.Migrations) == 0 {
		return nil
	}
	out := make([]workload.MigrationRecord, len(ir.Status.Migrations))
	for i := range ir.Status.Migrations {
		out[i] = migrationToWorkload(ir.Status.Migrations[i])
	}
	return out
}

// buildMutateMigration wraps the workload-typed MigrationRecord mutate
// callback with an IR-typed apiserver round-trip — the
// ReconcileInput.MutateMigration capability. Same shape as
// buildMutateRetryBlock: re-read under retry.RetryOnConflict, locate the
// entry for requestUUID, convert to the workload mirror, apply the
// callback (returning true persists, false writes nothing), persist via
// Status().Update, and mirror the committed slice back onto the
// caller's in-memory IR so later ops in the same pass observe the
// post-write state.
//
// A missing entry is a clean no-op. Owner disappearance or replacement
// returns ErrStatusOwnerGone so callers stop effects from a stale snapshot.
func buildMutateMigration(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, requestUUID string, mutate func(*workload.MigrationRecord) bool) error {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	return func(ctx context.Context, requestUUID string, mutate func(*workload.MigrationRecord) bool) error {
		var committed []v1beta1.MigrationStatus
		wrote := false
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			wrote = false
			fresh := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, reads, key, fresh)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return workload.ErrStatusOwnerGone
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			if ownerUID == "" || fresh.UID != ownerUID {
				return workload.ErrStatusOwnerGone
			}
			pos := -1
			for i := range fresh.Status.Migrations {
				if fresh.Status.Migrations[i].RequestUUID == requestUUID {
					pos = i
					break
				}
			}
			if pos == -1 {
				return nil
			}
			w := migrationToWorkload(fresh.Status.Migrations[pos])
			if !mutate(&w) {
				return nil
			}
			// The subject key is fixed: a callback cannot re-key the entry.
			w.RequestUUID = requestUUID
			fresh.Status.Migrations[pos] = migrationFromWorkload(w)
			if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
				if apierrors.IsNotFound(err) {
					return workload.ErrStatusOwnerGone
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			wrote = true
			committed = make([]v1beta1.MigrationStatus, len(fresh.Status.Migrations))
			for i := range fresh.Status.Migrations {
				committed[i] = *fresh.Status.Migrations[i].DeepCopy()
			}
			return nil
		})
		if err != nil {
			return err
		}
		if wrote {
			ir.Status.Migrations = committed
		}
		return nil
	}
}

// buildAppendMigration wraps the idempotent status append as the
// ReconcileInput.AppendMigration capability — the disposition's
// born-terminal Auto visibility mirror lands through it. Delegates to
// appendMigrationStatus (same RMW + in-memory-mirror discipline as the
// accept path); an entry with the RequestUUID already present writes
// nothing.
func buildAppendMigration(writer statusWriter, reads client.Reader, ir *v1beta1.InferenceReplica) func(ctx context.Context, rec workload.MigrationRecord) error {
	return func(ctx context.Context, rec workload.MigrationRecord) error {
		return appendMigrationStatus(ctx, writer, reads, ir, migrationFromWorkload(rec))
	}
}

// syncMigrationEntries runs the adapter-side status.migrations
// bookkeeping that precedes the accept pass each reconcile:
//
//  1. UPGRADE IMPORT: ledger Started rows for this Component (Reason
//     not AutoRecover/ForceDelete — records, never work orders) with no
//     status entry for the UUID and no terminal counterpart row are
//     synthesized into Accepted Manual entries so pre-upgrade in-flight
//     migrations resume through the status-entry path. Idempotent:
//     entry presence gates. SurgeInstance carries over when the row
//     recorded a real index; the -1 accept sentinel imports as unset
//     (fresh allocation path).
//  2. TRIM: terminal entries whose CompletedAt is older than the
//     capacity rate window are pruned (bounded-by-construction status;
//     full history stays in the ledger + events). Non-terminal entries
//     are never trimmed; terminal entries lacking CompletedAt are kept.
//     INVARIANT: status may forget only what the ledger remembers — an
//     aged terminal entry whose UUID has no terminal ledger row is
//     retained (logged at V(1), once per pass with the count), because
//     trimming it would let step 1 re-synthesize the UUID from its
//     Started row as fresh Accepted work. The expiry path's hard
//     ledger mirror means this backstop should never trigger.
//
// One status write covers both; the committed slice mirrors onto the
// caller's in-memory IR. Ledger read is best-effort — an unreadable
// ledger defers the import AND the trim (nothing may be forgotten
// against an unknown ledger) to the next pass.
func (r *Reconciler) syncMigrationEntries(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, instanceReadyTimeout time.Duration) error {
	if r.Client == nil || ir == nil {
		return nil
	}
	owner := client.Object(ir)
	if parent != nil {
		owner = parent
	}
	ledger, err := audit.LoadLedgerForOwner(ctx, r.Client, owner)
	if err != nil {
		log.V(1).Info("migration upgrade import skipped: ledger load failed; retrying next pass", "error", err.Error())
		ledger = &audit.Ledger{}
	}
	imports := importableLedgerMigrations(ledger, string(ir.Spec.Component), metav1.NewTime(r.now()), instanceReadyTimeout)

	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	var committed []v1beta1.MigrationStatus
	wrote := false
	trimBlocked := 0
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wrote = false
		fresh := &v1beta1.InferenceReplica{}
		source, err := irstatus.GetDecoded(ctx, r.liveReader(), key, fresh)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return workload.ErrStatusOwnerGone
			}
			return fmt.Errorf("re-read IR: %w", err)
		}
		if ownerUID == "" || fresh.UID != ownerUID {
			return workload.ErrStatusOwnerGone
		}
		changed := false

		// Trim aged-out terminal entries — but only those the ledger
		// also records terminally (see the invariant in the func doc).
		cutoff := r.now().Add(-audit.CapacityRateWindow)
		trimBlocked = 0
		kept := fresh.Status.Migrations[:0]
		for i := range fresh.Status.Migrations {
			e := fresh.Status.Migrations[i]
			if e.Phase.Terminal() && e.CompletedAt != nil && e.CompletedAt.Time.Before(cutoff) {
				if ledger.HasCompletedOrFailedRequest(e.RequestUUID) {
					changed = true
					continue
				}
				trimBlocked++
			}
			kept = append(kept, e)
		}
		fresh.Status.Migrations = kept

		// Import ledger rows without an entry.
		for i := range imports {
			exists := false
			for j := range fresh.Status.Migrations {
				if fresh.Status.Migrations[j].RequestUUID == imports[i].RequestUUID {
					exists = true
					break
				}
			}
			if !exists {
				fresh.Status.Migrations = append(fresh.Status.Migrations, *imports[i].DeepCopy())
				changed = true
			}
		}
		if !changed {
			committed = fresh.Status.Migrations
			wrote = true
			return nil
		}
		if err := updateInferenceReplicaStatus(ctx, r.statusWriter(), fresh, source); err != nil {
			if apierrors.IsNotFound(err) {
				return workload.ErrStatusOwnerGone
			}
			return fmt.Errorf("update IR status: %w", err)
		}
		committed = fresh.Status.Migrations
		wrote = true
		return nil
	})
	if err != nil {
		return err
	}
	if trimBlocked > 0 {
		log.V(1).Info("migration trim: retained aged terminal entries with no terminal ledger row (backstop — the ledger must remember what status forgets)",
			"count", trimBlocked)
	}
	if wrote {
		out := make([]v1beta1.MigrationStatus, len(committed))
		for i := range committed {
			out[i] = *committed[i].DeepCopy()
		}
		ir.Status.Migrations = out
	}
	return nil
}

// importableLedgerMigrations selects the ledger Started rows the
// upgrade import synthesizes entries for: this Component's rows whose
// Reason marks operator work (not AutoRecover relocation directives,
// not ForceDelete pod sweeps) and whose UUID has no terminal
// counterpart row. The caller gates on entry absence.
func importableLedgerMigrations(ledger *audit.Ledger, component string, now metav1.Time, instanceReadyTimeout time.Duration) []v1beta1.MigrationStatus {
	if ledger == nil {
		return nil
	}
	var out []v1beta1.MigrationStatus
	for i := range ledger.Entries {
		row := &ledger.Entries[i]
		if row.Phase != audit.PhaseStarted || row.Component != component {
			continue
		}
		if row.Reason == audit.ReasonAutoRecover || row.Reason == audit.ReasonForceDelete {
			continue
		}
		if ledger.HasCompletedOrFailedRequest(row.RequestUUID) {
			continue
		}
		entry := v1beta1.MigrationStatus{
			RequestUUID:     row.RequestUUID,
			Trigger:         v1beta1.MigrationTriggerManual,
			Phase:           v1beta1.MigrationPhaseAccepted,
			SourceInstance:  row.SourceInstance,
			FromNode:        row.FromNode,
			HintTargetNodes: append([]string(nil), row.HintTargetNodes...),
			Reason:          row.Reason,
			StartedAt:       now,
			Deadline:        metav1.NewTime(now.Add(instanceReadyTimeout)),
		}
		if started, ok := parseLedgerTime(row.StartedAt); ok {
			entry.StartedAt = metav1.NewTime(started)
		}
		// A real recorded surge index resumes in place; the accept-time
		// -1 sentinel imports as unset so the executor allocates fresh.
		// An allocated row is executing, so it gets an AllocatedAt for
		// the capacity gate — the row's StartedAt is the best available
		// execution timestamp.
		if row.SurgeInstance >= 0 {
			s := row.SurgeInstance
			entry.SurgeInstance = &s
			allocated := entry.StartedAt
			entry.AllocatedAt = &allocated
		}
		out = append(out, entry)
	}
	return out
}

// parseLedgerTime parses the ledger's RFC3339 timestamps; ok=false on
// empty/malformed input (caller falls back to now).
func parseLedgerTime(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// findMigrationStatus returns the ir.Status.Migrations entry for uuid,
// or nil when absent.
func findMigrationStatus(ir *v1beta1.InferenceReplica, uuid string) *v1beta1.MigrationStatus {
	for i := range ir.Status.Migrations {
		if ir.Status.Migrations[i].RequestUUID == uuid {
			return &ir.Status.Migrations[i]
		}
	}
	return nil
}

// parentHasComponent reports whether the parent ISVC declares the named
// component: engine always; decoder / router per spec presence (same
// enumeration rule as the ISVC webhook's migration-request validation).
func parentHasComponent(parent *v1beta1.InferenceService, component string) bool {
	switch component {
	case string(v1beta1.EngineComponent):
		return true
	case string(v1beta1.DecoderComponent):
		return parent.Spec.Decoder != nil
	case string(v1beta1.RouterComponent):
		return parent.Spec.Router != nil
	}
	return false
}

// now returns the injected clock's time, or wall-clock when no clock
// is wired (mirrors ReconcileInput.Now).
func (r *Reconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}
