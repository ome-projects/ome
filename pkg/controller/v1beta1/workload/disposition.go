package workload

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// DispositionOutcome reports which branch DisposeExpiredAttempt took.
type DispositionOutcome int

const (
	// DispositionHeldRevision — workload-caused failure: RetryBlock
	// recorded against the target revision, Operation cleared,
	// Phase=Failed (one transition).
	DispositionHeldRevision DispositionOutcome = iota
	// DispositionRelocationDirective — relocatable failure: a terminal
	// AutoRecover ledger entry (relocation directive) was recorded so
	// the rebuild is steered off the suspect node, then the same
	// clear-Operation + Phase=Failed backstop as the terminal branch
	// ran. RetryBlocks untouched.
	DispositionRelocationDirective
	// DispositionTerminal — everything else: Operation cleared,
	// Phase=Failed, no RetryBlock. The owning operation reconciler may
	// retry only when its own safety and authorization checks pass.
	DispositionTerminal
	// DispositionSkippedSuperseded — the only workload-caused pod belongs
	// to a SUPERSEDED revision (its revision-hash label is not the current
	// target's), so it is a drain leftover from a prior failed attempt, not
	// this attempt's failure. Nothing is mutated: recording a block would
	// poison the freshly-retargeted corrective revision and wedge recovery
	// . Cleanup of the leftover is owned by the surge reclassify
	// (ops/update_surge.go reclassifyByRevisionHash: non-target pods route
	// to drain) when a rollout is in flight, and by Plan's wreckage scan
	// (UpdateItem.CleanupOnly → ops.CleanupWreckage) when no revision diff
	// exists to trigger one.
	DispositionSkippedSuperseded
)

// DisposeExpiredAttempt classifies one expired / stuck Create-or-Update
// attempt and acts:
//
//  1. WORKLOAD-CAUSED — a live pod shows a workload-caused waiting
//     reason (IsWorkloadCausedReason) AND a target revision is resolvable
//     (Operation.TargetRevision, falling back to the owner's
//     UpdateRevision for an unpinned Create whose pod does not prove a
//     different revision): record the RetryBlock for that revision,
//     then clear the Operation and stamp Phase=Failed in ONE
//     MutateInstance call. The owning operation reconciler evaluates
//     the Failed-with-no-Operation state on the next pass; the RetryBlock
//     gate denies the same revision and admits a corrected one. No
//     resolvable revision → there is nothing to hold; fall through to
//     the terminal branch rather than report a held revision with no
//     block.
//  2. RELOCATABLE — no workload-caused reason, MigrationMode=Auto,
//     relocation budget (operator config) not exhausted, and the
//     attempt's pods occupy exactly ONE resolvable node (single-pod
//     instances and single-host gangs; multi-node attempts fall
//     through): record a RELOCATION DIRECTIVE — a TERMINAL AutoRecover
//     ledger entry (evidence, budget accounting, and node-exclusion
//     memory; NOT a migration work order — the migration detector
//     ignores AutoRecover entries), then apply the SAME unconditional
//     clear-Operation + Phase=Failed backstop as branch 3. The normal
//     rebuild relocates: renders for this instance carry a NodeAffinity
//     NotIn overlay built from its AutoRecover entries. RetryBlocks are
//     never touched. Budget exhausted → branch 3 without a new entry
//     (the cap-reached warning fired once, when the final budget slot
//     was recorded). An exclusion the template's required node affinity
//     can never satisfy also falls through to branch 3 — see
//     DispositionDeps.PodSpec.
//  3. TERMINAL — everything else: clear Operation + Phase=Failed. No
//     RetryBlock; a repeat failure re-enters the disposition and takes
//     branch 1 if it is genuinely revision-scoped.
//
// reason is the caller's escalation summary (kubelet waiting reason for
// the fast escalator, DeadlineExceeded detail for the deadline
// backstop) used for events and terminal diagnostics.
//
// Gang (multi-pod) UPDATE attempts must NOT be routed here — their
// Failed-with-Operation continuation drives the gang abandon path.
// Callers keep the plain Failed-preserving-Operation stamp for those.
func DisposeExpiredAttempt(ctx context.Context, deps Deps, input ReconcileInput, dd DispositionDeps, inst InstanceStatus, pods []*corev1.Pod, reason string) (DispositionOutcome, error) {
	now := metav1.NewTime(input.Now())
	causePod, matched := firstWorkloadCausedPod(pods)

	// Branch 1: workload-caused with a resolvable target revision.
	if pod := causePod; pod != nil {
		targetRev := ""
		unpinnedCreate := false
		if inst.Operation != nil {
			targetRev = inst.Operation.TargetRevision
			unpinnedCreate = inst.Operation.Type == InstanceOperationCreate && targetRev == ""
		}
		if targetRev == "" {
			// An empty target is a supported persisted state. The pod label
			// below decides whether the live rollout target can own its failure.
			targetRev = input.ObservedState.UpdateRevision
		}
		// Superseded-leftover guard: a corrective edit retargets the
		// instance to a new revision C, but the prior failed attempt's bad pod
		// (revision B) can linger in its drain window. Attributing that B pod's
		// failure to the freshly-stamped Operation.TargetRevision (now C) would
		// record a RetryBlock against C — the good revision — and the update
		// trigger would then deny C forever, wedging recovery. Skip only when
		// the cause pod is on a KNOWN, DIFFERENT revision than the target;
		// the leftover's cleanup is owned by the surge reclassify (in-flight
		// rollouts) or Plan's wreckage scan (zero revision distance) — see
		// DispositionSkippedSuperseded. Inert when the pod carries no
		// revision-hash label (zero), so the ordinary same-revision failure
		// path is unchanged.
		podRev := query.RevisionFromPod(pod)
		tgtRev := query.RevisionFromName(targetRev)
		if !podRev.IsZero() && !tgtRev.IsZero() && !podRev.Same(tgtRev) {
			if !unpinnedCreate {
				return DispositionSkippedSuperseded, nil
			}
			// The pod proves an unpinned Create belongs to another revision.
			// Clear the attempt without charging either revision below.
			targetRev = ""
		}
		if targetRev != "" {
			// Writer ordering: RetryBlock upsert lands BEFORE the mutation
			// that clears the failed attempt's Operation. Crash-safe: a
			// re-entered disposition (block landed, clear didn't) refreshes
			// the block via the writer's wave dedup without recounting. The
			// evidence here is workload-caused by construction, so the wave
			// charges the ladder.
			if err := RecordUpdateFailureInRetryBlock(ctx, input, targetRev, matched, true); err != nil {
				return DispositionHeldRevision, fmt.Errorf("record retry block for disposed attempt (instance=%d rev=%s): %w", inst.Index, targetRev, err)
			}
			termination := PodTerminationWithReason(pod, matched, now)
			if err := failInstanceClearingOperation(ctx, input, inst.Index, termination); err != nil {
				return DispositionHeldRevision, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", inst.Index, err)
			}
			if input.WarnInstanceFailed != nil {
				input.WarnInstanceFailed(inst.Index, pod.Name,
					fmt.Sprintf("%s: workload-caused failure; revision %s held for retry — fix the image/config and publish a corrected revision", matched, targetRev))
			}
			return DispositionHeldRevision, nil
		}
		// No resolvable revision (degenerate: no op target AND empty
		// UpdateRevision) — nothing to hold; dispose terminal below.
	}

	// Branch 2: relocatable — record the directive, then fall through
	// to the unconditional terminal backstop.
	directiveRecorded := false
	if causePod == nil &&
		migrationModeAllowsRelocation(dd.MigrationMode) && dd.AutoMigrateMaxAttempts > 0 {
		if node := singleLiveNode(pods); node != "" {
			recorded, err := recordRelocationDirective(ctx, deps, input, dd, inst, node)
			if err != nil {
				return DispositionTerminal, err
			}
			directiveRecorded = recorded
		}
	}

	// Terminal backstop (branches 2 and 3): clear the Operation + stamp
	// Phase=Failed unconditionally. Preserve the caller's short reason
	// token ("DeadlineExceeded: ..." → "DeadlineExceeded"; a bare
	// kubelet reason passes through) so LastFailure.Reason stays
	// grep-stable.
	shortReason := reason
	if i := strings.IndexByte(reason, ':'); i > 0 {
		shortReason = reason[:i]
	}
	termination := &InstanceTermination{
		Reason:  shortReason,
		Message: reason,
		Time:    now,
	}
	if causePod != nil {
		// Workload-caused evidence without a resolvable revision still
		// carries the wedged pod's diagnostics into LastFailure.
		termination = PodTerminationWithReason(causePod, matched, now)
	} else if pod := firstPodWaitingForReason(pods, shortReason); pod != nil {
		// Ambiguous runtime-start failures still identify the exact failed
		// pod so operation-specific cleanup can remove only that object.
		termination = PodTerminationWithReason(pod, shortReason, now)
	}
	outcome := DispositionTerminal
	detail := "attempt disposed terminal (no workload-caused evidence, relocation unavailable); operation-specific recovery decides whether another attempt is safe"
	if directiveRecorded {
		outcome = DispositionRelocationDirective
		detail = "attempt disposed with relocation directive; the rebuild is steered off the recorded node"
	}
	if err := failInstanceClearingOperation(ctx, input, inst.Index, termination); err != nil {
		return outcome, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", inst.Index, err)
	}
	// Suppress the per-reconcile warn+event storm when an instance is stuck
	// oscillating on the SAME unresolved failure — e.g. a same-target update
	// whose new-revision pod persistently CrashLoopBackOffs: the Terminal
	// disposition clears the op + stamps Failed, the next pass re-detects the
	// still-needed update and re-attempts (no pod actually replaced), and this
	// path re-fires every ~reconcile. inst is the pre-disposition observation,
	// so inst.LastFailure carries the prior reason; only warn when this is a
	// NEW terminal reason.
	if input.WarnInstanceFailed != nil && !sameTerminalFailure(inst, shortReason) {
		input.WarnInstanceFailed(inst.Index, "", fmt.Sprintf("%s: %s", reason, detail))
	}
	return outcome, nil
}

// sameTerminalFailure reports whether the instance's prior LastFailure already
// records this terminal reason, so a repeated disposition of the same
// unresolved failure doesn't re-emit the operator warning + Warning event
// every reconcile.
func sameTerminalFailure(inst InstanceStatus, reason string) bool {
	return inst.LastFailure != nil && inst.LastFailure.Reason == reason
}

// migrationModeAllowsRelocation gates branch 2 on the effective
// migration mode. Only the explicit Auto intent (Surge is its spelling
// alias) enables controller-filed relocation; Never and the zero value
// fail safe to the terminal branch.
func migrationModeAllowsRelocation(mode MigrationMode) bool {
	return mode == MigrationModeAuto || mode == MigrationModeSurge
}

// firstWorkloadCausedPod returns the first live (non-deleting) pod with
// a workload-caused container or init-container waiting reason
// (IsWorkloadCausedReason), plus the matched reason.
func firstWorkloadCausedPod(pods []*corev1.Pod) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting == nil {
				continue
			}
			if IsWorkloadCausedReason(cs.State.Waiting.Reason) {
				return pod, cs.State.Waiting.Reason
			}
		}
		for _, cs := range pod.Status.InitContainerStatuses {
			if cs.State.Waiting == nil {
				continue
			}
			if IsWorkloadCausedReason(cs.State.Waiting.Reason) {
				return pod, cs.State.Waiting.Reason
			}
		}
	}
	return nil, ""
}

func firstPodWaitingForReason(pods []*corev1.Pod, reason string) *corev1.Pod {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
			for _, status := range statuses {
				if status.State.Waiting != nil && status.State.Waiting.Reason == reason {
					return pod
				}
			}
		}
	}
	return nil
}

// singleLiveNode returns the one node name hosting every pod of the
// instance (single-pod, or a single-host gang), or "" when there are no
// pods, any pod is unscheduled, or pods span multiple nodes — a
// multi-node attempt has no single suspect node to record, so it takes
// the terminal branch; the never-scheduled case has nothing to record.
func singleLiveNode(pods []*corev1.Pod) string {
	if len(pods) == 0 {
		return ""
	}
	var observed string
	for _, pod := range pods {
		if pod == nil || pod.Spec.NodeName == "" {
			return ""
		}
		if observed == "" {
			observed = pod.Spec.NodeName
			continue
		}
		if observed != pod.Spec.NodeName {
			return ""
		}
	}
	return observed
}

// recordRelocationDirective writes the relocation directive: a TERMINAL
// AutoRecover ledger entry (Phase=Completed, Outcome=relocate-recreate,
// FromNode = the suspect node). It is a record — budget accounting,
// node-exclusion memory, audit evidence — NOT a migration work order:
// Auto records are born terminal and the work loop selects only
// non-terminal Manual entries, so the Ready-source copier machinery is
// never fed a broken source. The rebuild relocates through the
// render-time NotIn overlay instead.
//
// Three guards run BEFORE recording, in order:
//
//  1. REPLAY: when the newest AutoRecover entry for (component,
//     instance) carries the same FromNode AND a CompletedAt newer than
//     the current Operation's StartedAt, that entry IS this attempt's
//     directive — persisted on a prior pass that crashed (or lost a
//     stale-cache race) before the op-clear landed. Return
//     recorded=true with NO new entry, event, or metric; a replayed
//     write would burn a budget slot and double-count.
//  2. AFFINITY CONFLICT: when excluding fromNode plus the instance's
//     recorded exclusions would leave the pod templates' required node
//     affinity unsatisfiable (dd.PodSpec / dd.WorkerPodSpec), return
//     recorded=false — the caller disposes terminal instead of
//     recording an exclusion whose rebuild could never schedule.
//  3. BUDGET: at/over dd.AutoMigrateMaxAttempts, return recorded=false
//     so the caller disposes terminal without a new entry. Silent: the
//     cap-reached warning is emitted exactly once, at the TRANSITION —
//     when the directive that fills the final budget slot is recorded
//     below — so post-cap dispose cycles don't re-warn indefinitely.
//
// After the ledger persist succeeds (and only then — the ledger stays
// the exclusion-memory authority), a born-terminal Auto status record
// (Phase=Relocated) is mirrored through input.AppendMigration. The
// record is visibility, not work: it counts toward neither capacity
// cap (terminal → not in-flight; never allocates a surge → no
// AllocatedAt for the per-hour window — Auto churn is bounded
// separately by maxAttempts per instance) and the executor's work loop
// never selects it. Best-effort: a nil closure or a failed write is
// V(1)-logged and never fails the disposition — the relocation itself
// is ledger-driven.
func recordRelocationDirective(ctx context.Context, deps Deps, input ReconcileInput, dd DispositionDeps, inst InstanceStatus, fromNode string) (recorded bool, err error) {
	owner := dispositionLedgerOwner(input)
	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), owner)
	if err != nil {
		return false, fmt.Errorf("load audit ledger (instance=%d): %w", inst.Index, err)
	}
	component := string(input.Key.Component)

	// Guard 1: replay of an already-persisted directive for this attempt.
	// Anchored on the Operation's StartedAt; an op without one (synthetic
	// / zero value) can't prove the entry postdates it, so it records
	// normally.
	if newest := audit.NewestAutoRecoverEntry(ledger, component, inst.Index); newest != nil &&
		inst.Operation != nil && !inst.Operation.StartedAt.IsZero() && newest.FromNode == fromNode {
		if completed, ok := newest.CompletedAtTime(); ok && completed.After(inst.Operation.StartedAt.Time) {
			return true, nil
		}
	}

	// Guard 2: an unsatisfiable exclusion set disposes terminal instead.
	// Evaluate exactly the set the post-record rebuild will render: the
	// overlay keeps the newest maxAttempts distinct nodes, which after
	// this write is fromNode plus the newest maxAttempts-1 prior ones.
	exclusions := append(audit.RecentAutoRecoverFromNodes(ledger, component, inst.Index, dd.AutoMigrateMaxAttempts-1), fromNode)
	if workloadops.WouldExclusionsConflictWithNodeAffinity(dd.PodSpec, exclusions) ||
		workloadops.WouldExclusionsConflictWithNodeAffinity(dd.WorkerPodSpec, exclusions) {
		return false, nil
	}

	// Guard 3: relocation budget.
	attempts := audit.CountAutoRecoverAttempts(ledger, component, inst.Index)
	if attempts >= dd.AutoMigrateMaxAttempts {
		return false, nil
	}

	nowT := metav1.NewTime(input.Now())
	now := nowT.UTC().Format(time.RFC3339)
	reqUUID := uuid.NewString()
	ledger.UpsertEntry(audit.Entry{
		RequestUUID:    reqUUID,
		Component:      component,
		SourceInstance: inst.Index,
		Phase:          audit.PhaseCompleted,
		Reason:         audit.ReasonAutoRecover,
		Outcome:        audit.OutcomeRelocateRecreate,
		FromNode:       fromNode,
		StartedAt:      now,
		CompletedAt:    now,
	})
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, owner, dispositionLedgerOwnerGVK(input), ledger); err != nil {
		return false, fmt.Errorf("persist audit ledger (instance=%d): %w", inst.Index, err)
	}
	// Visibility mirror, only after the ledger persist: a born-terminal
	// Auto record. Deadline is required in the schema but carries no
	// semantics on a born-terminal record — stamped = now.
	if input.AppendMigration != nil {
		completedAt := nowT
		if aerr := input.AppendMigration(ctx, MigrationRecord{
			RequestUUID:    reqUUID,
			Trigger:        MigrationTriggerAuto,
			SourceInstance: inst.Index,
			FromNode:       fromNode,
			Phase:          MigrationPhaseRelocated,
			Attempt:        attempts + 1,
			Reason:         audit.ReasonAutoRecover,
			Message:        fmt.Sprintf("relocation directive recorded; rebuild steered off node %s", fromNode),
			StartedAt:      nowT,
			Deadline:       nowT,
			CompletedAt:    &completedAt,
		}); aerr != nil {
			logf.FromContext(ctx).V(1).Info("relocation status-record mirror failed; ledger remains authoritative",
				"uuid", reqUUID, "instance", inst.Index, "error", aerr.Error())
		}
	}
	if deps.Recorder != nil {
		if target := dispositionEventTarget(input); target != nil {
			deps.Recorder.Eventf(target, corev1.EventTypeNormal, string(EventReasonAutoMigrationTriggered),
				"OMENative component=%s instance=%d relocation directive recorded: attempt %d/%d, rebuild steered off node %s (uuid=%s)",
				component, inst.Index, attempts+1, dd.AutoMigrateMaxAttempts, fromNode, reqUUID)
			// Cap transition: this directive filled the last budget slot.
			// Announce once here; subsequent over-budget dispositions stay
			// silent (guard 3).
			if attempts+1 == dd.AutoMigrateMaxAttempts {
				deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(EventReasonAutoMigrationCapReached),
					"OMENative component=%s instance=%d relocation budget exhausted (%d/%d attempts); further expiries dispose terminal without relocation",
					component, inst.Index, attempts+1, dd.AutoMigrateMaxAttempts)
			}
		}
	}
	if dd.OnRelocationDirective != nil {
		dd.OnRelocationDirective(component)
	}
	return true, nil
}

// failInstanceClearingOperation delegates to the shared writer in the
// types package (types.FailInstanceClearingOperation) — one
// implementation for the dispositions here and the apiserver-rejection
// path in workload/ops.
func failInstanceClearingOperation(ctx context.Context, input ReconcileInput, idx int32, termination *InstanceTermination) error {
	return types.FailInstanceClearingOperation(ctx, input, idx, termination)
}

// dispositionLedgerOwner / dispositionLedgerOwnerGVK mirror the ops
// migrate ledger-owner resolution: the adapter may point LedgerOwner at
// the user-facing parent (ISVC); nil falls back to OwnerObject.
func dispositionLedgerOwner(input ReconcileInput) client.Object {
	if input.LedgerOwner != nil {
		return input.LedgerOwner
	}
	return input.OwnerObject
}

func dispositionLedgerOwnerGVK(input ReconcileInput) schema.GroupVersionKind {
	if input.LedgerOwner != nil {
		return input.LedgerOwnerGVK
	}
	return input.OwnerGVK
}

// dispositionEventTarget mirrors the ops event-target rule:
// input.EventTarget when set, falling back to OwnerObject.
func dispositionEventTarget(input ReconcileInput) client.Object {
	if input.EventTarget != nil {
		return input.EventTarget
	}
	return input.OwnerObject
}
