package escalation

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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
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
	// Phase=Failed. The wave is counted on the target revision's
	// RetryBlock first when it is an attempt the ladder covers (see
	// terminalChargeRevision); the owning operation reconciler may retry
	// when the block and its own safety and authorization checks allow.
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
	// DispositionWithheld — the fresh row does not carry the attempt
	// the pass observed, so its end was not written and nothing was
	// announced. A row that concluded the attempt (a promote got there
	// first) recorded nothing at all; a row re-opened under another
	// attempt keeps the ladder wave the ended attempt earned.
	DispositionWithheld
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
//  2. RELOCATABLE — no workload-caused reason, no crash loop among the
//     attempt's pods (attemptCrashLooping), MigrationMode=Auto,
//     relocation budget (operator config) not exhausted, the target
//     revision has served on some Instance (revisionHasServed) and is
//     not Held by the retry ladder (revisionHeld), and the attempt's OWN
//     pods occupy exactly ONE resolvable node (single-pod
//     instances and single-host gangs; multi-node attempts fall
//     through). A live pod of another revision in the same bucket — the
//     serving source of a single-pod surge — is not the attempt's and
//     neither names nor vetoes the node (evidence.AttemptSuspectNode).
//     Record a RELOCATION DIRECTIVE — a TERMINAL AutoRecover
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
//  3. TERMINAL — everything else: clear Operation + Phase=Failed. The
//     wave is one more failed attempt at the target revision and counts
//     on its RetryBlock toward UpdateRetryPolicy.MaxAttempts — the block
//     is written ahead of the clear, exactly as in branch 1, and the
//     bound Holds the revision — except for an environment cause (a
//     scheduler hold), which is never counted, and for evidence that
//     belongs to another revision. With no policy configured no block is
//     written; a repeat failure re-enters the disposition and takes
//     branch 1 if it is genuinely revision-scoped.
//
// reason is the caller's escalation summary (kubelet waiting reason for
// the fast escalator, DeadlineExceeded detail for the deadline
// backstop) used for events and terminal diagnostics.
//
// Gang SURGE attempts must NOT be routed here — their
// Failed-with-Operation continuation drives the gang abandon path.
// Callers keep the plain Failed-preserving-Operation stamp for those.
func DisposeExpiredAttempt(ctx context.Context, deps types.Deps, input types.ReconcileInput, dd types.DispositionDeps, inst types.InstanceStatus, pods []*corev1.Pod, reason string) (DispositionOutcome, error) {
	if inst.Operation == nil {
		return DispositionWithheld, nil
	}
	now := metav1.NewTime(input.Now())
	causePod, matched := evidence.FirstWorkloadCausedPod(pods)

	// Branch 1: workload-caused with a resolvable target revision. Only
	// the classification happens here; heldRev names the revision the
	// branch holds, "" when it does not apply.
	heldRev := ""
	if pod := causePod; pod != nil {
		targetRev := inst.Operation.TargetRevision
		unpinnedCreate := inst.Operation.Type == types.InstanceOperationCreate && targetRev == ""
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
		// No resolvable revision (degenerate: no op target AND empty
		// UpdateRevision) leaves heldRev empty — nothing to hold; dispose
		// terminal below.
		heldRev = targetRev
	}

	// Every write from here on is the observed attempt's: the ladder wave
	// and the directive recorded ahead of the end, and the end itself. A
	// fresh row that concluded the attempt records nothing — it may have
	// succeeded. One re-opened under another attempt keeps the wave the
	// ended attempt earned; the stamps withhold the end themselves.
	standing, err := status.AttemptStandingOnFreshRow(ctx, input, inst.Index, *inst.Operation)
	if err != nil {
		return DispositionWithheld, fmt.Errorf("read the row before disposing its attempt (instance=%d): %w", inst.Index, err)
	}
	if standing == status.AttemptConcluded {
		return DispositionWithheld, nil
	}

	if heldRev != "" {
		// Writer ordering: RetryBlock upsert lands BEFORE the mutation
		// that clears the failed attempt's Operation. Crash-safe: a
		// re-entered disposition (block landed, clear didn't) refreshes
		// the block via the writer's wave dedup without recounting.
		// The wave's cause is read off the matched reason rather than
		// asserted here, so this branch and the gang abandon classify it
		// from the same set.
		if err := types.RecordUpdateFailureInRetryBlock(ctx, input, heldRev, matched, types.FailureCauseOf(matched)); err != nil {
			return DispositionHeldRevision, fmt.Errorf("record retry block for disposed attempt (instance=%d rev=%s): %w", inst.Index, heldRev, err)
		}
		termination := types.PodTerminationWithReason(causePod, matched, now)
		ended, err := endAttempt(ctx, input, inst, pods, termination, parkedWaitAfterWave(input, heldRev, matched, types.FailureCauseOf(matched), now))
		if err != nil {
			return DispositionHeldRevision, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", inst.Index, err)
		}
		if !ended {
			return DispositionWithheld, nil
		}
		if input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(inst.Index, causePod.Name,
				fmt.Sprintf("%s: workload-caused failure; revision %s held for retry — fix the image/config and publish a corrected revision", matched, heldRev))
		}
		return DispositionHeldRevision, nil
	}

	// Branch 2: relocatable — the attempt's own pods occupy exactly one
	// resolvable node, so the rebuild can be steered off it. A single-pod
	// surge shares its Instance index with the serving source, whose node
	// is not the attempt's to record. The directive is written below,
	// once the evidence the backstop derives is available to travel with
	// it.
	//
	// The caller's short reason token ("DeadlineExceeded: ..." →
	// "DeadlineExceeded"; a bare kubelet reason passes through) names the
	// class the escalator matched and keeps LastFailure.Reason
	// grep-stable.
	shortReason := reason
	if i := strings.IndexByte(reason, ':'); i > 0 {
		shortReason = reason[:i]
	}
	// The node is blamed only when the revision is known to work: some
	// Instance has run the target revision to Ready — a sibling serving
	// it now, or this Instance before the attempt — and the failure is
	// one a node can cause (a runtime start rejection, a pod that never
	// reports ready, a pod whose node stopped reporting it; the
	// workload-caused reasons took branch 1 and an unplaced pod names no
	// node). A crash loop is never one: a container that keeps exiting
	// does so on any node, so under either face of the loop the attempt
	// takes the ladder below with no node blame, wherever the revision
	// serves. A never-Ready pod on a revision nobody has served is the
	// revision's own failure until proven otherwise and takes the ladder
	// the same way, so a bad push leaves no exclusion behind for its fix
	// to inherit. A revision the ladder has Held is not known to
	// work either: the hold is the ladder's verdict that the revision
	// itself fails, so a further attempt at it blames no node and records
	// no exclusion — a directive would steer a rebuild the hold never
	// runs.
	relocationNode := ""
	targetRev := attemptTargetRevision(input, inst)
	if causePod == nil && !attemptCrashLooping(pods, targetRev, shortReason) &&
		migrationModeAllowsRelocation(dd.MigrationMode) && dd.AutoMigrateMaxAttempts > 0 &&
		revisionHasServed(input.ObservedState.InstanceStatuses, inst, targetRev) &&
		!revisionHeld(input, targetRev) {
		relocationNode = evidence.AttemptSuspectNode(pods, targetRev)
	}

	// Terminal backstop (branches 2 and 3): clear the Operation + stamp
	// Phase=Failed unconditionally.
	// A rebuild the scheduler could not place outlasted the operator's
	// grace. If this Instance holds node exclusions for the revision it
	// rendered, they are what stands between the pod and the room the
	// cluster has: let them go, so the next rebuild lands wherever it can
	// — the blamed node included — rather than wait forever on a node
	// that is not coming. The scheduler's verdict is the capacity signal;
	// no cluster inventory is consulted.
	if causePod == nil && shortReason == types.WaitingReasonUnschedulable {
		if err := releaseNodeExclusions(ctx, deps, input, inst, targetRev); err != nil {
			return DispositionTerminal, err
		}
	}
	termination := &types.InstanceTermination{
		Reason:  shortReason,
		Message: reason,
		Time:    now,
	}
	limboDetail := ""
	var evidencePod *corev1.Pod
	if causePod != nil {
		// Workload-caused evidence without a resolvable revision still
		// carries the wedged pod's diagnostics into LastFailure.
		termination = types.PodTerminationWithReason(causePod, matched, now)
		evidencePod = causePod
	} else if pod := evidence.FirstPodWaitingForReason(pods, shortReason); pod != nil {
		// Ambiguous runtime-start failures still identify the exact failed
		// pod so operation-specific cleanup can remove only that object.
		termination = types.PodTerminationWithReason(pod, shortReason, now)
		evidencePod = pod
	} else if pod, crash := evidence.FirstCrashLoopingPod(pods, targetRev); pod != nil {
		// A crash loop the deadline caught under either face — parked in
		// CrashLoopBackOff, or Running for the moment between two crashes.
		// The record names the crash and its exit, not a readiness limbo.
		termination = types.PodTerminationWithReason(pod, crash, now)
		evidencePod = pod
	} else if pod, _ := evidence.FirstRunningNotReadyPod(pods, targetRev); pod != nil {
		// Readiness limbo: the branch above chose the disposition, this
		// only says what an operator will read. Without it the record is
		// the bare elapsed timeout and says nothing about the probe.
		termination = evidence.ContainersNotReadyTermination(pod, now)
		limboDetail = termination.Message
		evidencePod = pod
	} else if pod := evidence.FirstGateNotFoldedPod(pods, targetRev); pod != nil {
		// Gate limbo: the probes passed but a readiness gate keeps the pod
		// out of its Service, so no promote can fire. Same standing as the
		// limbo above — evidence, not a branch.
		termination = evidence.GateNotFoldedTermination(pod, now)
		limboDetail = termination.Message
		evidencePod = pod
	}

	directiveRecorded := false
	if relocationNode != "" {
		recorded, rerr := recordRelocationDirective(ctx, deps, input, dd, inst, targetRev, relocationNode, limboDetail)
		if rerr != nil {
			return DispositionTerminal, rerr
		}
		directiveRecorded = recorded
	}
	outcome := DispositionTerminal
	detail := "attempt disposed terminal (no workload-caused evidence, relocation unavailable); operation-specific recovery decides whether another attempt is safe"
	// The wait a parked attempt names, empty when no ladder counts the
	// wave: an uncounted attempt is cleared and re-opened at once.
	parkedWait := ""
	if directiveRecorded {
		outcome = DispositionRelocationDirective
		detail = "attempt disposed with relocation directive; the rebuild is steered off the recorded node"
	} else if chargeRev := terminalChargeRevision(input, inst, evidencePod, shortReason); chargeRev != "" {
		// One wave counts once: the relocation ledger took the waves above,
		// the revision's ladder takes this one. Written ahead of the
		// op-clear so a crash between the two re-enters here and the
		// writer's wave dedup refreshes without recounting.
		waveReason := terminalWaveReason(shortReason, termination)
		if err := types.RecordUpdateFailureInRetryBlock(ctx, input, chargeRev, waveReason, types.CauseUnattributed); err != nil {
			return DispositionTerminal, fmt.Errorf("record retry block for disposed attempt (instance=%d rev=%s): %w", inst.Index, chargeRev, err)
		}
		parkedWait = parkedWaitAfterWave(input, chargeRev, waveReason, types.CauseUnattributed, now)
		detail = "attempt disposed terminal (no workload-caused evidence, relocation unavailable) and counted on the revision's retry ladder"
	}
	ended, err := endAttempt(ctx, input, inst, pods, termination, parkedWait)
	if err != nil {
		return outcome, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", inst.Index, err)
	}
	if !ended {
		return DispositionWithheld, nil
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

// endAttempt writes the disposed attempt's end in one row write. An
// attempt the ladder counted whose own pod set is still alive is parked:
// the row keeps the operation with the wait named on it, so the roll's next
// attempt is visible on the row and the phase can follow a set the
// kubelet goes on retrying — a runner restarted in place serves between
// its crashes, a wedged pod comes up once its cause is gone. An attempt
// whose pods are gone, or that no ladder counted, is cleared. Either
// stamp lands only while the fresh row still carries the observed
// attempt; it reports whether the row changed, and a withheld stamp
// announces nothing.
func endAttempt(ctx context.Context, input types.ReconcileInput, inst types.InstanceStatus, pods []*corev1.Pod, termination *types.InstanceTermination, wait string) (bool, error) {
	if wait != "" && attemptParkable(inst, pods) {
		return status.StampFailedParkingAttempt(ctx, input, inst.Index, *inst.Operation, termination, wait)
	}
	return status.StampFailedEndingAttempt(ctx, input, inst.Index, *inst.Operation, termination)
}

// attemptParkable reports whether a disposed attempt leaves a pod set the
// row can follow: a recreate or in-place Update, single-pod or gang, with
// a live pod of its own revision. A surge keeps a serving source beside
// its replacement and is not parked.
func attemptParkable(inst types.InstanceStatus, pods []*corev1.Pod) bool {
	op := inst.Operation
	if op == nil || op.Type != types.InstanceOperationUpdate || op.SurgeIndex != nil {
		return false
	}
	if op.Step != types.UpdateStepDrain && op.Step != types.UpdateStepInPlace {
		return false
	}
	for _, pod := range evidence.AttemptStuckPods(inst, pods, op.TargetRevision) {
		if pod != nil && pod.DeletionTimestamp == nil && !query.IsTerminalPod(pod) {
			return true
		}
	}
	return false
}

// parkedWaitAfterWave names the wait a parked attempt stands behind once
// this wave is counted: the hold when the ladder holds at it, else the
// ladder's pacing. It is read off the same transition the block writer
// applies, since the pass's observation predates the write.
func parkedWaitAfterWave(input types.ReconcileInput, rev, reason string, cause types.FailureCause, now metav1.Time) string {
	block := types.RetryBlock{TargetRevision: rev}
	if cur := types.FindRetryBlock(input.ObservedState.RetryBlocks, rev); cur != nil {
		block = *cur
	}
	types.ApplyUpdateFailureToRetryBlock(&block, input.UpdateRetryPolicy, now, reason, cause)
	if block.State == types.RetryBlockHeld {
		return string(types.RolloutHoldGateHeld)
	}
	return string(types.RolloutHoldGateRetryBlock)
}

// terminalChargeRevision names the revision a terminal wave counts
// against on the retry ladder, or "" when the wave is not counted: no
// ladder is configured, the cause is the environment's (a scheduler
// hold), the attempt has no resolvable target, or the only evidence is a
// pod on a known, different revision — a leftover of a superseded
// attempt, whose crash must not charge the revision that replaced it.
func terminalChargeRevision(input types.ReconcileInput, inst types.InstanceStatus, evidencePod *corev1.Pod, shortReason string) string {
	if input.UpdateRetryPolicy == nil || types.FailureCauseOf(shortReason) == types.CauseEnvironment {
		return ""
	}
	targetRev := attemptTargetRevision(input, inst)
	if targetRev == "" {
		return ""
	}
	if evidencePod != nil {
		podRev, tgtRev := query.RevisionFromPod(evidencePod), query.RevisionFromName(targetRev)
		if !podRev.IsZero() && !tgtRev.IsZero() && !podRev.Same(tgtRev) {
			return ""
		}
	}
	return targetRev
}

// terminalWaveReason is what the counted wave records on the block: the
// kubelet waiting reason the escalation classified (the crash loop an
// operator reads on the hold), else the evidence's own reason (the probe
// limbo or the bare deadline).
func terminalWaveReason(shortReason string, termination *types.InstanceTermination) string {
	if types.IsTerminalWaitingReason(shortReason) || termination == nil || termination.Reason == "" {
		return shortReason
	}
	return termination.Reason
}

// sameTerminalFailure reports whether the instance's prior LastFailure already
// records this terminal reason, so a repeated disposition of the same
// unresolved failure doesn't re-emit the operator warning + Warning event
// every reconcile.
func sameTerminalFailure(inst types.InstanceStatus, reason string) bool {
	return inst.LastFailure != nil && inst.LastFailure.Reason == reason
}

// attemptCrashLooping reports whether the attempt's failure is a crash
// loop: the escalator classified CrashLoopBackOff, or a live pod of the
// attempt shows either face of one. A container that keeps exiting does
// so on any node, so the rebuild is never steered off the node it ran on.
func attemptCrashLooping(pods []*corev1.Pod, targetRev, shortReason string) bool {
	if shortReason == evidence.ReasonCrashLoop {
		return true
	}
	pod, _ := evidence.FirstCrashLoopingPod(pods, targetRev)
	return pod != nil
}

// migrationModeAllowsRelocation gates branch 2 on the effective
// migration mode. Only the explicit Auto intent (Surge is its spelling
// alias) enables controller-filed relocation; Never and the zero value
// fail safe to the terminal branch.
func migrationModeAllowsRelocation(mode types.MigrationMode) bool {
	return mode == types.MigrationModeAuto || mode == types.MigrationModeSurge
}

// revisionHasServed reports whether rev has run to Ready on some
// Instance. A row's RunningRevision is written only by a promote, so a
// row carrying rev — a sibling serving it now, or the attempt's own row
// from before this attempt — is proof the revision works somewhere.
func revisionHasServed(rows []types.InstanceStatus, own types.InstanceStatus, rev string) bool {
	if rev == "" {
		return false
	}
	if own.RunningRevision == rev {
		return true
	}
	for i := range rows {
		if rows[i].RunningRevision == rev {
			return true
		}
	}
	return false
}

// revisionHeld reports whether the retry ladder has Held rev: the
// ladder's verdict that the revision itself is the failure, which no
// node is blamed for.
func revisionHeld(input types.ReconcileInput, rev string) bool {
	block := types.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
	return block != nil && block.State == types.RetryBlockHeld
}

// attemptTargetRevision resolves the revision an attempt is converging
// toward: the pin its operation carries, else the owner's current
// UpdateRevision (an unpinned Create renders from that).
func attemptTargetRevision(input types.ReconcileInput, inst types.InstanceStatus) string {
	if inst.Operation != nil && inst.Operation.TargetRevision != "" {
		return inst.Operation.TargetRevision
	}
	return input.ObservedState.UpdateRevision
}

// recordRelocationDirective writes the relocation directive: a TERMINAL
// AutoRecover ledger entry (Phase=Completed, Outcome=relocate-recreate,
// FromNode = the suspect node, Revision = the attempt's target, which is
// the only revision the exclusion binds). It is a record — budget accounting,
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
func recordRelocationDirective(ctx context.Context, deps types.Deps, input types.ReconcileInput, dd types.DispositionDeps, inst types.InstanceStatus, revision, fromNode, detail string) (recorded bool, err error) {
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
	// overlay keeps the newest maxAttempts distinct nodes recorded for
	// this revision, which after this write is fromNode plus the newest
	// maxAttempts-1 prior ones.
	exclusions := append(audit.NodesExcludedForRevision(audit.RecentAutoRecoverExclusions(ledger, component, inst.Index, dd.AutoMigrateMaxAttempts-1), revision), fromNode)
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
		Revision:       revision,
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
		if aerr := input.AppendMigration(ctx, types.MigrationRecord{
			RequestUUID:    reqUUID,
			Trigger:        types.MigrationTriggerAuto,
			SourceInstance: inst.Index,
			FromNode:       fromNode,
			Phase:          types.MigrationPhaseRelocated,
			Attempt:        attempts + 1,
			Reason:         audit.ReasonAutoRecover,
			Message:        relocationRecordMessage(fromNode, detail),
			StartedAt:      nowT,
			Deadline:       nowT,
			CompletedAt:    &completedAt,
		}); aerr != nil {
			logf.FromContext(ctx).V(1).Info("relocation status-record mirror failed; ledger remains authoritative",
				"uuid", reqUUID, "instance", inst.Index, "error", aerr.Error())
		}
	}
	if deps.Recorder != nil {
		if target := types.EventTarget(input); target != nil {
			deps.Recorder.Eventf(target, corev1.EventTypeNormal, string(types.EventReasonAutoMigrationTriggered),
				"OMENative component=%s instance=%d relocation directive recorded: attempt %d/%d, rebuild steered off node %s (uuid=%s)",
				component, inst.Index, attempts+1, dd.AutoMigrateMaxAttempts, fromNode, reqUUID)
			// Cap transition: this directive filled the last budget slot.
			// Announce once here; subsequent over-budget dispositions stay
			// silent (guard 3).
			if attempts+1 == dd.AutoMigrateMaxAttempts {
				deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(types.EventReasonAutoMigrationCapReached),
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

// releaseNodeExclusions lets go of the exclusions this Instance holds
// for revision, persisting the ledger ahead of the status write and
// announcing the release once, with the nodes it frees. Nothing held →
// no write, no event. A persist conflict surfaces as the error the
// reconcile retries on; the hold is still in place next pass.
func releaseNodeExclusions(ctx context.Context, deps types.Deps, input types.ReconcileInput, inst types.InstanceStatus, revision string) error {
	owner := dispositionLedgerOwner(input)
	if deps.Client == nil || owner == nil || revision == "" {
		return nil
	}
	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), owner)
	if err != nil {
		return fmt.Errorf("load audit ledger (instance=%d): %w", inst.Index, err)
	}
	component := string(input.Key.Component)
	released := audit.ReleaseAutoRecoverExclusions(ledger, component, inst.Index, revision)
	if len(released) == 0 {
		return nil
	}
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, owner, dispositionLedgerOwnerGVK(input), ledger); err != nil {
		return fmt.Errorf("persist audit ledger (instance=%d): %w", inst.Index, err)
	}
	if deps.Recorder != nil {
		if target := types.EventTarget(input); target != nil {
			deps.Recorder.Eventf(target, corev1.EventTypeNormal, string(types.EventReasonNodeExclusionReleased),
				"OMENative component=%s instance=%d node exclusion released for revision %s: no other node had room for the rebuild; it may land on %s again",
				component, inst.Index, revision, strings.Join(released, ", "))
		}
	}
	return nil
}

// dispositionLedgerOwner / dispositionLedgerOwnerGVK mirror the ops
// migrate ledger-owner resolution: the adapter may point LedgerOwner at
// the user-facing parent (ISVC); nil falls back to OwnerObject.
func dispositionLedgerOwner(input types.ReconcileInput) client.Object {
	if input.LedgerOwner != nil {
		return input.LedgerOwner
	}
	return input.OwnerObject
}

func dispositionLedgerOwnerGVK(input types.ReconcileInput) schema.GroupVersionKind {
	if input.LedgerOwner != nil {
		return input.LedgerOwnerGVK
	}
	return input.OwnerGVK
}

// relocationRecordMessage is the operator-facing summary on a relocation
// directive's visibility record. detail, when the disposition derived
// any, names what actually went wrong on the suspect node — otherwise
// the record says only where the rebuild is steered away from.
func relocationRecordMessage(fromNode, detail string) string {
	base := fmt.Sprintf("relocation directive recorded; rebuild steered off node %s", fromNode)
	if detail == "" {
		return base
	}
	return base + " (" + detail + ")"
}

// disposableAttempt reports whether s's in-flight attempt is owned by
// the deadline disposition (DisposeExpiredAttempt) rather than the
// plain Failed-preserving-Operation stamp.
//
//   - Create ops (any pod count): YES. Creates have no abandon /
//     teardown continuation — a Failed-with-Operation create would
//     re-arm through the Create stamper every pass (the churn loop).
//     The disposition's Operation-clear is correct for gang creates
//     too: a Failed-no-Operation gang create rebuilds via the ordinary
//     trigger + RetryBlock gate.
//   - Recreate and in-place Update ops, single-pod or gang (no gang
//     surge markers): YES — the same re-arming stampers apply. A gang's
//     recreate has no continuation of its own: left Failed with its
//     Operation it is re-driven at the same target on the next pass,
//     which rebuilds the crashing gang every stuck-pod grace with no
//     attempt ever counted on the revision's ladder and no hold ever
//     reported.
//   - Gang surge Update ops (SurgeIndex set / gang surge target step):
//     NO. Their Failed-with-Operation continuation is what routes the
//     dispatcher into abandonFailedGangSurge (surge teardown →
//     RetryBlock → source reset); clearing the Operation here would
//     strand the wedged surge gang.
//   - Restart / Migrate ops: NO — they take the plain
//     Failed-preserving-Operation stamp.
func disposableAttempt(s *types.InstanceStatus) bool {
	if s == nil || s.Operation == nil {
		return false
	}
	switch s.Operation.Type {
	case types.InstanceOperationCreate:
		return true
	case types.InstanceOperationUpdate:
		return s.Operation.SurgeIndex == nil &&
			s.Operation.Step != types.UpdateStepGangSurgeTarget &&
			s.Operation.Step != types.UpdateStepGangSurgeTargetCleanup
	default:
		return false
	}
}
