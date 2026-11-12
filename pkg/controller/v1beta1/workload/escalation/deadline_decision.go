package escalation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// DeadlineDecision is the answer DecideExpiredAttempt gives for one
// expired attempt: which branch ended it, or why nothing was written.
type DeadlineDecision int

const (
	// DeadlineDecisionRevisionHeld: the failure was the workload's own, so
	// the target revision's retry record took the failed attempt and the
	// attempt ended in the same pass.
	DeadlineDecisionRevisionHeld DeadlineDecision = iota
	// DeadlineDecisionRelocate: the revision works elsewhere and the
	// attempt's pods sat on one node, so a node exclusion was recorded and
	// the attempt ended. The rebuild avoids that node; no retry record is
	// charged.
	DeadlineDecisionRelocate
	// DeadlineDecisionTerminal: the attempt ended. The failure counts on
	// the target revision's retry record unless the environment caused it
	// or no retry policy is configured.
	DeadlineDecisionTerminal
	// DeadlineDecisionSkippedTargetReplaced: the only failing pod runs a
	// known revision other than the attempt's target, a leftover of an
	// earlier attempt. Nothing is written, so the revision that replaced
	// it is not charged; the surge reclassification or the leftover-pod
	// cleanup removes the pod.
	DeadlineDecisionSkippedTargetReplaced
	// DeadlineDecisionHeldBack: the fresh row no longer carries the attempt
	// the pass observed, so no end was written and nothing was announced.
	// A row that concluded the attempt recorded nothing; a row re-opened
	// under another attempt keeps the failed attempt already counted.
	DeadlineDecisionHeldBack
)

// The summaries an operator reads on the event of an attempt the relocate
// and terminal branches ended.
const (
	terminalEndSummary        = "attempt disposed terminal (no workload-caused evidence, relocation unavailable); operation-specific recovery decides whether another attempt is safe"
	terminalCountedEndSummary = "attempt disposed terminal (no workload-caused evidence, relocation unavailable) and counted on the revision's retry ladder"
	relocatedEndSummary       = "attempt disposed with relocation directive; the rebuild is steered off the recorded node"
)

// DecideExpiredAttempt ends one Create, recreate or in-place Update
// attempt whose deadline passed, whose pod the kubelet cannot start, or
// whose pod the scheduler could not place within the operator's grace,
// and says what the failure means. The branches run in this order:
//
//  1. holdRevision: the failure was the workload's own, so the target
//     revision is held until a corrected one arrives.
//  2. relocate: the revision works elsewhere and the attempt's pods sat
//     on one node, so the rebuild avoids that node.
//  3. endTerminal: anything else, counted on the target revision's retry
//     record unless the environment caused it.
//
// reason is the caller's summary of what expired: the kubelet waiting
// reason, the deadline text, or the scheduler's message. Its text before
// the first colon names the failure class. A gang surge attempt never
// comes here: its Failed row keeps the operation for the gang abandon
// path.
func DecideExpiredAttempt(ctx context.Context, deps types.Deps, input types.ReconcileInput, dd types.DispositionDeps, inst types.InstanceStatus, pods []*corev1.Pod, reason string) (DeadlineDecision, error) {
	if inst.Operation == nil {
		return DeadlineDecisionHeldBack, nil
	}
	now := metav1.NewTime(input.Now())
	causePod, causeReason := evidence.FirstWorkloadCausedPod(pods)
	heldRev, targetReplaced := workloadCausedRevision(input, inst, causePod)
	if targetReplaced {
		return DeadlineDecisionSkippedTargetReplaced, nil
	}
	// Every write below belongs to the attempt the pass observed. A fresh
	// row that concluded it may have succeeded, so nothing is written.
	standing, err := status.AttemptStandingOnFreshRow(ctx, input, inst.Index, *inst.Operation)
	if err != nil {
		return DeadlineDecisionHeldBack, fmt.Errorf("read the row before deciding its expired attempt (instance=%d): %w", inst.Index, err)
	}
	if standing == status.AttemptConcluded {
		return DeadlineDecisionHeldBack, nil
	}
	if heldRev != "" {
		return holdRevision(ctx, deps, input, inst, pods, causePod, causeReason, heldRev, now)
	}
	attempt := readExpiredAttempt(input, inst, pods, causePod, causeReason, reason, now)
	if attempt.unplaceable() {
		if err := releaseNodeExclusions(ctx, deps, input, inst, attempt.targetRev); err != nil {
			return DeadlineDecisionTerminal, err
		}
	}
	relocated, err := relocate(ctx, deps, input, dd, attempt)
	if err != nil {
		return DeadlineDecisionTerminal, err
	}
	if relocated {
		return endExpiredAttempt(ctx, deps, input, attempt, DeadlineDecisionRelocate, relocatedEndSummary, "")
	}
	return endTerminal(ctx, deps, input, attempt)
}

// workloadCausedRevision names the revision a workload-caused failure
// holds, "" when it holds none: no pod of the attempt shows such a
// waiting reason, or no revision resolves. The attempt's target revision
// is charged; a Create with no target recorded is charged to the owner's
// UpdateRevision unless its pod's label proves another revision, in
// which case nothing is charged. targetReplaced reports a pod whose label
// names a known revision other than the target: a leftover of an earlier
// attempt, which must not charge the revision that replaced it.
func workloadCausedRevision(input types.ReconcileInput, inst types.InstanceStatus, causePod *corev1.Pod) (heldRev string, targetReplaced bool) {
	if causePod == nil {
		return "", false
	}
	targetRev := inst.Operation.TargetRevision
	createWithoutTarget := inst.Operation.Type == types.InstanceOperationCreate && targetRev == ""
	if targetRev == "" {
		targetRev = input.ObservedState.UpdateRevision
	}
	podRev, tgtRev := query.RevisionFromPod(causePod), query.RevisionFromName(targetRev)
	if !podRev.IsZero() && !tgtRev.IsZero() && !podRev.Same(tgtRev) {
		return "", !createWithoutTarget
	}
	return targetRev, false
}

// holdRevision is the branch for a failure that is the workload's own: a
// live pod of the attempt shows a waiting reason the kubelet retries in
// place (an image it cannot pull, an invalid image name, a container
// configuration error) and a revision resolves to charge. The revision's
// retry record takes the failed attempt first and the attempt ends
// second, so a crash between the two re-enters here and the record's
// dedup keeps the count. The warning names the held revision; the
// operator publishes a corrected one.
func holdRevision(ctx context.Context, deps types.Deps, input types.ReconcileInput, inst types.InstanceStatus, pods []*corev1.Pod, causePod *corev1.Pod, causeReason, heldRev string, now metav1.Time) (DeadlineDecision, error) {
	cause := types.FailureCauseOf(causeReason)
	if err := types.RecordUpdateFailureInRetryBlock(ctx, input, heldRev, causeReason, cause); err != nil {
		return DeadlineDecisionRevisionHeld, fmt.Errorf("record retry block for the expired attempt (instance=%d rev=%s): %w", inst.Index, heldRev, err)
	}
	termination := types.PodTerminationWithReason(causePod, causeReason, now)
	ended, err := endAttempt(ctx, deps, input, inst, pods, termination, parkedWaitAfterFailure(input, heldRev, causeReason, cause, now))
	if err != nil {
		return DeadlineDecisionRevisionHeld, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", inst.Index, err)
	}
	if !ended {
		return DeadlineDecisionHeldBack, nil
	}
	if input.WarnInstanceFailed != nil {
		input.WarnInstanceFailed(inst.Index, causePod.Name,
			fmt.Sprintf("%s: workload-caused failure; revision %s held for retry — fix the image/config and publish a corrected revision", causeReason, heldRev))
	}
	return DeadlineDecisionRevisionHeld, nil
}

// expiredAttempt is what the relocate and terminal branches read and
// record about an attempt whose failure is not the workload's own.
type expiredAttempt struct {
	inst types.InstanceStatus
	pods []*corev1.Pod
	// causePod is a pod with a workload-caused waiting reason that no
	// revision could be charged for; nil otherwise.
	causePod *corev1.Pod
	// reason is the caller's full summary; shortReason is its text before
	// the first colon, the failure class the record and the dedup use.
	reason      string
	shortReason string
	targetRev   string
	now         metav1.Time
	// termination is the failure record the end writes, naming
	// evidencePod when a pod explains the failure better than the elapsed
	// deadline does. notReadyDetail is the probe or gate text a relocation
	// record carries when the pod ran but never reported ready.
	termination    *types.InstanceTermination
	evidencePod    *corev1.Pod
	notReadyDetail string
}

// readExpiredAttempt gathers the facts of an expired attempt without
// writing anything. The pod that best explains the failure is chosen in
// this order: the workload-caused pod, a pod waiting for the classified
// reason, a crash-looping pod of the target revision, a running pod that
// never reported ready, a pod whose readiness gate never folded into
// Ready. The bare deadline stands when no pod explains more.
func readExpiredAttempt(input types.ReconcileInput, inst types.InstanceStatus, pods []*corev1.Pod, causePod *corev1.Pod, causeReason, reason string, now metav1.Time) expiredAttempt {
	a := expiredAttempt{inst: inst, pods: pods, causePod: causePod, reason: reason, shortReason: reason, now: now}
	if i := strings.IndexByte(reason, ':'); i > 0 {
		a.shortReason = reason[:i]
	}
	a.targetRev = attemptTargetRevision(input, inst)
	a.termination = &types.InstanceTermination{Reason: a.shortReason, Message: reason, Time: now}
	if causePod != nil {
		a.termination, a.evidencePod = types.PodTerminationWithReason(causePod, causeReason, now), causePod
		return a
	}
	if pod := evidence.FirstPodWaitingForReason(pods, a.shortReason); pod != nil {
		a.termination, a.evidencePod = types.PodTerminationWithReason(pod, a.shortReason, now), pod
		return a
	}
	if pod, crash := evidence.FirstCrashLoopingPod(pods, a.targetRev); pod != nil {
		a.termination, a.evidencePod = types.PodTerminationWithReason(pod, crash, now), pod
		return a
	}
	if pod, _ := evidence.FirstRunningNotReadyPod(pods, a.targetRev); pod != nil {
		a.termination, a.evidencePod = evidence.ContainersNotReadyTermination(pod, now), pod
		a.notReadyDetail = a.termination.Message
		return a
	}
	if pod := evidence.FirstGateNotFoldedPod(pods, a.targetRev); pod != nil {
		a.termination, a.evidencePod = evidence.GateNotFoldedTermination(pod, now), pod
		a.notReadyDetail = a.termination.Message
	}
	return a
}

// unplaceable reports a rebuild the scheduler could not place within the
// operator's grace. The node exclusions the Instance holds for the
// revision are released before the attempt ends, so the next rebuild
// lands wherever the cluster has room, the excluded node included.
func (a expiredAttempt) unplaceable() bool {
	return a.causePod == nil && a.shortReason == types.WaitingReasonUnschedulable
}

// relocate is the branch for an Instance that can move to another node.
// It records a node exclusion in the audit record (a completed
// AutoRecover entry naming the node and the revision) and reports whether
// it did; the rebuild then renders a required NotIn node term and no retry
// record is charged. Nothing is recorded when the relocation budget is
// spent or the exclusion would leave the pod template unplaceable, and
// the terminal branch runs instead.
func relocate(ctx context.Context, deps types.Deps, input types.ReconcileInput, dd types.DispositionDeps, a expiredAttempt) (bool, error) {
	node := nodeToAvoid(input, dd, a)
	if node == "" {
		return false, nil
	}
	return recordRelocationDirective(ctx, deps, input, dd, a.inst, a.targetRev, node, a.notReadyDetail)
}

// nodeToAvoid names the node the rebuild must avoid, "" when the failure
// gives no reason to charge one: a pod's own workload caused it; the
// attempt crash-loops (a container that keeps exiting does so on any
// node); the migration policy allows no move; no relocation budget is
// configured; the target revision has never served on any Instance (a
// never-ready pod of an unproven revision is the revision's own failure
// until proven otherwise); the retry record holds the revision; or the
// attempt's own pods do not sit on exactly one known node. A single-pod
// surge's serving source shares the Instance index and is not the
// attempt's pod.
func nodeToAvoid(input types.ReconcileInput, dd types.DispositionDeps, a expiredAttempt) string {
	if a.causePod != nil || attemptCrashLooping(a.pods, a.targetRev, a.shortReason) ||
		!migrationModeAllowsRelocation(dd.MigrationMode) || dd.AutoMigrateMaxAttempts <= 0 ||
		!revisionHasServed(input.ObservedState.InstanceStatuses, a.inst, a.targetRev) ||
		revisionHeld(input, a.targetRev) {
		return ""
	}
	return evidence.AttemptSuspectNode(a.pods, a.targetRev)
}

// endTerminal is the last branch: the attempt ends, and the failure
// counts on the target revision's retry record first when a retry policy
// is configured, the cause is not the environment's (a scheduler hold),
// a target revision resolves, and the pod that explains the failure is
// not another revision's leftover. The record is written before the end,
// so a crash between the two re-enters here and the record's dedup keeps
// the count.
func endTerminal(ctx context.Context, deps types.Deps, input types.ReconcileInput, a expiredAttempt) (DeadlineDecision, error) {
	summary, parkedWait := terminalEndSummary, ""
	if chargeRev := terminalChargeRevision(input, a.inst, a.evidencePod, a.shortReason); chargeRev != "" {
		failureReason := countedFailureReason(a.shortReason, a.termination)
		if err := types.RecordUpdateFailureInRetryBlock(ctx, input, chargeRev, failureReason, types.CauseUnattributed); err != nil {
			return DeadlineDecisionTerminal, fmt.Errorf("record retry block for the expired attempt (instance=%d rev=%s): %w", a.inst.Index, chargeRev, err)
		}
		summary, parkedWait = terminalCountedEndSummary, parkedWaitAfterFailure(input, chargeRev, failureReason, types.CauseUnattributed, a.now)
	}
	return endExpiredAttempt(ctx, deps, input, a, DeadlineDecisionTerminal, summary, parkedWait)
}

// endExpiredAttempt writes the end the relocate and terminal branches
// share and warns once per failure class: the same unresolved failure
// re-enters every pass (the end clears the operation, the next pass
// re-opens the same attempt), and inst.LastFailure, read from the pass's
// observation, already names the class when the warning fired before.
// parkedWait is the hold the stopped attempt stands behind, "" when the
// failure was not counted.
func endExpiredAttempt(ctx context.Context, deps types.Deps, input types.ReconcileInput, a expiredAttempt, decision DeadlineDecision, summary, parkedWait string) (DeadlineDecision, error) {
	ended, err := endAttempt(ctx, deps, input, a.inst, a.pods, a.termination, parkedWait)
	if err != nil {
		return decision, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", a.inst.Index, err)
	}
	if !ended {
		return DeadlineDecisionHeldBack, nil
	}
	if input.WarnInstanceFailed != nil && !sameTerminalFailure(a.inst, a.shortReason) {
		input.WarnInstanceFailed(a.inst.Index, "", fmt.Sprintf("%s: %s", a.reason, summary))
	}
	return decision, nil
}

// endAttempt writes the attempt's end in one row write. A counted
// attempt whose own pod set is still alive keeps its operation on the row
// at step Parked, with the hold its next attempt waits on, so the row's
// phase can follow a set the kubelet goes on retrying. An attempt whose
// pods are gone, or whose failure was not counted, has its operation
// cleared. A single-pod surge that took its source out of rotation hands
// it back first, before the row is written, so the Instance serves from
// the pod it had while nothing else serves for it. Either write lands
// only while the fresh row still carries the observed attempt; it reports
// whether the row changed.
func endAttempt(ctx context.Context, deps types.Deps, input types.ReconcileInput, inst types.InstanceStatus, pods []*corev1.Pod, termination *types.InstanceTermination, wait string) (bool, error) {
	if err := workloadops.RestoreSourceRotation(ctx, deps, inst, pods); err != nil {
		return false, fmt.Errorf("return the ended surge's source to rotation (instance=%d): %w", inst.Index, err)
	}
	if wait != "" && attemptParkable(inst, pods) {
		return status.StampFailedParkingAttempt(ctx, input, inst.Index, *inst.Operation, termination, wait)
	}
	return status.StampFailedEndingAttempt(ctx, input, inst.Index, *inst.Operation, termination)
}

// attemptParkable reports whether an ended attempt leaves a pod set the
// row can follow: a recreate or in-place Update, single-pod or gang, with
// a live pod of its own revision. A surge keeps a serving source beside
// its replacement, so its operation is cleared instead.
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

// parkedWaitAfterFailure names the hold a kept attempt waits on once this
// failure is counted: the Held gate when the retry record holds at it,
// else the retry record's backoff. It is read off the same transition the
// record writer applies, because the pass's observation predates the
// write.
func parkedWaitAfterFailure(input types.ReconcileInput, rev, reason string, cause types.FailureCause, now metav1.Time) string {
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

// terminalChargeRevision names the revision a terminal failure counts
// against, "" when it is not counted: no retry policy is configured, the
// environment caused it (a scheduler hold), no target revision resolves,
// or the pod that explains the failure runs a known, different revision,
// a leftover of an earlier attempt that must not charge the revision that
// replaced it.
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

// countedFailureReason is what the counted failure records on the retry
// record: the kubelet waiting reason the escalation classified (the crash
// loop an operator reads on the hold), else the failure record's own
// reason (the pod that never reported ready, or the bare deadline).
func countedFailureReason(shortReason string, termination *types.InstanceTermination) string {
	if types.IsTerminalWaitingReason(shortReason) || termination == nil || termination.Reason == "" {
		return shortReason
	}
	return termination.Reason
}

// sameTerminalFailure reports whether the row's LastFailure already
// records this failure class, so the same unresolved failure warns once.
func sameTerminalFailure(inst types.InstanceStatus, reason string) bool {
	return inst.LastFailure != nil && inst.LastFailure.Reason == reason
}

// attemptCrashLooping reports whether the attempt's failure is a crash
// loop: the escalation classified CrashLoopBackOff, or a live pod of the
// attempt waits in it or runs between two crashes. A container that keeps
// exiting does so on any node, so no node is charged for it.
func attemptCrashLooping(pods []*corev1.Pod, targetRev, shortReason string) bool {
	if shortReason == evidence.ReasonCrashLoop {
		return true
	}
	pod, _ := evidence.FirstCrashLoopingPod(pods, targetRev)
	return pod != nil
}

// migrationModeAllowsRelocation reports whether the migration policy lets
// the controller move an Instance on its own: Auto, or Surge, its other
// spelling. Never and the zero value do not.
func migrationModeAllowsRelocation(mode types.MigrationMode) bool {
	return mode == types.MigrationModeAuto || mode == types.MigrationModeSurge
}

// revisionHasServed reports whether rev has run to Ready on some
// Instance. A row's RunningRevision is written only by a promote, so a
// row carrying rev, a sibling serving it now or the attempt's own row
// from before this attempt, proves the revision works somewhere.
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

// revisionHeld reports whether the retry record holds rev: the record's
// answer that the revision itself fails, so no node is charged for it.
func revisionHeld(input types.ReconcileInput, rev string) bool {
	block := types.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
	return block != nil && block.State == types.RetryBlockHeld
}

// attemptTargetRevision names the revision an attempt converges toward:
// the target its operation records, else the owner's UpdateRevision,
// which a Create with no target recorded renders from.
func attemptTargetRevision(input types.ReconcileInput, inst types.InstanceStatus) string {
	if inst.Operation != nil && inst.Operation.TargetRevision != "" {
		return inst.Operation.TargetRevision
	}
	return input.ObservedState.UpdateRevision
}

// relocationDirective is one node exclusion to record for an Instance whose
// attempt expired: the revision the exclusion binds, the node the rebuild
// avoids, the readiness detail that explains it, and the relocation budget.
type relocationDirective struct {
	component   string
	instance    int32
	revision    string
	fromNode    string
	detail      string
	maxAttempts int32
}

// relocationRecord is a directive taken: the budget slot it takes, its
// request UUID and the time it was written.
type relocationRecord struct {
	relocationDirective
	attempt     int32
	requestUUID string
	at          metav1.Time
}

// recordRelocationDirective records the node exclusion: a completed
// AutoRecover entry in the audit record with the node and the revision,
// the only revision the exclusion binds. The entry is a record, not a
// migration request: the migration work loop never selects it, and the
// rebuild avoids the node through the node term it renders. Three guards
// run first, in order: a pass already wrote this attempt's entry
// (recordedFor), the exclusions would leave the pod templates
// unplaceable (leavesRebuildUnplaceable), the relocation budget is
// spent. A spent budget publishes nothing, because the cap warning fired
// once when the last slot was taken. After the audit record persists,
// the completed Auto migration record is mirrored into the status and
// the events are published.
func recordRelocationDirective(ctx context.Context, deps types.Deps, input types.ReconcileInput, dd types.DispositionDeps, inst types.InstanceStatus, revision, fromNode, detail string) (recorded bool, err error) {
	owner, ownerGVK := input.AuditOwner()
	auditRecord, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), owner)
	if err != nil {
		return false, fmt.Errorf("load audit ledger (instance=%d): %w", inst.Index, err)
	}
	directive := relocationDirective{
		component: string(input.Key.Component), instance: inst.Index,
		revision: revision, fromNode: fromNode, detail: detail, maxAttempts: dd.AutoMigrateMaxAttempts,
	}
	if directive.recordedFor(auditRecord, inst.Operation) {
		return true, nil
	}
	if directive.leavesRebuildUnplaceable(auditRecord, dd) {
		return false, nil
	}
	attempts := audit.CountAutoRecoverAttempts(auditRecord, directive.component, directive.instance)
	if attempts >= directive.maxAttempts {
		return false, nil
	}
	record := relocationRecord{relocationDirective: directive, attempt: attempts + 1, requestUUID: uuid.NewString(), at: metav1.NewTime(input.Now())}
	auditRecord.UpsertEntry(record.entry())
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, owner, ownerGVK, auditRecord); err != nil {
		return false, fmt.Errorf("persist audit ledger (instance=%d): %w", inst.Index, err)
	}
	record.mirror(ctx, input)
	record.publish(deps, input)
	if dd.OnRelocationDirective != nil {
		dd.OnRelocationDirective(directive.component)
	}
	return true, nil
}

// recordedFor reports whether a pass already wrote this attempt's exclusion
// and then lost the operation clear: the newest AutoRecover entry names the
// same node and completed after the operation started. An operation with no
// start time cannot prove the entry postdates it.
func (d relocationDirective) recordedFor(auditRecord *audit.Ledger, operation *types.InstanceOperation) bool {
	if operation == nil || operation.StartedAt.IsZero() {
		return false
	}
	newest := audit.NewestAutoRecoverEntry(auditRecord, d.component, d.instance)
	if newest == nil || newest.FromNode != d.fromNode {
		return false
	}
	completed, ok := newest.CompletedAtTime()
	return ok && completed.After(operation.StartedAt.Time)
}

// leavesRebuildUnplaceable reports whether the exclusions the rebuild would
// render, this node plus the newest maxAttempts-1 nodes recorded for the
// revision, would leave a pod template's required node affinity
// unsatisfiable.
func (d relocationDirective) leavesRebuildUnplaceable(auditRecord *audit.Ledger, dd types.DispositionDeps) bool {
	kept := audit.RecentAutoRecoverExclusions(auditRecord, d.component, d.instance, d.maxAttempts-1)
	exclusions := append(audit.NodesExcludedForRevision(kept, d.revision), d.fromNode)
	return workloadops.WouldExclusionsConflictWithNodeAffinity(dd.PodSpec, exclusions) ||
		workloadops.WouldExclusionsConflictWithNodeAffinity(dd.WorkerPodSpec, exclusions)
}

// entry is the completed AutoRecover entry the audit record keeps for the
// directive.
func (r relocationRecord) entry() audit.Entry {
	at := r.at.UTC().Format(time.RFC3339)
	return audit.Entry{
		RequestUUID:    r.requestUUID,
		Component:      r.component,
		SourceInstance: r.instance,
		Phase:          audit.PhaseCompleted,
		Reason:         audit.ReasonAutoRecover,
		Outcome:        audit.OutcomeRelocateRecreate,
		FromNode:       r.fromNode,
		Revision:       r.revision,
		StartedAt:      at,
		CompletedAt:    at,
	}
}

// mirror writes the completed Auto migration record into the status for
// visibility. A failed mirror is logged and never fails the decision. The
// record's Deadline is required by the schema and carries no meaning here.
func (r relocationRecord) mirror(ctx context.Context, input types.ReconcileInput) {
	if input.AppendMigration == nil {
		return
	}
	completedAt := r.at
	err := input.AppendMigration(ctx, types.MigrationRecord{
		RequestUUID:    r.requestUUID,
		Trigger:        types.MigrationTriggerAuto,
		SourceInstance: r.instance,
		FromNode:       r.fromNode,
		Phase:          types.MigrationPhaseRelocated,
		Attempt:        r.attempt,
		Reason:         audit.ReasonAutoRecover,
		Message:        relocationRecordMessage(r.fromNode, r.detail),
		StartedAt:      r.at,
		Deadline:       r.at,
		CompletedAt:    &completedAt,
	})
	if err != nil {
		logf.FromContext(ctx).V(1).Info("relocation status-record mirror failed; ledger remains authoritative",
			"uuid", r.requestUUID, "instance", r.instance, "error", err.Error())
	}
}

// publish emits the directive's event and, on the directive that takes the
// last budget slot, the cap warning, once.
func (r relocationRecord) publish(deps types.Deps, input types.ReconcileInput) {
	target := types.EventTarget(input)
	types.RecordNormal(deps.Recorder, target, types.EventReasonAutoMigrationTriggered,
		"OMENative component=%s instance=%d relocation directive recorded: attempt %d/%d, rebuild steered off node %s (uuid=%s)",
		r.component, r.instance, r.attempt, r.maxAttempts, r.fromNode, r.requestUUID)
	if r.attempt == r.maxAttempts {
		types.RecordWarning(deps.Recorder, target, types.EventReasonAutoMigrationCapReached,
			"OMENative component=%s instance=%d relocation budget exhausted (%d/%d attempts); further expiries dispose terminal without relocation",
			r.component, r.instance, r.attempt, r.maxAttempts)
	}
}

// releaseNodeExclusions removes the node exclusions this Instance holds
// for revision, persists the audit record ahead of the status write and
// announces the release once with the nodes it frees. Nothing held means
// no write and no event. A persist conflict is returned as the error the
// reconcile retries on.
func releaseNodeExclusions(ctx context.Context, deps types.Deps, input types.ReconcileInput, inst types.InstanceStatus, revision string) error {
	owner, ownerGVK := input.AuditOwner()
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
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, owner, ownerGVK, ledger); err != nil {
		return fmt.Errorf("persist audit ledger (instance=%d): %w", inst.Index, err)
	}
	types.RecordNormal(deps.Recorder, types.EventTarget(input), types.EventReasonNodeExclusionReleased,
		"OMENative component=%s instance=%d node exclusion released for revision %s: no other node had room for the rebuild; it may land on %s again",
		component, inst.Index, revision, strings.Join(released, ", "))
	return nil
}

// relocationRecordMessage is the summary on the mirrored migration
// record: the node the rebuild avoids, and what went wrong there when a
// pod's readiness said so.
func relocationRecordMessage(fromNode, detail string) string {
	base := fmt.Sprintf("relocation directive recorded; rebuild steered off node %s", fromNode)
	if detail == "" {
		return base
	}
	return base + " (" + detail + ")"
}

// takesDeadlineDecision reports whether the row's in-flight attempt is
// ended by DecideExpiredAttempt rather than by the plain Failed write
// that keeps the operation. A Create of any pod count and a recreate or
// in-place Update, single-pod or gang, are: left Failed with their
// operation they would be driven again at the same target every pass,
// with no failure counted and no hold reported. A gang surge is not: its
// Failed row keeps the operation so the gang abandon path ends the pair.
// A Restart or a Migrate is not: each has its own authority.
func takesDeadlineDecision(s *types.InstanceStatus) bool {
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
