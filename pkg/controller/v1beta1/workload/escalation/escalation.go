// escalation.go — the terminal-failure escalation pass. Reconcile runs
// it once at the end of every eligible non-teardown, non-paused
// reconcile: per Instance it consumes the snapshot's failure evidence
// (stuck pod / elapsed Operation deadline) and decides the Failed
// transition through the shared disposition classification
// (disposition.go). The Failed DECISION lives here, next to the rest of
// the transition decisions; the adapter-side status aggregator owns
// only counters + conditions and never writes a transition field.
//
// The holds this pass reads were recorded at the top of the reconcile
// (holds.Run); nothing here writes one.
package escalation

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// PassInput is everything the escalation pass reads.
type PassInput struct {
	Deps  types.Deps
	Input types.ReconcileInput
	Plan  types.ComponentPlan

	// Target is the reconcile's roll target; nil when the Component has
	// no pod spec (the Create-op RetryBlock fallback then stays empty).
	Target *appsv1.ControllerRevision

	// Pods is the reconcile's one memoized cached pod observation,
	// bucketed by Instance index — the same read the hold pass judged on.
	Pods func(ctx context.Context) (map[int32][]*corev1.Pod, error)

	// Excluded marks the deferred scale-down victims: they receive no
	// lifecycle mutation before the wave admits them.
	Excluded map[int32]struct{}

	// Held is what the hold pass recorded this reconcile.
	Held holds.Result
}

// Run walks every observed InstanceStatus and escalates terminal
// failures from the pass's evidence. Three paths converge
// on Phase=Failed, and they don't fight — whichever fires first wins and
// the other no-ops on the already-Failed Instance:
//
//   - FAST path: a pod stuck in a terminal kubelet waiting state
//     (CrashLoopBackOff, ImagePullBackOff, ...) past input.StuckPodGrace
//     fails the Instance without waiting for the (default 30 min)
//     per-Instance Operation deadline. Qualification is
//     ShouldCheckForStuckPods: a transient-phase Instance with an
//     in-flight Operation, or the wedged-pod recovery shape (a pod whose
//     revision-hash label disagrees with CurrentRevision). A zero or
//     negative grace disables this path (the snapshot reports no stuck
//     evidence).
//
//   - BROAD path: any operation that never converges — a perpetually-
//     Pending gang, a run-then-crash loop the kubelet never parks in a
//     terminal waiting reason — fails once its Operation.Deadline
//     (StartedAt + InstanceReadyTimeout, stamped by the per-op writers)
//     elapses. Instances whose blamed pods (own bucket, plus the
//     SurgeIndex bucket for a gang-surge source) are held by an
//     admission scheduling gate (e.g. Kueue) are excluded, as are those
//     whose operation is parked on the capacity-blocked Waiting token
//     (a create admission refused for lack of quota leaves no pod to
//     carry a gate): the deadline-parking step (ReconcileGatedDeadlines)
//     owns their clock, and a queued wait must not count against the
//     timeout.
//
//   - SCHEDULER-HOLD path: a pod the scheduler reports it cannot place
//     is queued on cluster capacity, not stuck, so it parks the clock
//     through the Waiting token rather than counting against it. Only
//     input.UnschedulableGrace ends that wait, and it ends it as an
//     ENVIRONMENT cause — the evidence names the scheduler's message and
//     no revision is held. Unconfigured, the hold simply lasts.
//
//   - GANG path: what only the Instance's PodGroup can say, since a gang
//     is admitted as a unit. A deterministic name held by another
//     controller ends the attempt: nothing this controller does frees
//     it. It is an ENVIRONMENT cause and holds no revision. A name still
//     being collected parks the clock instead and never escalates — the
//     gang is announced again once the old object is gone — and a group
//     the gang scheduler has failed ends nothing at all, because the
//     PodGroup pass rebuilds it. Rows whose outcome another state
//     machine owns (Delete, Migrate), rows with no attempt in flight,
//     and a gang-surge SOURCE (whose attempt lives under the surge
//     index) are left alone on every gang reading. Whether a gang can be
//     PLACED is not read here — that arrives on the member pods and
//     takes the SCHEDULER-HOLD path above.
//
// The FAST and BROAD paths skip operations whose state machine owns its
// own terminal handling:
//
//   - Migrate (either pair side): timeout authority is the owner's
//     status.migrations record, consumed by the dispatcher's
//     migration-expiry pass. Stamping here would mark a healthy serving
//     source Failed while the record keeps driving the pair. The
//     scheduler hold still runs for them — it is scoped to the row whose
//     OWN pod is unplaceable, so it can only reach a replacement.
//   - Delete: DeleteBatch owns progress, stuck-Terminating escalation,
//     and completion. Its durable ownership must survive an elapsed
//     generic operation deadline, including when the index is planned
//     again while deletion finishes. The SCHEDULER-HOLD path skips it
//     as well: a paced teardown of a never-placed pod would otherwise
//     be failed out from under the batch still driving it.
//
// FAILED-WHILE-SERVING GUARD: an Instance whose blamed pod set is fully
// healthy — every live (non-deleting) pod ContainersReady AND in the
// serving rotation, at the desired count — is never escalated, whatever
// the evidence says. Stamping Phase=Failed over a serving workload is a
// status lie (the pods are fine; only the bookkeeping is stale), and the
// coordination layer would amplify it into a group-level failure. Such
// an Instance is skipped untouched.
//
// Instances with a disposable in-flight attempt (single-pod-updateable
// Create/Update Operations — see disposableAttempt) route through
// DisposeExpiredAttempt; gang Update attempts keep the
// Failed-preserving-Operation stamp (the gang abandon path consumes the
// continuation), and everything else takes the plain stamp for its path.
//
// WRITE BATCHING: the plain Failed stamps are standalone — escalation is
// the reconcile's final pass and nothing later depends on them being
// persisted first — so they are coalesced into ONE batched status write
// via a failureStampBuffer. Disposition routes flush the buffer first
// and keep their immediate per-call writes: their ordering is
// write-ahead (RetryBlock upsert before the op-clear, ledger persist
// before the status mirror) and must not be deferred.
//
// Adapter-agnostic: writes Phase=Failed via
// input.ApplyInstanceMutations / input.MutateInstance, emits the
// operator-facing event via input.WarnInstanceFailed.
func Run(ctx context.Context, in PassInput) error {
	deps, input, plan, target := in.Deps, in.Input, in.Plan, in.Target
	excluded, heldRows := in.Excluded, in.Held
	byIdx, err := in.Pods(ctx)
	if err != nil {
		return fmt.Errorf("workload.Reconcile: list pods for escalation pass (component=%s): %w", plan.Component, err)
	}
	// The Create-op disposition resolves its RetryBlock target from
	// ObservedState.UpdateRevision; on a first rollout the aggregator may
	// not have stamped it yet, so fall back to this reconcile's target
	// revision. Empty-only: a non-empty stamp stays authoritative (during
	// a canary rollback the roll target diverges from the spec target the
	// stamp names, and the block must hold the spec target).
	if input.ObservedState.UpdateRevision == "" && target != nil {
		input.ObservedState.UpdateRevision = target.Name
	}
	now := input.Now()
	desiredByIdx := status.DesiredPodCountByInstance(plan)
	currentHash := query.RevisionFromName(input.ObservedState.CurrentRevision).Hash()
	disposition := input.Disposition
	disposition.MigrationMode = plan.MigrationMode
	stamps := &failureStampBuffer{input: input}
	for _, row := range input.ObservedState.InstanceStatuses {
		if _, skip := excluded[row.Index]; skip {
			continue
		}
		// Idempotent: skip an already-Failed Instance so the
		// operator-facing event fires exactly once per escalation. Its
		// recorded evidence still gets one refresh, which takes no edge.
		if row.Phase == types.InstancePhaseFailed {
			rowEvidence := evidenceFor(input.ObservedState.InstanceStatuses, byIdx, row.Index, now, input.StuckPodGrace)
			if t := refreshedFailureEvidence(row, rowEvidence); t != nil {
				stamps.add(row.Index, status.RecordRefreshedFailure(t, DeadlineExceededReason), nil)
			}
			continue
		}
		rowEvidence := evidenceFor(input.ObservedState.InstanceStatuses, byIdx, row.Index, now, input.StuckPodGrace)
		pods := evidence.PodsForStuckCheck(row, byIdx)
		desired := status.DesiredFor(desiredByIdx, row.Index, row.PodCount)
		gang := types.GangReadingFor(input, row)
		// The hold pass already recorded or released this row's tokens.
		// What is owed here is the repair each wait ends with — the
		// scheduler's grace, the gang's terminal verdict — and which
		// authority owns the row is what says whether one is owed at all.
		held := heldRows.Holding(row.Index, types.WaitingReasonUnschedulable)
		gangHeld := heldRows.Holding(row.Index, types.WaitingReasonPodGroupTerminating)
		// The hold arms above judge on pod health alone, so a row whose
		// pods are healthy still hands a stale token back whatever it is
		// reporting about its rotation. The FAILED-WHILE-SERVING exemption
		// is the stricter question — is this Instance actually capacity —
		// and a surge that has reported its source out of rotation is not.
		if podSetIsCapacity(row, pods, desired) {
			continue
		}
		if reason := gangTerminalReason(gang.State); reason != "" && gangVerdictActionable(row.Operation) {
			escalateGangFailure(input, stamps, row, gang, reason, now)
			continue
		}
		if gangHeld {
			// Parked until the object at the gang's name is collected,
			// which resolves without anyone acting. A genuinely wedged
			// sibling pod still escalates through the fast path below.
			if rowEvidence.StuckPod == nil {
				continue
			}
		}
		if held {
			if !unschedulableGraceElapsed(rowEvidence.UnschedulableSince, now, input.UnschedulableGrace) {
				// Parked. A genuinely wedged sibling pod still escalates
				// through the fast path below; the hold itself does not.
				if rowEvidence.StuckPod == nil {
					continue
				}
			} else {
				if err := escalateSchedulerHold(ctx, deps, input, disposition, stamps, row, pods, desired, rowEvidence, now); err != nil {
					return fmt.Errorf("escalate scheduler hold (instance=%d): %w", row.Index, err)
				}
				continue
			}
		}
		// Whether either clock may end this row's step is the ownership
		// table's answer, per state: a migration pair is the record's,
		// which fails both rows through itself, and a teardown is
		// DeleteBatch's, which paces a removal past any grace and warns
		// on its own overdue drain.
		stuckInterrupts := types.Interruptible(&row, types.EventStuckPodGrace)
		deadlineInterrupts := types.Interruptible(&row, types.EventOperationDeadline)
		if !stuckInterrupts && !deadlineInterrupts {
			continue
		}
		// An attempt an operator is holding is not stalled, it was told
		// to wait. Neither path may end it: the deadline is parked for
		// the length of the hold, and a pod parked in a terminal waiting
		// state is waiting on the unpause rather than on a verdict.
		if types.OperationPaused(row.Operation) {
			continue
		}
		if rowEvidence.StuckPod == nil && !rowEvidence.DeadlinePassed {
			continue
		}
		// FAST path.
		if stuckInterrupts && rowEvidence.StuckPod != nil && ShouldCheckForStuckPods(&row, pods, currentHash, query.LabelRevisionHash) {
			if disposableAttempt(&row, desired) {
				// The disposition's writes are write-ahead-ordered; land
				// the pending plain stamps first so the overall write +
				// event order matches the unbatched pass.
				if err := stamps.flush(ctx); err != nil {
					return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, err)
				}
				dispositionPods := pods
				if singlePodSurgeAttempt(&row, desired) {
					// A single-pod surge shares its Instance index with the
					// serving source. Relocation evidence belongs to the exact
					// stuck target, not the source-plus-target node set.
					dispositionPods = []*corev1.Pod{rowEvidence.StuckPod}
				}
				if _, err := DisposeExpiredAttempt(ctx, deps, input, disposition, row, dispositionPods, rowEvidence.StuckReason); err != nil {
					return fmt.Errorf("dispose stuck attempt (instance=%d): %w", row.Index, err)
				}
				continue
			}
			// Preserve the stuck pod's diagnostics into LastFailure alongside
			// the Phase=Failed flip — the escalation is followed by a recreate
			// (or operator teardown) that deletes the wedged pod, so this is
			// the surviving trace. StuckReason is the live waiting-state reason
			// the classifier matched; PodTerminationWithReason fills in
			// container / exit-code detail when present and falls back to the
			// bare reason.
			termination := types.PodTerminationWithReason(rowEvidence.StuckPod, rowEvidence.StuckReason, metav1.NewTime(now))
			idx, podName, reason := row.Index, rowEvidence.StuckPod.Name, rowEvidence.StuckReason
			stamps.add(idx, status.StampFailedOnStuckPod(termination), func() {
				input.WarnInstanceFailed(idx, podName, reason)
			})
			continue
		}
		// BROAD path.
		if !deadlineInterrupts || !rowEvidence.DeadlinePassed {
			continue
		}
		// Admission-gated pods are queued, not stuck: the parking step
		// zeroes their deadline; a gate-enter observed before the park
		// lands must not expire either. A gang-surge source's attempt
		// pods live in its SurgeIndex bucket, so check that bucket too.
		if anyPodAdmissionGated(byIdx[row.Index]) {
			continue
		}
		if row.Operation != nil && row.Operation.SurgeIndex != nil && anyPodAdmissionGated(byIdx[*row.Operation.SurgeIndex]) {
			continue
		}
		// Same rationale for the waits no pod can carry a gate for: a
		// create admission refused for lack of quota leaves no pod at
		// all, and a scheduler hold records itself on the Operation. Both
		// live on the surge row for a gang surge, whose source this check
		// follows the same way the gate check above follows the
		// SurgeIndex bucket. The parking step zeroes that deadline; one
		// stamped before the park landed must not expire either.
		if instanceHeldExternally(&row, input.ObservedState.InstanceStatuses) {
			continue
		}
		op := row.Operation
		if disposableAttempt(&row, desired) {
			if err := stamps.flush(ctx); err != nil {
				return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, err)
			}
			if _, err := DisposeExpiredAttempt(ctx, deps, input, disposition, row, pods, deadlineFailureMessage(op)); err != nil {
				return fmt.Errorf("dispose expired attempt (instance=%d): %w", row.Index, err)
			}
			continue
		}
		idx := row.Index
		// The teardown that follows deletes the blamed pods, so revision-scoped
		// evidence must survive on LastFailure: a workload-caused waiting reason
		// — or a pod that ran every container yet never reported ready — names
		// the cause more precisely than the elapsed timeout, and it is what the
		// gang abandon charges the revision's retry ladder on.
		termination := deadlineTermination(now, op)
		if pod, reason := evidence.FirstWorkloadCausedPod(pods); pod != nil {
			termination = types.PodTerminationWithReason(pod, reason, metav1.NewTime(now))
		} else if pod, _ := evidence.FirstRunningNotReadyPod(pods, attemptTargetRevision(input, row)); pod != nil {
			// Evidence only: a pod that runs yet never reports ready leaves
			// no container-level diagnostics, so without this the record is
			// the bare elapsed timeout. It does not change who is blamed —
			// the gang abandon reads the reason and this one is not in the
			// workload-caused set.
			termination = evidence.ContainersNotReadyTermination(pod, metav1.NewTime(now))
		} else if pod := evidence.FirstGateNotFoldedPod(pods, attemptTargetRevision(input, row)); pod != nil {
			// Evidence only, same standing: a pod whose probes passed but
			// whose readiness gate never folded into Ready is invisible to
			// its Service, and that is what the record must say instead of
			// the bare elapsed timeout.
			termination = evidence.GateNotFoldedTermination(pod, metav1.NewTime(now))
		}
		stamps.add(idx, status.StampFailedKeepingOperation(termination), func() {
			input.WarnInstanceFailed(idx, "", deadlineFailureMessage(op))
		})
	}
	if err := stamps.flush(ctx); err != nil {
		return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, err)
	}
	return nil
}

// InstanceEvidence is the per-instance observed failure evidence for one
// reconcile: the first workload-caused/terminal stuck pod (if any) and
// whether the in-flight Operation's deadline has elapsed. It is EVIDENCE
// ONLY — building it never writes Phase; deciding the Failed transition
// from it is the pass's job.
type InstanceEvidence struct {
	// StuckPod is the first live, non-deleting pod parked in a
	// workload-caused/terminal waiting state past the grace window
	// (evidence.FirstStuckPodForInstance). Nil when no pod is stuck.
	StuckPod *corev1.Pod
	// StuckReason is the kubelet waiting reason of StuckPod ("" when none).
	StuckReason string
	// DeadlinePassed reports whether the instance is in a transient phase
	// with an in-flight Operation whose Deadline lies in the past.
	DeadlinePassed bool
	// Unschedulable is the first live pod of the instance's OWN bucket
	// the scheduler reports it cannot place. Nil when every pod is
	// scheduled, gone, or deleting.
	//
	// Own-bucket, unlike StuckPod: a hold names the row that has to wait,
	// and attributing a replacement's lack of placement to the serving
	// source would both misreport the source and, past the grace, fail
	// it. A sibling reaches the hold through the deadline parking's
	// SurgeIndex hop instead.
	//
	// Independent of the stuck-pod grace: the hold is recorded from the
	// first observation and only its ESCALATION waits out a window.
	Unschedulable *corev1.Pod
	// UnschedulableMessage is the scheduler's own explanation of which
	// constraint had no placement ("" when it gave none).
	UnschedulableMessage string
	// UnschedulableSince is when PodScheduled last transitioned — the
	// start of the hold, which the escalation window is measured from.
	UnschedulableSince metav1.Time
}

// evidenceFor returns the per-instance failure evidence for idx, derived
// from the cached-read pods + the instance's Operation deadline. Evidence
// only — no Phase write. Stuck detection attributes pods the same way the
// pass blames them (evidence.PodsForStuckCheck: a gang Update surge is
// inspected through its replacement gang, a Migrate pair through
// own-plus-sibling). A non-positive grace disables stuck-pod evidence
// entirely (fast escalation off; the deadline backstop and the scheduler
// hold still report). now/grace are supplied by the caller (clock seam).
func evidenceFor(insts []types.InstanceStatus, byIdx map[int32][]*corev1.Pod, idx int32, now time.Time, grace time.Duration) InstanceEvidence {
	var ev InstanceEvidence
	var inst *types.InstanceStatus
	for i := range insts {
		if insts[i].Index == idx {
			inst = &insts[i]
			break
		}
	}
	if inst != nil {
		ev.DeadlinePassed = operationDeadlinePassed(inst, now)
	}
	ev.Unschedulable, ev.UnschedulableMessage, ev.UnschedulableSince = types.FirstUnschedulablePod(byIdx[idx])
	pods := byIdx[idx]
	if inst != nil {
		pods = evidence.PodsForStuckCheck(*inst, byIdx)
	}
	if grace <= 0 {
		return ev
	}
	if pod, reason := evidence.FirstStuckPodForInstance(pods, now, grace); pod != nil {
		ev.StuckPod = pod
		ev.StuckReason = reason
	}
	return ev
}

func singlePodSurgeAttempt(row *types.InstanceStatus, desiredPods int32) bool {
	return row != nil && desiredPods == 1 && row.Operation != nil &&
		row.Operation.Type == types.InstanceOperationUpdate &&
		row.Operation.Step == types.UpdateStepSurge && row.Operation.SurgeIndex == nil
}

// failureStampBuffer coalesces the escalation pass's plain Failed
// stamps into one batched status write. Buffer only standalone stamps:
// mutations nothing later in the reconcile depends on being persisted
// first. Each buffered warn fires only after its stamp's write
// succeeded, matching the immediate path (a failed write emits nothing;
// the evidence re-escalates next reconcile). A nil warn is a mutation
// that carries no operator-facing event of its own.
type failureStampBuffer struct {
	input types.ReconcileInput
	muts  []types.InstanceMutation
	warns []func()
}

func (b *failureStampBuffer) add(idx int32, mutate func(*types.InstanceStatus) bool, warn func()) {
	b.muts = append(b.muts, types.InstanceMutation{Index: idx, Mutate: mutate})
	b.warns = append(b.warns, warn)
}

// flush persists the buffered stamps — one batched write when the
// adapter provides ApplyInstanceMutations, one MutateInstance call per
// stamp otherwise — then fires the deferred warnings. Empty buffer =
// zero writes. The buffer resets whichever path ran.
func (b *failureStampBuffer) flush(ctx context.Context) error {
	if len(b.muts) == 0 {
		return nil
	}
	if b.input.ApplyInstanceMutations != nil {
		if err := b.input.ApplyInstanceMutations(ctx, b.muts); err != nil {
			return err
		}
		for _, warn := range b.warns {
			if warn != nil {
				warn()
			}
		}
	} else {
		for i, m := range b.muts {
			if err := b.input.MutateInstance(ctx, m.Index, m.Mutate); err != nil {
				return err
			}
			if b.warns[i] != nil {
				b.warns[i]()
			}
		}
	}
	b.muts, b.warns = nil, nil
	return nil
}

// podSetIsCapacity reports whether the Instance is healthy capacity,
// which is what buys it the FAILED-WHILE-SERVING exemption.
//
// Pod conditions alone do not settle that during a surge. Until the
// drain step the source is what holds the Instance's traffic, and a
// surge whose source has been taken out of rotation by someone else says
// so on its operation: the pods can read fully serving while nothing is
// in rotation, and a replacement wedged in that state must escalate
// rather than be exempted by a source that is not serving.
func podSetIsCapacity(row types.InstanceStatus, pods []*corev1.Pod, desired int32) bool {
	return podSetFullyServing(pods, desired) && !types.OperationSourceUnrouted(row.Operation)
}

// podSetFullyServing is query.PodSetFullyServing under the name the
// escalation pass's guards read by.
func podSetFullyServing(pods []*corev1.Pod, desired int32) bool {
	return query.PodSetFullyServing(pods, desired)
}

// anyPodAdmissionGated reports whether any pod still carries an
// admission scheduling gate (queued by an external admission authority,
// e.g. Kueue).
func anyPodAdmissionGated(pods []*corev1.Pod) bool {
	for _, p := range pods {
		if types.PodAdmissionGated(p) {
			return true
		}
	}
	return false
}

// HasWedgedPodAgainstCurrent reports whether any pod carries a
// revision-hash label disagreeing with currentHash — i.e., a pod
// created for a revision the controller doesn't yet (or no longer)
// recognize as current.
//
// Pods missing the label are skipped (no signal). Empty currentHash
// means no revision has been promoted yet — treat every pod hash as
// matching so the wedged-recovery branch doesn't over-fire on the
// initial Create.
//
// Used by the wedged-pod recovery branch of the escalator: catches
// the post-surge shape where the state machine has been reset out of
// the transient phase set but a real wedged surge pod still exists.
func HasWedgedPodAgainstCurrent(pods []*corev1.Pod, currentHash, revisionHashLabel string) bool {
	if currentHash == "" || revisionHashLabel == "" {
		return false
	}
	current := query.RevisionFromHash(currentHash)
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		hash := pod.Labels[revisionHashLabel]
		if hash == "" {
			continue
		}
		if !query.RevisionFromHash(hash).Same(current) {
			return true
		}
	}
	return false
}

// ShouldCheckForStuckPods reports whether s qualifies for the
// fast-failure check. Two paths qualify:
//
//   - Transient-phase path: an Instance with an in-flight Operation in
//     the {Creating, Updating, Restarting, Migrating} set.
//
//   - Wedged-pod recovery path: an Instance with at least one pod
//     whose revision-hash label disagrees with currentHash, regardless
//     of Phase / Operation. Catches the wedged-state recovery case
//     where the controller's per-Instance bookkeeping has been reset
//     while a real stuck pod still exists.
//
// Skips Failed (already done) and Deleting (the scale-down pass owns its own
// deadline). Empty currentHash suppresses the wedged branch (no
// rollout has ever promoted → over-firing risk during initial Create).
func ShouldCheckForStuckPods(s *types.InstanceStatus, pods []*corev1.Pod, currentHash, revisionHashLabel string) bool {
	if s == nil {
		return false
	}
	if s.Phase == types.InstancePhaseFailed || s.Phase == types.InstancePhaseDeleting {
		return false
	}
	if s.Operation != nil {
		switch s.Phase {
		case types.InstancePhaseCreating,
			types.InstancePhaseUpdating,
			types.InstancePhaseRestarting,
			types.InstancePhaseMigrating:
			return true
		}
	}
	return HasWedgedPodAgainstCurrent(pods, currentHash, revisionHashLabel)
}
