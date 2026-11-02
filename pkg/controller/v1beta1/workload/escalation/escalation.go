// escalation.go — the pass that ends the attempts that will not finish.
// Reconcile runs it once at the end of every pass that is neither a
// teardown nor paused. Per Instance row it reads the pass's failure
// reading (a pod held in a terminal kubelet waiting reason past the
// stuck-pod grace, an operation past its deadline) beside the holds the
// hold pass recorded, and decides the Failed write: a Create, recreate or
// in-place attempt takes the deadline decision (deadline_decision.go),
// every other attempt a plain Failed write with its operation kept. The
// Failed decision lives here beside the other transition decisions; the
// adapter's status aggregator owns counters and conditions and never
// writes a transition field.
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
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
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

	// Excluded marks the leaving Instances the scale-down has not admitted
	// yet: they receive no lifecycle mutation before the wave admits them.
	Excluded map[int32]struct{}

	// Held is what the hold pass recorded this reconcile.
	Held holds.Result
}

// Run walks every Instance row at the end of the pass and ends the attempts
// that will not finish. Four readings end an attempt, and they do not
// compete: the first to write wins, and the others skip a row that already
// reads Failed.
//
//   - A pod of the attempt held in a terminal kubelet waiting reason
//     (CrashLoopBackOff, ImagePullBackOff, ...) for longer than
//     input.StuckPodGrace. A non-positive grace turns this reading off.
//   - The operation's deadline elapsed (StartedAt + InstanceReadyTimeout).
//     A pod behind an admission gate, or an operation waiting on quota or
//     the scheduler, has its clock zeroed by the deadline step instead, and
//     a deadline written before that zeroing landed does not count.
//   - An Unschedulable hold older than input.UnschedulableGrace, or than
//     the shorter input.GangDomainFullGrace when the gang is held to a
//     topology domain that cannot take it. The hold's age runs from the
//     attempt's placement wait when that began before the pod's own wait
//     (schedulerHoldSince). The attempt ends as the
//     environment's failure: the scheduler's message is recorded and no
//     revision is charged. With no grace set, the hold lasts.
//   - A PodGroup name another controller owns. The attempt ends at once as
//     the environment's failure; a name still being collected only holds
//     the row, and a group the gang scheduler failed ends nothing, because
//     the PodGroup pass rebuilds it.
//
// What a row's owner permits comes first. The ownership table says whether
// either clock may end the row's state: a migration pair ends through its
// record and a teardown through the scale-down wave, so neither is written
// here. A paused attempt waits for the unpause. A pod set fully in rotation
// at the desired count is never written Failed, whatever the reading says;
// a surge whose source left rotation is not such a set. A Create, recreate
// or in-place attempt (takesDeadlineDecision) takes the deadline decision;
// every other attempt keeps its operation under the Failed write so the
// pass that owns it, a gang surge's end or a repair, can finish.
//
// The plain Failed writes of one pass land in one batched status write, the
// failureStampBuffer; nothing later in the reconcile depends on them. A
// deadline decision flushes the buffer first, because its own writes keep
// an order of their own: the retry record before the operation is cleared,
// the audit record before the status. Writes go through
// input.ApplyInstanceMutations or input.MutateInstance, the operator-facing
// event through input.WarnInstanceFailed.
//
// The steps run per row in the order rowEscalation.run lists them.
func Run(ctx context.Context, in PassInput) error {
	podsByInstance, err := in.Pods(ctx)
	if err != nil {
		return fmt.Errorf("workload.Reconcile: list pods for escalation pass (component=%s): %w", in.Plan.Component, err)
	}
	// The deadline decision of a Create charges the retry record of
	// ObservedState.UpdateRevision. On a first rollout the status aggregator
	// may not have written it yet, so this pass's roll target stands in. A
	// written value stays: during a canary rollback the roll target is not
	// the spec target the value names, and the record must hold the spec
	// target. The write lands on the pass's own copy of the input.
	if in.Input.ObservedState.UpdateRevision == "" && in.Target != nil {
		in.Input.ObservedState.UpdateRevision = in.Target.Name
	}
	pass := newEscalationPass(in, podsByInstance)
	for _, row := range pass.input.ObservedState.InstanceStatuses {
		if _, deferred := pass.excluded[row.Index]; deferred {
			continue
		}
		r := pass.readRow(row)
		if err := r.run(ctx); err != nil {
			return err
		}
	}
	return pass.flushFailedWrites(ctx)
}

// escalationPass is what one run of the pass reads once: the inputs, the
// pods of every Instance, the holds the hold pass recorded, the clock, the
// planned pod count per Instance, the current revision's hash, the
// deadline decision's inputs, and the buffer of plain Failed writes.
type escalationPass struct {
	deps  types.Deps
	input types.ReconcileInput
	plan  types.ComponentPlan
	// podsByInstance is the reconcile's one cached pod observation by
	// Instance index, the same read the hold pass used.
	podsByInstance map[int32][]*corev1.Pod
	// excluded holds the Instances a deferred scale-down owns; they take no
	// lifecycle write before the wave admits them.
	excluded map[int32]struct{}
	held     holds.Result
	now      time.Time
	// desiredByInstance is the planned pod count per Instance index.
	desiredByInstance map[int32]int32
	// currentRevisionHash is the hash of the Component's current revision;
	// a pod labelled with another hash belongs to a roll left behind.
	currentRevisionHash string
	deadlineDeps        types.DispositionDeps
	// failedWrites collects the pass's plain Failed writes into one batched
	// status write; a deadline decision flushes it before its own writes.
	failedWrites *failureStampBuffer
}

// newEscalationPass reads what every row of the pass shares. The input is
// the pass's own copy, with the update revision already settled by Run.
func newEscalationPass(in PassInput, podsByInstance map[int32][]*corev1.Pod) *escalationPass {
	input := in.Input
	deadlineDeps := input.Disposition
	deadlineDeps.MigrationMode = in.Plan.MigrationMode
	return &escalationPass{
		deps:                in.Deps,
		input:               input,
		plan:                in.Plan,
		podsByInstance:      podsByInstance,
		excluded:            in.Excluded,
		held:                in.Held,
		now:                 input.Now(),
		desiredByInstance:   status.DesiredPodCountByInstance(in.Plan),
		currentRevisionHash: query.RevisionFromName(input.ObservedState.CurrentRevision).Hash(),
		deadlineDeps:        deadlineDeps,
		failedWrites:        &failureStampBuffer{input: input},
	}
}

// flushFailedWrites lands the buffered plain Failed writes and fires the
// warnings of the ones that committed.
func (p *escalationPass) flushFailedWrites(ctx context.Context) error {
	if err := p.failedWrites.flush(ctx); err != nil {
		return fmt.Errorf("flush escalation stamps (component=%s): %w", p.plan.Component, err)
	}
	return nil
}

// rowEscalation is one Instance row as the pass reads it: the row, the
// failure reading of its attempt, the pods that reading is taken over, the
// planned pod count, the PodGroup reading, the attempt's target revision,
// the two holds the pass answers, and whether the ownership table lets the
// stuck-pod grace or the deadline end the row's state.
type rowEscalation struct {
	pass *escalationPass
	row  types.InstanceStatus
	// reading is the attempt's failure reading: the first pod held in a
	// terminal waiting reason past the grace, whether the deadline elapsed,
	// and the pod the scheduler could not place.
	reading InstanceEvidence
	// pods are the pods the attempt's reading is taken over: the row's own,
	// a gang surge's replacement gang, a migration pair's own and sibling.
	pods        []*corev1.Pod
	desiredPods int32
	gang        types.GangObservation
	// attemptRevision is the revision the attempt converges on.
	attemptRevision string
	// unschedulableHold and podGroupHold are the two waits the hold pass
	// recorded that this pass answers.
	unschedulableHold bool
	podGroupHold      bool
	// graceMayEnd and deadlineMayEnd are the ownership table's answers:
	// whether the stuck-pod grace, or the operation deadline, may end the
	// row's state at all.
	graceMayEnd    bool
	deadlineMayEnd bool
}

// readRow reads one row once, before any step runs.
func (p *escalationPass) readRow(row types.InstanceStatus) rowEscalation {
	attemptRevision := attemptTargetRevision(p.input, row)
	r := rowEscalation{
		pass:              p,
		row:               row,
		reading:           evidenceFor(&p.input.ObservedState, p.podsByInstance, row.Index, p.now, p.input.StuckPodGrace, attemptRevision),
		pods:              evidence.PodsForStuckCheck(row, p.podsByInstance),
		desiredPods:       status.DesiredFor(p.desiredByInstance, row.Index, row.PodCount),
		gang:              types.GangReadingFor(p.input, row),
		attemptRevision:   attemptRevision,
		unschedulableHold: p.held.Holding(row.Index, types.WaitingReasonUnschedulable),
		podGroupHold:      p.held.Holding(row.Index, types.WaitingReasonPodGroupTerminating),
	}
	r.graceMayEnd = types.Interruptible(&r.row, types.EventStuckPodGrace)
	r.deadlineMayEnd = types.Interruptible(&r.row, types.EventOperationDeadline)
	return r
}

// rowStep is one step of a row's walk through the pass. It reports whether
// the walk goes on to the next step; an error ends the whole pass.
type rowStep func(ctx context.Context) (next bool, err error)

// runRowSteps runs the steps in order and stops at the first that ends the
// walk or fails. It reports whether every step went on.
func runRowSteps(ctx context.Context, steps []rowStep) (bool, error) {
	for _, step := range steps {
		if next, err := step(ctx); !next || err != nil {
			return false, err
		}
	}
	return true, nil
}

// run takes the row through the pass's steps in order. Each step answers
// one question about the row; the first that ends the walk, or fails, is
// the last one run.
func (r *rowEscalation) run(ctx context.Context) error {
	_, err := runRowSteps(ctx, []rowStep{
		r.refreshFailedRow,
		r.keepServingSet,
		r.endOnPodGroupOwnedElsewhere,
		r.waitForPodGroupName,
		r.endOnSchedulerGrace,
		r.requireInterruptibleState,
		r.skipPausedAttempt,
		r.endOnStuckPod,
		r.requireElapsedDeadline,
		r.waitForAdmissionGate,
		r.waitForExternalHold,
		r.decideExpiredAttempt,
		r.waitForRepairRebuild,
		r.endOnDeadline,
	})
	return err
}

// refreshFailedRow is the one visit a row already Failed gets: its failure
// record is refreshed once when the kubelet now names a concrete cause, and
// the once-only repair warnings are published. The row is never written
// Failed again, so the operator-facing event fires once per escalation.
func (r *rowEscalation) refreshFailedRow(ctx context.Context) (bool, error) {
	if r.row.Phase != types.InstancePhaseFailed {
		return true, nil
	}
	p := r.pass
	if t := refreshedFailureEvidence(r.row, r.reading); t != nil {
		p.failedWrites.add(r.row.Index, status.RecordRefreshedFailure(t, DeadlineExceededReason), nil)
	}
	if err := announceRepairExhausted(ctx, p.deps, p.input, p.plan, r.row); err != nil {
		return false, fmt.Errorf("announce exhausted repair (instance=%d): %w", r.row.Index, err)
	}
	if err := announceRepairWaiting(ctx, p.deps, p.input, p.plan, r.row); err != nil {
		return false, fmt.Errorf("announce waiting repair (instance=%d): %w", r.row.Index, err)
	}
	return false, nil
}

// keepServingSet is the failed-while-serving rule: a pod set fully in
// rotation at the desired count is capacity and is never written Failed,
// whatever the reading says. The hold steps after it read pod health
// alone, so a serving set hands any stale token back here. A surge whose
// source left rotation is not capacity (podSetIsCapacity).
func (r *rowEscalation) keepServingSet(context.Context) (bool, error) {
	return !podSetIsCapacity(r.row, r.pods, r.desiredPods), nil
}

// endOnPodGroupOwnedElsewhere ends an attempt whose PodGroup name another
// controller owns: nothing this controller does frees the name. The
// failure is the environment's and charges no revision.
func (r *rowEscalation) endOnPodGroupOwnedElsewhere(context.Context) (bool, error) {
	reason := gangTerminalReason(r.gang.State)
	if reason == "" || !gangVerdictActionable(&r.row) {
		return true, nil
	}
	p := r.pass
	escalateGangFailure(p.input, p.failedWrites, r.row, r.gang, reason, p.now)
	return false, nil
}

// waitForPodGroupName keeps a row whose PodGroup name is still being
// collected: the wait ends on its own once the old object is gone. A pod of
// the row's own held in a terminal waiting reason still reaches the
// stuck-pod step.
func (r *rowEscalation) waitForPodGroupName(context.Context) (bool, error) {
	return !r.podGroupHold || r.reading.StuckPod != nil, nil
}

// endOnSchedulerGrace answers an Unschedulable hold. Inside the operator's
// grace the row waits on cluster capacity, unless a pod of its own is held
// in a terminal waiting reason, which the stuck-pod step reads. Past the
// grace the attempt ends as the environment's failure: the scheduler's
// message is recorded and no revision is charged. With no grace set, the
// hold lasts. A gang held to a domain that cannot take it may end sooner
// (schedulerHoldGrace). The grace runs from the attempt's placement wait
// (schedulerHoldSince), so it does not start over when the gang member
// waiting for placement changes.
func (r *rowEscalation) endOnSchedulerGrace(ctx context.Context) (bool, error) {
	if !r.unschedulableHold {
		return true, nil
	}
	p := r.pass
	grace := schedulerHoldGrace(p.input, r.reading.UnschedulableMessage, r.pods)
	since := schedulerHoldSince(p.input.ObservedState.InstanceStatuses, r.row, r.reading.UnschedulableSince)
	if !unschedulableGraceElapsed(since, p.now, grace) {
		return r.reading.StuckPod != nil, nil
	}
	if err := escalateSchedulerHold(ctx, p.deps, p.input, p.deadlineDeps, p.failedWrites, r.row, r.pods, r.desiredPods, r.reading, p.now); err != nil {
		return false, fmt.Errorf("escalate scheduler hold (instance=%d): %w", r.row.Index, err)
	}
	if gangSurgeSourceOf(p.input.ObservedState.InstanceStatuses, r.row, types.UpdateStepSurgeDrain) != nil &&
		r.row.Operation.PlacementWaitSince != nil {
		// The ending landed on the draining source alone and this row
		// stays live, so its start is cleared in a write of its own: the
		// member rebuilt next waits on its own clock. Every other ending
		// clears the start in the row's own Failed write, and one batch
		// may name a row only once.
		p.failedWrites.add(r.row.Index, status.ClearPlacementWaitMutation(), nil)
	}
	return false, nil
}

// requireInterruptibleState asks the ownership table whether either clock
// may end the row's state. A migration pair ends through its record, which
// fails both rows; a teardown is the scale-down wave's, which paces a
// removal past any grace and reports its own overdue drain.
func (r *rowEscalation) requireInterruptibleState(context.Context) (bool, error) {
	return r.graceMayEnd || r.deadlineMayEnd, nil
}

// skipPausedAttempt leaves an attempt the operator paused: it was told to
// wait, its deadline rests for the length of the pause, and a pod held in a
// terminal waiting reason waits on the unpause, not on this pass.
func (r *rowEscalation) skipPausedAttempt(context.Context) (bool, error) {
	return !types.OperationPaused(r.row.Operation), nil
}

// endOnStuckPod ends an attempt one of whose pods the kubelet has held in a
// terminal waiting reason past the stuck-pod grace, when the table lets the
// grace end the state and the row qualifies (ShouldCheckForStuckPods: an
// attempt in flight, or a pod whose revision label disagrees with the
// current revision). A Create, recreate or in-place attempt takes the
// deadline decision; every other attempt is written Failed with the pod's
// diagnostics.
func (r *rowEscalation) endOnStuckPod(ctx context.Context) (bool, error) {
	if !r.graceMayEnd || r.reading.StuckPod == nil ||
		!ShouldCheckForStuckPods(&r.row, r.pods, r.pass.currentRevisionHash, query.LabelRevisionHash) {
		return true, nil
	}
	if takesDeadlineDecision(&r.row) {
		return false, r.decideStuckAttempt(ctx)
	}
	r.failOnStuckPod()
	return false, nil
}

// decideStuckAttempt hands the attempt to the deadline decision with the
// pod that is held. A single-pod surge shares its Instance index with the
// serving source, so the decision reads the held replacement alone, not
// both pods' nodes. The decision's own writes are ordered (the retry record
// before the operation is cleared), so the pending plain writes land first,
// as they would in an unbatched pass.
func (r *rowEscalation) decideStuckAttempt(ctx context.Context) error {
	p := r.pass
	if err := p.flushFailedWrites(ctx); err != nil {
		return err
	}
	attemptPods := r.pods
	if singlePodSurgeAttempt(&r.row, r.desiredPods) {
		attemptPods = []*corev1.Pod{r.reading.StuckPod}
	}
	if _, err := DecideExpiredAttempt(ctx, p.deps, p.input, p.deadlineDeps, r.row, attemptPods, r.reading.StuckReason); err != nil {
		return fmt.Errorf("decide stuck attempt (instance=%d): %w", r.row.Index, err)
	}
	return nil
}

// failOnStuckPod writes the row Failed with the held pod's waiting reason
// and container detail on its failure record, the one trace that survives
// the rebuild or teardown that deletes the pod. The write names the pod set
// by incarnation, so a repair that claimed that set since this observation
// keeps it.
func (r *rowEscalation) failOnStuckPod() {
	p := r.pass
	termination := types.PodTerminationWithReason(r.reading.StuckPod, r.reading.StuckReason, metav1.NewTime(p.now))
	idx, podName, reason := r.row.Index, r.reading.StuckPod.Name, r.reading.StuckReason
	p.failedWrites.add(idx, status.StampFailedOnStuckPod(termination, blamedIncarnation(r.row, r.reading.StuckPod)), func() {
		p.input.WarnInstanceFailed(idx, podName, reason)
	})
}

// requireElapsedDeadline admits a row to the deadline steps only when its
// operation's deadline has elapsed and the table lets the deadline end the
// state.
func (r *rowEscalation) requireElapsedDeadline(context.Context) (bool, error) {
	return r.deadlineMayEnd && r.reading.DeadlinePassed, nil
}

// waitForAdmissionGate keeps a row whose pods still carry an admission
// scheduling gate: they are queued, not failing, and the deadline step
// zeroes their clock; a gate seen before that write lands must not expire
// the attempt either. A gang surge's attempt pods live under its surge
// index, so that bucket is read too.
func (r *rowEscalation) waitForAdmissionGate(context.Context) (bool, error) {
	pods := r.pass.podsByInstance
	if anyPodAdmissionGated(pods[r.row.Index]) {
		return false, nil
	}
	op := r.row.Operation
	if op != nil && op.SurgeIndex != nil && anyPodAdmissionGated(pods[*op.SurgeIndex]) {
		return false, nil
	}
	return true, nil
}

// waitForExternalHold keeps a row whose operation waits on an authority
// outside the workload: quota the apiserver refused, a placement the
// scheduler has not found. No pod carries a gate for these, so the wait is
// on the operation, on the surge row for a gang surge. The deadline step
// zeroes that clock; one written before the hold landed must not expire
// the attempt either.
func (r *rowEscalation) waitForExternalHold(context.Context) (bool, error) {
	return !instanceHeldExternally(&r.row, r.pass.input.ObservedState.InstanceStatuses), nil
}

// decideExpiredAttempt hands an expired Create, recreate or in-place
// attempt to the deadline decision, after the pending plain writes have
// landed; the decision's writes keep their own order.
func (r *rowEscalation) decideExpiredAttempt(ctx context.Context) (bool, error) {
	if !takesDeadlineDecision(&r.row) {
		return true, nil
	}
	p := r.pass
	if err := p.flushFailedWrites(ctx); err != nil {
		return false, err
	}
	if _, err := DecideExpiredAttempt(ctx, p.deps, p.input, p.deadlineDeps, r.row, r.pods, deadlineFailureMessage(r.row.Operation)); err != nil {
		return false, fmt.Errorf("decide expired attempt (instance=%d): %w", r.row.Index, err)
	}
	return false, nil
}

// waitForRepairRebuild keeps a repair whose drained set is already gone:
// the Failed write would stand on no pod set, so it waits for the pass that
// observes the rebuilt one (repairSetGone).
func (r *rowEscalation) waitForRepairRebuild(ctx context.Context) (bool, error) {
	if r.row.Operation.Type != types.InstanceOperationRestart {
		return true, nil
	}
	p := r.pass
	gone, err := repairSetGone(ctx, p.deps, p.input, p.plan, r.row.Index)
	if err != nil {
		return false, fmt.Errorf("read the repair's pod set (instance=%d): %w", r.row.Index, err)
	}
	return !gone, nil
}

// endOnDeadline writes the row Failed with its operation kept, so the pass
// that owns the attempt (a gang surge's end, a repair) can finish it, and
// warns with the operation and step that ran out of time.
func (r *rowEscalation) endOnDeadline(context.Context) (bool, error) {
	p := r.pass
	idx, op := r.row.Index, r.row.Operation
	p.failedWrites.add(idx, status.StampFailedKeepingOperation(r.deadlineFailureRecord()), func() {
		p.input.WarnInstanceFailed(idx, "", deadlineFailureMessage(op))
	})
	return false, nil
}

// deadlineFailureRecord is what the row keeps of an elapsed deadline once
// the pods it names are deleted. A workload-caused waiting reason, a pod
// that ran yet never reported ready, or a pod whose readiness gate never
// folded into Ready names the cause better than the elapsed timeout, and a
// gang surge's end charges the revision's retry record by that reason.
func (r *rowEscalation) deadlineFailureRecord() *types.InstanceTermination {
	now := metav1.NewTime(r.pass.now)
	if pod, reason := evidence.FirstWorkloadCausedPod(r.pods); pod != nil {
		return types.PodTerminationWithReason(pod, reason, now)
	}
	if pod, _ := evidence.FirstRunningNotReadyPod(r.pods, r.attemptRevision); pod != nil {
		return evidence.ContainersNotReadyTermination(pod, now)
	}
	if pod := evidence.FirstGateNotFoldedPod(r.pods, r.attemptRevision); pod != nil {
		return evidence.GateNotFoldedTermination(pod, now)
	}
	return deadlineTermination(r.pass.now, r.row.Operation)
}

// announceRepairExhausted warns once per parked attempt that a repair has
// spent every re-arm the operator's ladder allows: the row stays Failed
// until an operator resets it or a new revision arrives, and nothing else
// in the cluster is going to say so.
func announceRepairExhausted(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, row types.InstanceStatus) error {
	if deps.Recorder == nil || !workloadops.RepairRetriesExhausted(input, &row) {
		return nil
	}
	target := types.EventTarget(input)
	if target == nil {
		return nil
	}
	announced, err := status.Announce(ctx, input, row.Index, types.EventReasonRepairRetriesExhausted)
	if err != nil || !announced {
		return err
	}
	deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(types.EventReasonRepairRetriesExhausted),
		"OMENative component=%s instance=%d repair parked at Failed after %d re-arm(s): %s; reset the Instance or publish a corrected revision",
		plan.Component, row.Index, row.Operation.RetryCount, workloadops.RepairFailureSummary(&row))
	return nil
}

// announceRepairWaiting records once per parked attempt that a repair is
// parked on a cause the kubelet retries in place — the configuration or
// image the pod template names — so the retry ladder owes it no re-arm:
// the note lands on the row's failure message, and the Warning fires on
// the write that recorded it. Nothing here moves the row; it resumes on
// its own once the kubelet starts the pod (ops.RepairResumes).
func announceRepairWaiting(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, row types.InstanceStatus) error {
	if !types.RepairWaitsOnWorkload(&row) {
		return nil
	}
	recorded, err := status.AnnounceRepairWaiting(ctx, input, row.Index, workloadops.RepairWaitingNote)
	if err != nil || !recorded {
		return err
	}
	types.RecordWarning(deps.Recorder, types.EventTarget(input), types.EventReasonRepairWaitingOnWorkload,
		"OMENative %s repair parked at Failed on %s: %s, and the Instance resumes on its own once the pod starts",
		types.InstanceKey(plan.Component, row.Index), workloadops.RepairFailureSummary(&row), workloadops.RepairWaitingNote)
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
	// (evidence.FirstStuckPodForInstanceSince). Nil when no pod is stuck.
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
// own-plus-sibling), and an Update attempt only through the pods on the
// revision it converges toward (evidence.AttemptStuckPods): the pod it is
// replacing may be the broken workload the roll corrects. A non-positive
// grace disables stuck-pod evidence entirely (fast escalation off; the
// deadline backstop and the scheduler hold still report). now/grace are
// supplied by the caller (clock seam); attemptRev is the attempt's
// resolved target revision.
func evidenceFor(observed *types.WorkloadObservedState, byIdx map[int32][]*corev1.Pod, idx int32, now time.Time, grace time.Duration, attemptRev string) InstanceEvidence {
	var ev InstanceEvidence
	inst := observed.Instance(idx)
	if inst != nil {
		ev.DeadlinePassed = operationDeadlinePassed(inst, now)
	}
	ev.Unschedulable, ev.UnschedulableMessage, ev.UnschedulableSince = types.FirstUnschedulablePod(byIdx[idx])
	pods := byIdx[idx]
	if inst != nil {
		pods = evidence.AttemptStuckPods(*inst, evidence.PodsForStuckCheck(*inst, byIdx), attemptRev)
	}
	if grace <= 0 {
		return ev
	}
	if pod, reason := evidence.FirstStuckPodForInstanceSince(pods, now, grace, attemptEvidenceFloor(inst)); pod != nil {
		ev.StuckPod = pod
		ev.StuckReason = reason
	}
	return ev
}

// attemptEvidenceFloor is the instant before which a pod's waiting
// evidence is not the attempt's own. An in-place patch acts on a pod that
// already exists and restamps it before the kubelet has replaced its
// container, so a reason the pod held when the attempt opened — the
// crash loop of the revision the patch replaces — is the attempt's reason,
// and the grace to clear it runs from the attempt's start. Every other
// attempt's pods are created after it opens, so its floor is zero.
func attemptEvidenceFloor(inst *types.InstanceStatus) time.Time {
	if inst == nil || inst.Operation == nil ||
		inst.Operation.Type != types.InstanceOperationUpdate || inst.Operation.Step != types.UpdateStepInPlace {
		return time.Time{}
	}
	return inst.Operation.StartedAt.Time
}

// blamedIncarnation names the pod set the stuck-pod stamp blames: the pod's
// own incarnation when it is the row's, and the row's observed incarnation
// for a gang surge source, whose replacement pods carry the marker row's.
func blamedIncarnation(row types.InstanceStatus, stuck *corev1.Pod) int64 {
	if row.Operation != nil && row.Operation.Type == types.InstanceOperationUpdate && row.Operation.SurgeIndex != nil {
		return row.Incarnation
	}
	inc, _ := query.InstanceIncarnationFromLabels(stuck)
	return inc
}

func singlePodSurgeAttempt(row *types.InstanceStatus, desiredPods int32) bool {
	return row != nil && desiredPods == 1 && row.Operation != nil &&
		row.Operation.Type == types.InstanceOperationUpdate &&
		row.Operation.Step == types.UpdateStepSurge && row.Operation.SurgeIndex == nil
}

// repairSetGone reports whether every pod of the Instance is gone, or
// deleting inside its own deletion deadline, on a live read: the repair's
// drain has deleted the set it replaces and the rebuild has not run yet.
// The deadline was judged on the pass's pod observation, which predates
// that delete, and a spent repair's exits are read off its pods, so a park
// with no set behind it has none. The repair owns the set until the
// rebuilt one exists; the elapsed deadline parks that one. A set still
// live on the read — a drain not finished, or a rebuilt set in any state —
// parks as the deadline says. So does a pod deleting past its own deadline:
// a kubelet removes a pod within moments of it, so what stays is a name no
// delete in flight will free, and the park is what makes that visible.
func repairSetGone(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, idx int32) (bool, error) {
	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, idx)
	if err != nil {
		return false, err
	}
	now := input.Now()
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil || now.After(pod.DeletionTimestamp.Time) {
			return false, nil
		}
	}
	return true, nil
}

// failureStampBuffer coalesces the escalation pass's plain Failed
// stamps into one batched status write. Buffer only standalone stamps:
// mutations nothing later in the reconcile depends on being persisted
// first. Each buffered warn fires only after its stamp changed the row
// and the write succeeded, matching the immediate path: a failed write
// emits nothing and the evidence re-escalates next reconcile, and a
// stamp the fresh row withheld announces nothing. A nil warn is a
// mutation that carries no operator-facing event of its own.
type failureStampBuffer struct {
	input  types.ReconcileInput
	stamps []bufferedStamp
}

// bufferedStamp is one stamp with the warning it owes once it commits.
type bufferedStamp struct {
	mutation  types.InstanceMutation
	warn      func()
	committed bool
}

func (b *failureStampBuffer) add(idx int32, mutate func(*types.InstanceStatus) bool, warn func()) {
	b.stamps = append(b.stamps, bufferedStamp{mutation: types.InstanceMutation{Index: idx, Mutate: mutate}, warn: warn})
}

// flush persists the buffered stamps — one batched write when the
// adapter provides ApplyInstanceMutations, one single-row write per
// stamp otherwise — then fires the warnings of the stamps that
// committed. Empty buffer = zero writes. The buffer resets whichever
// path ran.
func (b *failureStampBuffer) flush(ctx context.Context) error {
	if len(b.stamps) == 0 {
		return nil
	}
	if b.input.ApplyInstanceMutations != nil {
		muts := make([]types.InstanceMutation, len(b.stamps))
		for i := range b.stamps {
			stamp := &b.stamps[i]
			muts[i] = stamp.mutation
			muts[i].OnCommit = func(_, _ *types.InstanceStatus) { stamp.committed = true }
		}
		if err := b.input.ApplyInstanceMutations(ctx, muts); err != nil {
			return err
		}
		for _, stamp := range b.stamps {
			if stamp.committed && stamp.warn != nil {
				stamp.warn()
			}
		}
	} else {
		for i := range b.stamps {
			stamp := &b.stamps[i]
			committed, err := status.ApplyStamp(ctx, b.input, stamp.mutation.Index, stamp.mutation.Mutate)
			if err != nil {
				return err
			}
			if committed && stamp.warn != nil {
				stamp.warn()
			}
		}
	}
	b.stamps = nil
	return nil
}

// podSetIsCapacity reports whether the Instance is healthy capacity,
// which is what buys it the FAILED-WHILE-SERVING exemption: a set in
// rotation, PodReady and the serving gate agreeing on every pod.
//
// Pod conditions alone do not settle that during a surge. Until the
// drain step the source is what holds the Instance's traffic, and a
// surge whose source has been taken out of rotation by someone else says
// so on its operation: the pods can read fully serving while nothing is
// in rotation, and a replacement wedged in that state must escalate
// rather than be exempted by a source that is not serving.
func podSetIsCapacity(row types.InstanceStatus, pods []*corev1.Pod, desired int32) bool {
	return query.PodSetReadyAndServing(pods, desired) && !types.OperationSourceUnrouted(row.Operation)
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
