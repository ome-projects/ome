// escalation.go — the terminal-failure escalation pass. Reconcile runs
// it once at the end of every eligible non-teardown, non-paused,
// non-error reconcile: per Instance it consumes the snapshot's failure evidence
// (stuck pod / elapsed Operation deadline) and decides the Failed
// transition through the shared disposition classification
// (disposition.go). The Failed DECISION lives here, next to the rest of
// the transition decisions; the adapter-side status aggregator owns
// only counters + conditions and never writes a transition field.
package workload

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// escalateFromEvidence walks every observed InstanceStatus and escalates
// terminal failures from the snapshot's evidence. Two paths converge on
// Phase=Failed, and they don't fight — whichever fires first wins and
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
// BOTH paths skip operations whose state machine owns its own terminal
// handling:
//
//   - Migrate (either pair side): timeout authority is the owner's
//     status.migrations record, consumed by the dispatcher's
//     migration-expiry pass. Stamping here would mark a healthy serving
//     source Failed while the record keeps driving the pair.
//   - Delete: DeleteBatch owns progress, stuck-Terminating escalation,
//     and completion. Its durable ownership must survive an elapsed
//     generic operation deadline, including when the index is planned
//     again while deletion finishes.
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
func escalateFromEvidence(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, excluded map[int32]struct{}) error {
	byIdx, err := snapshot.CachedPods(ctx)
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
	desiredByIdx := DesiredPodCountByInstance(plan)
	currentHash := query.RevisionFromName(input.ObservedState.CurrentRevision).Hash()
	dd := input.Disposition
	dd.MigrationMode = plan.MigrationMode
	buf := &failureStampBuffer{input: input}
	for _, s := range input.ObservedState.InstanceStatuses {
		if _, skip := excluded[s.Index]; skip {
			continue
		}
		// Idempotent: skip an already-Failed Instance so the
		// operator-facing event fires exactly once per escalation.
		if s.Phase == InstancePhaseFailed {
			continue
		}
		if s.Operation != nil {
			switch s.Operation.Type {
			case InstanceOperationMigrate, InstanceOperationDelete:
				continue
			}
		}
		ev := snapshot.EvidenceFor(ctx, s.Index, now, input.StuckPodGrace)
		if ev.StuckPod == nil && !ev.DeadlinePassed {
			continue
		}
		pods := podsForStuckCheck(s, byIdx)
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		if podSetFullyServing(pods, desired) {
			continue
		}
		// FAST path.
		if ev.StuckPod != nil && ShouldCheckForStuckPods(&s, pods, currentHash, query.LabelRevisionHash) {
			if disposableAttempt(&s, desired) {
				// The disposition's writes are write-ahead-ordered; land
				// the pending plain stamps first so the overall write +
				// event order matches the unbatched pass.
				if ferr := buf.flush(ctx); ferr != nil {
					return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, ferr)
				}
				dispositionPods := pods
				if singlePodSurgeAttempt(&s, desired) {
					// A single-pod surge shares its Instance index with the
					// serving source. Relocation evidence belongs to the exact
					// stuck target, not the source-plus-target node set.
					dispositionPods = []*corev1.Pod{ev.StuckPod}
				}
				if _, derr := DisposeExpiredAttempt(ctx, deps, input, dd, s, dispositionPods, ev.StuckReason); derr != nil {
					return fmt.Errorf("dispose stuck attempt (instance=%d): %w", s.Index, derr)
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
			termination := PodTerminationWithReason(ev.StuckPod, ev.StuckReason, metav1.NewTime(now))
			idx, podName, reason := s.Index, ev.StuckPod.Name, ev.StuckReason
			buf.add(idx, stuckPodFailedMutation(termination), func() {
				input.WarnInstanceFailed(idx, podName, reason)
			})
			continue
		}
		// BROAD path.
		if !ev.DeadlinePassed {
			continue
		}
		// Admission-gated pods are queued, not stuck: the parking step
		// zeroes their deadline; a gate-enter observed before the park
		// lands must not expire either. A gang-surge source's attempt
		// pods live in its SurgeIndex bucket, so check that bucket too.
		if anyPodAdmissionGated(byIdx[s.Index]) {
			continue
		}
		if s.Operation != nil && s.Operation.SurgeIndex != nil && anyPodAdmissionGated(byIdx[*s.Operation.SurgeIndex]) {
			continue
		}
		// Same rationale, no pod to read it from: a create admission
		// refused for lack of quota leaves nothing to carry a scheduling
		// gate, so the wait is recorded on the Operation instead — on the
		// surge row for a gang surge, whose source this check follows the
		// same way the gate check above follows the SurgeIndex bucket. The
		// parking step zeroes that deadline; one stamped before the park
		// landed must not expire either.
		if instanceCapacityBlocked(&s, input.ObservedState.InstanceStatuses) {
			continue
		}
		op := s.Operation
		if disposableAttempt(&s, desired) {
			if ferr := buf.flush(ctx); ferr != nil {
				return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, ferr)
			}
			if _, derr := DisposeExpiredAttempt(ctx, deps, input, dd, s, pods, deadlineFailureMessage(op)); derr != nil {
				return fmt.Errorf("dispose expired attempt (instance=%d): %w", s.Index, derr)
			}
			continue
		}
		idx := s.Index
		// The teardown that follows deletes the blamed pods, so revision-scoped
		// evidence must survive on LastFailure: a workload-caused waiting reason
		// names the cause more precisely than the elapsed timeout, and it is what
		// the gang abandon charges the revision's retry ladder on.
		termination := deadlineTermination(now, op)
		if pod, reason := firstWorkloadCausedPod(pods); pod != nil {
			termination = PodTerminationWithReason(pod, reason, metav1.NewTime(now))
		}
		buf.add(idx, deadlineFailedMutation(termination), func() {
			input.WarnInstanceFailed(idx, "", deadlineFailureMessage(op))
		})
	}
	if ferr := buf.flush(ctx); ferr != nil {
		return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, ferr)
	}
	return nil
}

func singlePodSurgeAttempt(s *InstanceStatus, desiredPods int32) bool {
	return s != nil && desiredPods == 1 && s.Operation != nil &&
		s.Operation.Type == InstanceOperationUpdate &&
		s.Operation.Step == UpdateStepSurge && s.Operation.SurgeIndex == nil
}

// failureStampBuffer coalesces the escalation pass's plain Failed
// stamps into one batched status write. Buffer only standalone stamps:
// mutations nothing later in the reconcile depends on being persisted
// first. Each buffered warn fires only after its stamp's write
// succeeded, matching the immediate path (a failed write emits nothing;
// the evidence re-escalates next reconcile).
type failureStampBuffer struct {
	input ReconcileInput
	muts  []InstanceMutation
	warns []func()
}

func (b *failureStampBuffer) add(idx int32, mutate func(*InstanceStatus) bool, warn func()) {
	b.muts = append(b.muts, InstanceMutation{Index: idx, Mutate: mutate})
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
			warn()
		}
	} else {
		for i, m := range b.muts {
			if err := b.input.MutateInstance(ctx, m.Index, m.Mutate); err != nil {
				return err
			}
			b.warns[i]()
		}
	}
	b.muts, b.warns = nil, nil
	return nil
}

// podSetFullyServing reports whether the pod set is fully healthy in the
// rotation: at least `desired` live (non-deleting) pods, every one of
// them ContainersReady AND carrying the serving gate. Deleting pods are
// excluded rather than disqualifying — a completed surge leaves the old
// pod draining next to the serving replacement. A terminal pod is never
// live and disqualifies the set outright: it is failure evidence that
// must reach escalation even when stale conditions on it still read
// healthy, or when a serving sibling would otherwise cover the count.
// desired <= 0 never counts as serving (nothing is expected, so nothing
// can prove health).
func podSetFullyServing(pods []*corev1.Pod, desired int32) bool {
	if desired <= 0 {
		return false
	}
	var live int32
	for _, p := range pods {
		if p == nil || p.DeletionTimestamp != nil {
			continue
		}
		if query.IsTerminalPod(p) || !podreadiness.IsContainersReady(p) || !podreadiness.IsServing(p) {
			return false
		}
		live++
	}
	return live >= desired
}

// anyPodAdmissionGated reports whether any pod still carries an
// admission scheduling gate (queued by an external admission authority,
// e.g. Kueue).
func anyPodAdmissionGated(pods []*corev1.Pod) bool {
	for _, p := range pods {
		if PodAdmissionGated(p) {
			return true
		}
	}
	return false
}
