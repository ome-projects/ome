package canary

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// parkedWithoutStable reports a rollback that parked because the stable
// revision it would return to is not retained: the rejected hash stays
// recorded, but nothing is draining and nothing will.
func parkedWithoutStable(cs *v1beta1.CanaryStatus) bool {
	return cs.Failed != nil && cs.Failed.Reason == v1beta1.CanaryFailureStableRevisionMissing
}

// ignoreRollback consumes a rollback request that has no canary to act on
// and says so: left in place it would fire on the next canary's first pass.
func ignoreRollback(in ReconcileInputs, take func(string)) {
	if !isRollbackRequested(in.ISVC) {
		return
	}
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryRollbackIgnored,
		"rollback requested for %s with no canary in flight; the request is removed", in.Component)
	take(constants.RolloutRollbackAnnotation)
}

// refuseRollback hands back a rollback request against a unit parked because
// its stable revision is not retained, and says so. The request is spent:
// the rejected hash it recorded keeps a copy still visible after the flush
// inert, and the park is the only answer it can get.
func refuseRollback(in ReconcileInputs, cs *v1beta1.CanaryStatus, take func(string)) {
	if !parkedWithoutStable(cs) || !isRollbackRequested(in.ISVC) {
		return
	}
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryRollbackIgnored,
		"rollback requested for %s, which is parked because no ControllerRevision is retained for its stable revision; the request is removed", in.Component)
	take(constants.RolloutRollbackAnnotation)
}

// rearmRule says which observed targets a state treats as new. Every state
// re-arms when the run's TargetID changed for a target the run offered the
// unit; the hash test is the fallback for status recorded before targets
// carried an id, and its exclusions differ by state because the observed
// target means different things there: in a live canary a target equal to
// the stable revision is a revert to apply, while a parked or rolled-back
// unit observes the stable revision as the hold it already sits in, and
// must not re-arm toward it.
type rearmRule struct {
	// excludeCurrent: the recorded canary hash is not a new target.
	excludeCurrent bool
	// excludeRejected: the rolled-back hash is not a new target.
	excludeRejected bool
	// excludeStable: the stable revision (the persisted identity, else the
	// live pod set's other revision) is not a new target.
	excludeStable bool
}

var (
	rearmLive       = rearmRule{excludeCurrent: true}
	rearmRolledBack = rearmRule{excludeRejected: true, excludeStable: true}
	rearmFailed     = rearmRule{excludeCurrent: true, excludeStable: true}
	rearmDone       = rearmRule{excludeCurrent: true}
)

// shouldRearm reports whether a pinned run names a target this state treats
// as new. Without a pinned run nothing re-arms: the run layer opens or parks
// the run, and starting a canary outside one would execute an unpinned plan.
//
// For a unit holding a rejected revision, a changed TargetID counts only when
// the run retargeted the unit. A run opened by another group's retarget pins
// such a unit at its stable revision in place of the rejected one, which
// changes the unit's TargetID without offering it a target; re-arming on it
// would present the rejected revision again. The unit falls through to the
// hash test, which reads the rejected and stable revisions as its hold.
func shouldRearm(in ReconcileInputs, cs *v1beta1.CanaryStatus, targetChanged bool, rule rearmRule) bool {
	if !in.RunActive || in.CanaryRevisionHash == "" {
		return false
	}
	if targetChanged && (cs.RolledBackRevisionHash == "" || unitRetargeted(in)) {
		return true
	}
	if rule.excludeCurrent && in.CanaryRevisionHash == cs.CanaryRevisionHash {
		return false
	}
	if rule.excludeRejected && in.CanaryRevisionHash == cs.RolledBackRevisionHash {
		return false
	}
	if rule.excludeStable {
		held := cs.RolledBackRevisionHash
		if held == "" {
			held = cs.CanaryRevisionHash
		}
		if in.CanaryRevisionHash == stableHashFor(cs, in.PerRevisionPods, held) {
			return false
		}
	}
	return true
}

// applyHolds runs the verbs and holds of the canary grid in the order they
// take precedence: a live canary's retarget, the rollback hold, a rollback
// request, the Failed hold, the done sentinel, then arming. It returns the
// status the step machine runs against (freshly armed when it was nil) and
// a non-nil Result when the pass ends inside a hold.
func applyHolds(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, targetChanged bool, take func(string)) (*v1beta1.CanaryStatus, *Result, error) {
	steps := len(plan.Steps)

	// A live canary binds to the IR's authoritative target before any verb
	// is read. A changed target restarts the staged plan with fresh
	// per-target state while preserving the original stable identity.
	if cs != nil && int(cs.CurrentStep) < steps && cs.RolledBackRevisionHash == "" &&
		rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, steps) != rollout.CanaryStateFailed {
		if cs.StableRevisionHash == "" && in.StableRevisionHash != "" {
			cs.StableRevisionHash = in.StableRevisionHash
		}
		if shouldRearm(in, cs, targetChanged, rearmLive) {
			resetCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, in.CanaryRevisionHash, in.Now)
		}
	}

	// Rollback: a request records the rejected revision and starts the
	// revert; the hold rejects the rolled-back revision until a genuinely
	// new target appears (clearing the annotation alone never retries it).
	// The revert itself is driven by the controller, which points the IR at
	// the stable ControllerRevision while cs.RolledBackRevisionHash is set.
	// The recorded rejection is also the record that the request was
	// applied: a unit carrying one never takes a request again, so a copy
	// still visible after the flush is inert, whether the revert is
	// draining or parked because the stable revision is not retained.
	if cs != nil {
		if cs.RolledBackRevisionHash != "" {
			if shouldRearm(in, cs, targetChanged, rearmRolledBack) {
				// The re-arm does not depend on the annotation, so a lost
				// status flush recomputes it; consuming after the flush
				// would let a lingering request roll the new canary back.
				if err := consumeAnnotation(ctx, in.Client, in.ISVC, constants.RolloutRollbackAnnotation); err != nil {
					return cs, nil, err
				}
				resetCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, in.CanaryRevisionHash, in.Now)
			} else if !parkedWithoutStable(cs) {
				return cs, reconcileRollback(in, cs), nil
			}
		} else if isRollbackRequested(in.ISVC) && int(cs.CurrentStep) < steps {
			// A rollback is the operator's decision about the canary, parked
			// or not: it ends a Failed park and drains the rejected revision.
			cs.RolledBackRevisionHash = cs.CanaryRevisionHash
			cs.Failed = nil
			recordRollback(in.ISVC, in.Component, "manual")
			return cs, reconcileRollback(in, cs), nil
		}
	}

	// Failed is a parked terminal: stable keeps serving until the operator
	// acts or a genuinely new target appears. The park is not re-evaluated
	// and its clocks are not re-stamped, so it cannot oscillate back into
	// the step machine.
	if cs != nil && rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, steps) == rollout.CanaryStateFailed {
		if shouldRearm(in, cs, targetChanged, rearmFailed) {
			resetCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, in.CanaryRevisionHash, in.Now)
		} else {
			// The marker is the record; the phase is re-projected from it
			// every pass, so a status write that dropped the phase cannot
			// un-park the canary.
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseFailed)
			refuseRollback(in, cs, take)
			return cs, (&Result{Active: true}).wake(in.ParkedRequeue), nil
		}
	}

	// Done sentinel: a finished canary keeps its status at CurrentStep ==
	// len(steps). EffectivePartition maps that to partition 0; a nil status
	// would re-default to step 0's partition and hold instances back on the
	// old revision after completion. The sentinel is the release of the held
	// floor, so the unit finishes the cutover from here: it stays Promoting
	// while the last stable instance rolls and reads Stable once every
	// member serves on its target alone.
	if cs != nil && int(cs.CurrentStep) >= steps {
		if shouldRearm(in, cs, targetChanged, rearmDone) {
			resetCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, in.CanaryRevisionHash, in.Now)
		} else {
			// A canary can complete while the durable promote record is still
			// waiting on observed annotation absence; converge it here since
			// a done canary evaluates no gates.
			syncPromotedThrough(in, cs, take)
			ignoreRollback(in, take)
			return cs, finishCutover(in, cs), nil
		}
	}

	// A live canary with no pinned run holds in place: opening, adopting or
	// parking the run is the run layer's job, and stepping an unpinned plan
	// is the same hazard arming one would be. The update gates already hold
	// the forward roll fail-closed underneath.
	if cs != nil && !in.RunActive {
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryRunLost,
			"%s canary has no pinned rollout run; holding at step %d until the run layer reopens or adopts it", in.Component, cs.CurrentStep)
		partition := stepPartition(plan.Steps[cs.CurrentStep], in.DesiredReplicas)
		return cs, (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
	}

	// Arming: a new canary starts only under a pinned run, with a stable
	// revision to shift traffic from, toward a target the unit does not
	// already serve. A unit the run did not retarget sits the run out: arming
	// it would walk a full ladder, spend its analysis budget on a no-op, and
	// could reach a rollback for a rollout that never happened.
	if cs == nil {
		ignoreRollback(in, take)
		if in.CanaryRevisionHash == "" || in.StableRevisionHash == "" ||
			(in.TargetID == "" && readyCanaryCapacity(in) >= in.DesiredReplicas) ||
			!unitRetargeted(in) {
			if in.CanaryRevisionHash != "" && in.DesiredReplicas > 0 {
				setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			}
			return nil, &Result{Active: false}, nil
		}
		// A diverged target with no pinned run holds as-is: the update gates
		// already hold the forward roll fail-closed.
		if !in.RunActive {
			return nil, &Result{Active: false}, nil
		}
		rollout.SetCanaryStatusFor(&in.ISVC.Status, in.Component, &v1beta1.CanaryStatus{
			TargetID:           in.TargetID,
			CanaryRevisionHash: in.CanaryRevisionHash,
			StableRevisionHash: in.StableRevisionHash,
			CurrentStep:        0,
			StepEnteredTime:    &metav1.Time{Time: in.Now},
		})
		cs = rollout.CanaryStatusFor(&in.ISVC.Status, in.Component)
	}
	return cs, nil, nil
}
