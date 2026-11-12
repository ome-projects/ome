package canary

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// stoppedWithoutStable reports a rollback that stopped Failed because the stable
// revision it would return to is not retained: the rejected hash stays
// recorded, but nothing is draining and nothing will.
func stoppedWithoutStable(cs *v1beta1.CanaryStatus) bool {
	return cs.Failed != nil && cs.Failed.Reason == v1beta1.CanaryFailureStableRevisionMissing
}

// cutoverRolling reports a unit at its done sentinel whose cutover has not
// landed: the pre-canary stable identity is still on record and the unit has
// not read Stable, so the released floor is still rolling and the stable
// revision is still one the unit can return to.
func cutoverRolling(in ReconcileInputs, cs *v1beta1.CanaryStatus, steps int) bool {
	return int(cs.CurrentStep) >= steps && cs.StableRevisionHash != "" &&
		in.ISVC.Status.Components[in.Component].RolloutPhase != v1beta1.RolloutPhaseStable
}

// ignoreRollback consumes a rollback request no unit of the service can act
// on and says so: left in place it would fire on the next canary's first
// pass. A request a unit holds, or one another unit's canary can still
// apply, is that unit's and is left for it.
func ignoreRollback(in ReconcileInputs, cs *v1beta1.CanaryStatus, take func(string)) {
	if !rollbackRequestSpendable(in.ISVC, in.Policies, in.Component, cs) {
		return
	}
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryRollbackIgnored,
		"rollback requested for %s with no canary in flight; the request is removed", in.Component)
	take(constants.RolloutRollbackAnnotation)
}

// ignoreForce consumes a forced promote the unit's state gives nothing to
// act on, and says why: left in place it would act on the first step the
// ladder meets later, which is not the step the operator forced.
func ignoreForce(in ReconcileInputs, cs *v1beta1.CanaryStatus, take func(string), why string) {
	if cs == nil || !forceRequested(in.ISVC, cs) {
		return
	}
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryForceIgnored,
		"forced promote of %s revision %s ignored: %s; the request is removed", in.Component, cs.CanaryRevisionHash, why)
	take(constants.RolloutPromoteForceAnnotation)
}

// refuseRollback hands back a rollback request against a unit parked because
// its stable revision is not retained, and says so. The request is spent:
// the rejected hash it recorded keeps a copy still visible after the flush
// inert, and the park is the only answer it can get. A request another unit
// holds, or one another unit's canary can still apply, is that unit's and is
// left for it.
func refuseRollback(in ReconcileInputs, cs *v1beta1.CanaryStatus, take func(string)) {
	if !stoppedWithoutStable(cs) || !rollbackRequestSpendable(in.ISVC, in.Policies, in.Component, cs) {
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
// target means different things there: a parked or rolled-back unit
// observes the stable revision as the hold it already sits in, and treats
// it as no new target. A record holding no rejection never meets a run that
// offers the unit no target: such a run is the operator taking the push
// back, and the record ends before this rule is asked (rollTakenBack).
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

// targetReverted reports a record whose roll no longer exists: no run is
// open, the targets the record was written for are not the members'
// targets any more, and every member already has its full capacity Ready
// on the target it has now with no pod on another revision. A push
// reverted before any Instance moved reads so on the pass that closes its
// run; with Instances still trailing, the run layer opens a run toward the
// restored revision instead, and the record ends under it as a roll the
// operator took back (rollTakenBack).
func targetReverted(in ReconcileInputs, g *v1beta1.RolloutGroup, cs *v1beta1.CanaryStatus) bool {
	if in.RunActive || cs == nil {
		return false
	}
	if cs.TargetID != "" {
		observed := observedTargetID(in, g)
		if observed == "" || observed == cs.TargetID {
			return false
		}
	} else if in.CanaryRevisionHash == "" || in.CanaryRevisionHash == cs.CanaryRevisionHash {
		return false
	}
	return cutoverComplete(in)
}

// observedTargetID fingerprints the targets the unit's members have now,
// the way the bind fingerprints a run's targets, so the two compare.
func observedTargetID(in ReconcileInputs, g *v1beta1.RolloutGroup) string {
	members := []v1beta1.ComponentType{in.Component}
	if g != nil {
		members = g.Components
	}
	return rollout.CanaryTargetID(members, func(c v1beta1.ComponentType) string {
		if c == in.Component {
			return in.CanaryRevisionHash
		}
		if m, ok := in.Secondaries[c]; ok {
			return m.CanaryRevisionHash
		}
		return ""
	})
}

// rollTakenBack reports a record whose roll the operator took back: a run
// is open that records every member of the unit at its stable revision, so
// it offers the unit no target, and the record names a target the run does
// not. The spec went back to the stable revision while Instances still run
// another, which is what makes the run open. A unit holding a rejection is
// not read here: its hold stands until a resume or a target the run offers.
// A record whose target the run still names, by the run's own id and
// revision, is the run's own and keeps its steps: the id is a fingerprint of
// the targets alone, so a finished unit under a run another group opened
// over it carries the new run's id and finishes its cutover, and a record
// that already names the stable revision as its target walks on as one.
func rollTakenBack(in ReconcileInputs, cs *v1beta1.CanaryStatus) bool {
	if cs == nil || cs.RolledBackRevisionHash != "" || !in.RunActive || in.TargetID == "" || in.CanaryRevisionHash == "" {
		return false
	}
	if cs.TargetID == in.TargetID && cs.CanaryRevisionHash == in.CanaryRevisionHash {
		return false
	}
	return !unitRetargeted(in)
}

// endCanary ends a record whose roll no longer exists: the unit reads
// Stable with no record, as before its push, and a split a step wrote is
// moved onto the revision every member runs.
func endCanary(in ReconcileInputs, cs *v1beta1.CanaryStatus) *Result {
	emit(in.Recorder, in.ISVC, corev1.EventTypeNormal, EventReasonCanaryTargetReverted,
		"%s canary toward revision %s ended: the spec returned to revision %s and every Instance already runs it, so nothing is left to roll", in.Component, cs.CanaryRevisionHash, in.CanaryRevisionHash)
	if unitHoldsSplit(in) {
		applyCutoverTraffic(in)
	}
	return clearRecord(in, v1beta1.RolloutPhaseStable)
}

// endTakenBackCanary ends a record whose push the operator took back while
// Instances still run another revision: the unit keeps no record, those
// Instances roll back under the run with no canary step, with the phase
// reading RollingBack until the last of them lands, and each member's split
// moves onto the stable revision where a pod of it serves, resting on the
// revision that serves until one does.
func endTakenBackCanary(in ReconcileInputs, cs *v1beta1.CanaryStatus) *Result {
	emit(in.Recorder, in.ISVC, corev1.EventTypeNormal, EventReasonCanaryTargetReverted,
		"%s canary toward revision %s ended: the spec returned to its stable revision %s while Instances still run another revision; they roll back to it with no canary step, and the traffic follows the Instances that serve it", in.Component, cs.CanaryRevisionHash, in.CanaryRevisionHash)
	settleTrafficOnTarget(in)
	return clearRecord(in, idlePhase(in))
}

// clearRecord removes the unit's record and projects the phase a unit with
// no record reads: Stable, as before its push, or RollingBack while the
// Instances of a roll the operator took back are still coming back.
func clearRecord(in ReconcileInputs, phase v1beta1.RolloutPhase) *Result {
	rollout.SetCanaryStatusFor(&in.ISVC.Status, in.Component, nil)
	if !anyUnitRecord(in.ISVC) {
		in.ISVC.Status.Canary = nil
	}
	setPhase(in.ISVC, in.Component, phase)
	return &Result{Active: false}
}

// sitsOut reports a unit under an open run that records every member at
// its stable revision: the run offers it no target, so no step starts.
func sitsOut(in ReconcileInputs) bool {
	return in.RunActive && in.TargetID != "" && in.CanaryRevisionHash != "" && !unitRetargeted(in)
}

// idlePhase is the phase a unit with no record projects: RollingBack while
// it sits a run out with a pod still on a revision the run does not record
// for its member, the Instances of a roll the operator took back rolling
// back with no step, else Stable. The projector holds none of a unit that
// reads RollingBack with no record; a unit that sits a run out whole keeps
// step 0's hold, which the next push lands on before its record is written.
func idlePhase(in ReconcileInputs) v1beta1.RolloutPhase {
	if sitsOut(in) && instancesTrail(in) {
		return v1beta1.RolloutPhaseRollingBack
	}
	return v1beta1.RolloutPhaseStable
}

// instancesTrail reports whether a pod of the unit, ready or not, runs a
// revision other than the one the run records for its member.
func instancesTrail(in ReconcileInputs) bool {
	if podsOff(memberPods(in, in.Component, in.PerRevisionPods), in.CanaryRevisionHash) {
		return true
	}
	for c, m := range in.Secondaries {
		if podsOff(memberPods(in, c, in.GroupReadyPerRevisionPods[c]), m.CanaryRevisionHash) {
			return true
		}
	}
	return false
}

// memberPods is every pod of a member per revision when the dispatcher
// counted them, else the ready pods that stand in for them.
func memberPods(in ReconcileInputs, c v1beta1.ComponentType, ready map[string]int32) map[string]int32 {
	if total, ok := in.GroupTotalPerRevisionPods[c]; ok {
		return total
	}
	return ready
}

// podsOff reports a pod on a revision other than the one named.
func podsOff(pods map[string]int32, revision string) bool {
	for h, n := range pods {
		if h != "" && h != revision && n > 0 {
			return true
		}
	}
	return false
}

// unitHoldsSplit reports whether any member of the unit has a traffic split
// written.
func unitHoldsSplit(in ReconcileInputs) bool {
	if len(in.ISVC.Status.Components[in.Component].Traffic) > 0 {
		return true
	}
	for c := range in.Secondaries {
		if len(in.ISVC.Status.Components[c].Traffic) > 0 {
			return true
		}
	}
	return false
}

// planHoldClause is the tail of the run-lost event while the run layer holds
// the plan (RolloutPlanReady False): the reason and message the operator
// must act on, what still applies under the hold and what waits. Empty
// while the plan composes, since the run layer then takes the record over
// on its next pass.
func planHoldClause(isvc *v1beta1.InferenceService) string {
	cond := isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanReadyCondition))
	if cond == nil || cond.Status != corev1.ConditionFalse {
		return ""
	}
	return fmt.Sprintf("; the run layer cannot take it over because the rollout plan does not compose (%s: %s): fix the plan, or undo the plan edit, and the canary continues; a rollback request (ome.io/rollout-rollback) still applies under this hold, while a promote waits until the plan composes again", cond.Reason, cond.Message)
}

// applyHolds runs the verbs and holds of the canary grid in the order they
// take precedence: the end of a roll the operator took back, a live
// canary's retarget, the rollback hold, a rollback request, the Failed
// hold, the done sentinel, then arming. It returns the status the step
// machine runs against (freshly armed when it was nil) and a non-nil
// Result when the pass ends inside a hold.
func applyHolds(ctx context.Context, in ReconcileInputs, g *v1beta1.RolloutGroup, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, targetChanged bool, take func(string)) (*v1beta1.CanaryStatus, *Result, error) {
	steps := len(plan.Steps)

	// The request a unit applied is its own through its hold and is released
	// when it is observed removed; the hold rests on the rejected hash and
	// stays.
	if cs != nil && cs.RollbackRequested && !isRollbackRequested(in.ISVC) {
		cs.RollbackRequested = false
	}

	// The operator set the spec back to the unit's stable revision while
	// Instances still run another: the run opened for them records every
	// member at its stable, so the roll the record names is gone. The
	// record ends and the Instances roll back with no canary step; a unit
	// holding a rejection keeps its hold instead.
	if rollTakenBack(in, cs) {
		return nil, endTakenBackCanary(in, cs), nil
	}

	// A live canary binds to the IR's authoritative target before any verb
	// is read. A changed target restarts the staged plan with fresh
	// per-target state while preserving the original stable identity. With
	// no run open and every member already resting on the target it has
	// now, the record names a roll that no longer exists and ends.
	if cs != nil && int(cs.CurrentStep) < steps && cs.RolledBackRevisionHash == "" &&
		rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, steps) != rollout.CanaryStateFailed {
		if cs.StableRevisionHash == "" && in.StableRevisionHash != "" {
			cs.StableRevisionHash = in.StableRevisionHash
		}
		if shouldRearm(in, cs, targetChanged, rearmLive) {
			resetCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, in.CanaryRevisionHash, in.Now)
		} else if targetReverted(in, g, cs) {
			return nil, endCanary(in, cs), nil
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
			} else if !stoppedWithoutStable(cs) {
				why := "the canary is rolling back"
				if in.ISVC.Status.Components[in.Component].RolloutPhase == v1beta1.RolloutPhaseRolledBack {
					why = "the canary is held rolled back"
				}
				ignoreForce(in, cs, take, why)
				return cs, reconcileRollback(in, cs), nil
			}
		} else if rollbackRequestPending(in.ISVC) && (int(cs.CurrentStep) < steps || cutoverRolling(in, cs, steps)) {
			// A rollback is the operator's decision about the canary, parked
			// or not: it ends a Failed park and drains the rejected revision,
			// toward an empty stable revision too, which is said out loud. It
			// is taken until the cutover lands: a unit at its sentinel whose
			// last stable instance is still rolling can still return, and the
			// record keeps the sentinel beside the rejection.
			// The request is this unit's from here: it stays on the object
			// through the hold, and no other unit acts on it.
			warnEmptyRollbackTargets(in, cs)
			cs.RolledBackRevisionHash = cs.CanaryRevisionHash
			cs.RollbackRequested = true
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
		} else if targetReverted(in, g, cs) {
			// The stop holds the roll where it stands; a roll that no longer
			// exists leaves nothing to hold.
			return nil, endCanary(in, cs), nil
		} else {
			// The recorded failure is the record, and the phase is projected
			// from it every pass, so a status write that dropped the phase
			// cannot end the hold. A Failed phase with no recorded failure
			// is a stop written from outside the controller: it is kept,
			// recorded with its own reason and reported once with
			// CanaryRollHeld, as any stop. The split it left follows the
			// pods only where a stable revision has none serving.
			parkFailed(in, cs, v1beta1.CanaryFailurePhaseWritten)
			holdStoppedTraffic(in, cs)
			refuseRollback(in, cs, take)
			ignoreForce(in, cs, take, "the canary is parked Failed")
			return cs, (&Result{Active: true}).wake(in.ParkedRequeue), nil
		}
	}

	// Done sentinel: a finished canary keeps its status at CurrentStep ==
	// len(steps). EffectivePartition maps that to partition 0; a nil status
	// would re-default to step 0's partition and hold instances back on the
	// old revision after completion. The sentinel is the release of the held
	// floor, so the unit finishes the cutover from here: it stays Promoting
	// while the last stable instance rolls and reads Stable once every
	// member serves on its target alone. A rollback request reaches this
	// branch only once the cutover has landed, with no stable revision left
	// to return to. A Failed phase written onto a finished unit from outside
	// the controller reads Done (StateOf), so the pass projects the phase
	// from the record again and holds nothing.
	if cs != nil && int(cs.CurrentStep) >= steps {
		if shouldRearm(in, cs, targetChanged, rearmDone) {
			resetCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, in.CanaryRevisionHash, in.Now)
		} else {
			// A canary can complete while the durable promote record is still
			// waiting on observed annotation absence; converge it here since
			// a done canary evaluates no gates.
			syncPromotedThrough(in, cs, take)
			ignoreRollback(in, cs, take)
			return cs, finishCutover(in, cs, plan, true), nil
		}
	}

	// A live canary with no pinned run holds in place: opening, adopting or
	// parking the run is the run layer's job, and stepping an unpinned plan
	// is the same hazard arming one would be. The update gates already hold
	// the forward roll fail-closed underneath. When the run layer holds the
	// plan, the event says why, so the operator knows the hold ends with the
	// plan and not with a promote.
	if cs != nil && !in.RunActive {
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryRunLost,
			"%s canary has no pinned rollout run; holding at step %d until the run layer reopens or adopts it%s", in.Component, cs.CurrentStep, planHoldClause(in.ISVC))
		partition := stepPartition(plan.Steps[cs.CurrentStep], in.DesiredReplicas)
		return cs, (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
	}

	// Arming: a new canary starts only under a pinned run, with a stable
	// revision to shift traffic from, toward a target the unit does not
	// already serve. A unit the run did not retarget sits the run out: arming
	// it would walk a full ladder, spend its analysis budget on a no-op, and
	// could reach a rollback for a rollout that never happened. While such a
	// run is open its Instances may still be rolling back to the revision
	// the run names, after a push the operator took back: the phase says so
	// while they do, and a split a member still carries follows the pods
	// onto that revision.
	if cs == nil {
		ignoreRollback(in, nil, take)
		if in.CanaryRevisionHash == "" || in.StableRevisionHash == "" ||
			(in.TargetID == "" && readyCanaryCapacity(in) >= in.DesiredReplicas) ||
			!unitRetargeted(in) {
			if sitsOut(in) {
				settleTrafficOnTarget(in)
			}
			if in.CanaryRevisionHash != "" && in.DesiredReplicas > 0 {
				setPhase(in.ISVC, in.Component, idlePhase(in))
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
