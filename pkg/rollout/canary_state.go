package rollout

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// CanaryState is the executor's state for one canary unit: the row of the
// canary state × event table. It is derived from the persisted CanaryStatus,
// plus the last projected phase for the one split the status does not record
// on its own (capacity gate unmet, split serving, final drain).
type CanaryState string

const (
	// CanaryStateIdle: no canary status; the unit is not armed.
	CanaryStateIdle CanaryState = "Idle"
	// CanaryStateStaging: armed, the step's capacity gate is unmet.
	CanaryStateStaging CanaryState = "Staging"
	// CanaryStateServing: the step's split is live and its gate decides.
	CanaryStateServing CanaryState = "Serving"
	// CanaryStatePreHold: a repin clamped the ladder; traffic waits for a
	// matching promote before it may rise.
	CanaryStatePreHold CanaryState = "PreHold"
	// CanaryStateDraining: the final step serves 100%; the stable revision
	// drains until the window elapses.
	CanaryStateDraining CanaryState = "Draining"
	// CanaryStateDone: the done sentinel; the held floor is released, and the
	// unit reads Stable once every instance serves the canary revision.
	CanaryStateDone CanaryState = "Done"
	// CanaryStateRollingBack: a rejected revision is draining back to stable.
	CanaryStateRollingBack CanaryState = "RollingBack"
	// CanaryStateRolledBack: the revert is complete and the rejected
	// revision is held out until a different target appears.
	CanaryStateRolledBack CanaryState = "RolledBack"
	// CanaryStateFailed: parked at the current step; the stable revision
	// keeps serving until an operator acts or a new target appears.
	CanaryStateFailed CanaryState = "Failed"
)

// StateOf resolves a unit's state. stepCount is the pinned ladder length.
// A rollback request outranks a failure park: rolling back is the operator's
// explicit decision about a parked canary. A Failed phase with no recorded
// failure is status written from outside the controller: it stops a unit
// short of its last step, and a unit at or past its done mark outranks it,
// since a finished unit has nothing a stop could protect.
func StateOf(cs *v1beta1.CanaryStatus, phase v1beta1.RolloutPhase, stepCount int) CanaryState {
	if cs == nil {
		return CanaryStateIdle
	}
	// A rollback that found no stable revision to return to is a park, not a
	// revert in progress: the rejected hash stays recorded, nothing drains.
	if cs.Failed != nil && cs.Failed.Reason == v1beta1.CanaryFailureStableRevisionMissing {
		return CanaryStateFailed
	}
	if cs.RolledBackRevisionHash != "" {
		if phase == v1beta1.RolloutPhaseRolledBack {
			return CanaryStateRolledBack
		}
		return CanaryStateRollingBack
	}
	if cs.Failed != nil {
		return CanaryStateFailed
	}
	if stepCount > 0 && int(cs.CurrentStep) >= stepCount {
		return CanaryStateDone
	}
	if phase == v1beta1.RolloutPhaseFailed {
		return CanaryStateFailed
	}
	if cs.PreStepHold {
		return CanaryStatePreHold
	}
	switch phase {
	case v1beta1.RolloutPhasePaused:
		// A final gate held at 100% traffic is the drain waiting, not a split.
		if stepCount > 0 && int(cs.CurrentStep) == stepCount-1 && cs.ObservedTrafficWeight == 100 {
			return CanaryStateDraining
		}
		return CanaryStateServing
	case v1beta1.RolloutPhaseCanarying:
		return CanaryStateServing
	case v1beta1.RolloutPhasePromoting:
		return CanaryStateDraining
	default:
		return CanaryStateStaging
	}
}

// StepOf is the step a unit's record is read at. A negative index is an external
// status write and reads as past the last step once the unit is Stable, else as
// the last step the recorded traffic weight reached: nothing already done re-runs.
func StepOf(cs *v1beta1.CanaryStatus, phase v1beta1.RolloutPhase, steps []v1beta1.RolloutGroupStep) int32 {
	if cs.CurrentStep >= 0 {
		return cs.CurrentStep
	}
	if phase == v1beta1.RolloutPhaseStable {
		return int32(len(steps))
	}
	step := int32(0)
	for i := range steps {
		if steps[i].Traffic <= cs.ObservedTrafficWeight {
			step = int32(i)
		}
	}
	return step
}

// ClampStep keeps a record's step inside steps once ranUnder, the list the
// step indexes, is replaced by it. A unit past the end of ranUnder stays past
// the end of steps; a live index beyond the last of steps lands on that last
// step; and the pre-step hold is set when the step at the kept index serves
// more traffic than the record does, so the raise waits for a promote. A
// negative index is read as StepOf reads it, against ranUnder. A record with
// no steps to land on is left as it is.
func ClampStep(cs *v1beta1.CanaryStatus, phase v1beta1.RolloutPhase, ranUnder, steps []v1beta1.RolloutGroupStep) {
	if cs == nil || len(steps) == 0 {
		return
	}
	step := StepOf(cs, phase, ranUnder)
	if len(ranUnder) > 0 && int(step) >= len(ranUnder) {
		cs.CurrentStep = int32(len(steps))
		return
	}
	cs.CurrentStep = step
	if int(cs.CurrentStep) >= len(steps) {
		cs.CurrentStep = int32(len(steps)) - 1
	}
	if steps[cs.CurrentStep].Traffic > cs.ObservedTrafficWeight {
		cs.PreStepHold = true
	}
}

// Terminal reports the two parked states an operator must act on.
func (s CanaryState) Terminal() bool {
	return s == CanaryStateFailed || s == CanaryStateRolledBack
}
