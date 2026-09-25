package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// pauseHold is the operator pause's side of the shared hold contract. A
// pause withholds the next operation and the next step; the token is
// what makes the resulting wait visible on the attempt, so `kubectl
// describe` says who is holding it and the InstanceReadyTimeout clock
// does not run through the hold.
//
// The weakest authority on the row. Quota, the scheduler, the gang
// scheduler and a node that went silent each hold an attempt for a wait
// only they can end, and a pause is not a reason to stop reporting it; a
// surge reporting its own source out of rotation outranks it too — "this
// Instance is serving nothing" is the more urgent fact. When another
// authority speaks the pause reports nothing and still does its work,
// which is not planning the next step.
var pauseHold = authority{
	token:    types.WaitingReasonPaused,
	mayOwn:   pauseHeldOperation,
	everyRow: true,
	report:   reportPause,
}

// reportPause reads the pause off the plan: it holds every attempt in a
// phase the timeout backstop bounds that it is actually what stands in
// front of, and releases exactly its own token once the pause is
// cleared.
//
// A paused row whose owner the pause does not hold keeps whatever it
// carries rather than being released: the pause is still in force, and
// the token it left belongs to the episode, not to this pass's owner.
func reportPause(_ context.Context, in PassInput, row Row) (reading, error) {
	if !in.Plan.Paused {
		return reading{release: true}, nil
	}
	if !types.TransientPhase(row.Status.Phase) ||
		!pauseHeldOperation(types.Owner(&row.Status), in.Plan) {
		return reading{}, nil
	}
	return reading{waiting: true}, nil
}

// pauseHeldOperation reports whether a pause is what holds this attempt
// back, which is not true of every owner a paused Component carries:
//
//   - delete: scale-down keeps running while paused, so its clock must
//     keep running with it.
//   - create: a committed create finishes the set it started; what the
//     pause holds is the operation that would come after it.
//   - restart: repair keeps running under a standard pause. Only a frozen
//     pause holds it, and then only past the step under way.
//   - update and migrate: held.
//   - none: there is no attempt for a pause to stand in front of.
func pauseHeldOperation(owner types.RowOwner, plan types.ComponentPlan) bool {
	switch owner {
	case types.OwnerNone, types.OwnerDelete, types.OwnerCreate:
		return false
	case types.OwnerRestart:
		return plan.PauseFreeze
	}
	return true
}
