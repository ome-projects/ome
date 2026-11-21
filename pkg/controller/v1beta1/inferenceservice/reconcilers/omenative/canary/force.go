package canary

import (
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// forceKeys are the annotations of a force request. They are removed once no
// canary unit of the service has a use left for them.
var forceKeys = []string{
	constants.RolloutForceModeAnnotation,
	constants.RolloutForceRunAnnotation,
	constants.RolloutForceReasonAnnotation,
}

// handleForce runs ahead of the canary's holds. It ends a running force whose
// run, request or time limit is gone, starts a force the operator asked for,
// and drives a running disruptive force. handled reports that the pass ends
// here; otherwise the canary continues as a normal canary.
func handleForce(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, take func(string)) (res *Result, handled bool) {
	if cs == nil {
		return nil, false
	}
	mode, requested := in.ISVC.Annotations[constants.RolloutForceModeAnnotation]
	value := in.ISVC.Annotations[constants.RolloutForceRunAnnotation]
	runID := v1beta1.ActiveRolloutRunID(in.ISVC)

	if cs.Forced.Running() {
		if cause, why := forceEndCause(in, cs); cause != "" {
			endForce(in, cs, plan, cause, why, take)
			return nil, false
		}
		return runDisruptive(in, cs, plan, take), true
	}

	if !requested {
		return nil, false
	}
	if why, everyUnit := forceRefusal(in, cs, plan, mode, value, runID); why != "" {
		// A refusal that holds for this unit alone leaves the request to a
		// unit that can still use it.
		if everyUnit || forceSpendable(in, value) {
			refuseForce(in, why, take)
		}
		return nil, false
	}
	startForce(in, cs, value)
	return runDisruptive(in, cs, plan, take), true
}

// forceEndCause says why a running force must end now, and how; an empty
// cause means it is still in effect. The run, the request, the rollback,
// the time limit and the configuration decide it, read from the object and
// the pass's limits alone, so the step that writes the replicas and the
// canary pass end a force on the same facts.
func forceEndCause(in ReconcileInputs, cs *v1beta1.CanaryStatus) (v1beta1.CanaryForceEndReason, string) {
	mode, requested := in.ISVC.Annotations[constants.RolloutForceModeAnnotation]
	value, bound := in.ISVC.Annotations[constants.RolloutForceRunAnnotation]
	switch {
	case !in.RunActive || v1beta1.ActiveRolloutRunID(in.ISVC) != cs.Forced.RunID:
		return v1beta1.CanaryForceEndedByRunClose, "the rollout run it was bound to is no longer open"
	case cs.RolledBackRevisionHash != "" || rollbackRequestPending(in.ISVC):
		return v1beta1.CanaryForceEndedByRollback, "the operator rolled back"
	case !requested || !bound:
		return v1beta1.CanaryForceEndedByRemoval, "the force annotations were removed; the run continues as a normal canary"
	case mode != string(cs.Forced.Mode) || value != cs.Forced.RunID:
		return v1beta1.CanaryForceEndedByChange, "the force annotations were changed; the run continues as a normal canary and is not forced again"
	case in.ForceMaxDuration <= 0 || !in.Now.Before(cs.Forced.StartedAt.Add(in.ForceMaxDuration)):
		return v1beta1.CanaryForceExpired, "the time limit rollout.forceComplete.maxDuration passed, or the block was removed; the run continues as a normal canary"
	}
	return "", ""
}

// SettleForces ends every running force the pass no longer allows, before
// the replicas are written: a force whose run, request, time limit or
// configuration is gone, or whose rollout is paused, must not project its
// partition 0 and 100% budget for one more pass while old Instances still
// stand. It emits the same events and removes the same annotations as the
// canary pass, deleting them in memory so nothing later in the pass reads
// them again, and returns the annotations to remove.
func SettleForces(isvc *v1beta1.InferenceService, policies rollout.Policies, recorder record.EventRecorder, now time.Time, maxDuration time.Duration) []string {
	var consumed []string
	take := func(key string) {
		delete(isvc.Annotations, key)
		consumed = append(consumed, key)
	}
	primaries := map[v1beta1.ComponentType]bool{}
	for _, g := range rollout.CanaryGroups(isvc, policies) {
		primary := rollout.PrimaryOf(g)
		primaries[primary] = true
		cs := rollout.CanaryStatusFor(&isvc.Status, primary)
		if cs == nil || !cs.Forced.Running() || g.Canary == nil {
			continue
		}
		in := ReconcileInputs{ISVC: isvc, Policies: policies, Group: g, Component: primary, Recorder: recorder,
			Now: now, ForceMaxDuration: maxDuration, RunActive: v1beta1.RolloutRunActive(isvc)}
		if paused, _ := constants.RolloutPauseState(isvc.Annotations); paused {
			endForce(in, cs, g.Canary, v1beta1.CanaryForceEndedByPause, "the operator paused the rollout", take)
			continue
		}
		if cause, why := forceEndCause(in, cs); cause != "" {
			endForce(in, cs, g.Canary, cause, why, take)
		}
	}
	endForcesWithoutGroup(isvc, policies, recorder, now, primaries, take)
	return consumed
}

// endForcesWithoutGroup ends a running force on a unit that no canary group
// has any more, as when a new run's plan changed or removed its group:
// nothing else would end it, and the unit's record is released later in the
// pass. It ends as RunClosed, since a plan change applies only at a new run,
// and says why. Each Component's own record is read, never another member's
// through the unit lookup, and a member of a current canary group is left
// to that group's pass.
func endForcesWithoutGroup(isvc *v1beta1.InferenceService, policies rollout.Policies, recorder record.EventRecorder, now time.Time, primaries map[v1beta1.ComponentType]bool, take func(string)) {
	components := make([]v1beta1.ComponentType, 0, len(isvc.Status.Components))
	for c := range isvc.Status.Components {
		components = append(components, c)
	}
	slices.Sort(components)
	for _, c := range components {
		cs := isvc.Status.Components[c].Canary
		if primaries[c] || cs == nil || !cs.Forced.Running() || rollout.CanaryGroupFor(isvc, policies, c) != nil {
			continue
		}
		in := ReconcileInputs{ISVC: isvc, Policies: policies, Component: c, Recorder: recorder, Now: now,
			RunActive: v1beta1.RolloutRunActive(isvc)}
		endForce(in, cs, &v1beta1.GroupCanary{}, v1beta1.CanaryForceEndedByRunClose,
			"the rollout plan no longer has the canary group the force ran on", take)
	}
}

// forceRefusal says why a force request cannot start on this unit, empty when
// it can. everyUnit reports a reason that holds for the whole service. A
// request admission let through can still find its run closed, rolled back or
// complete by the time the executor reads it.
func forceRefusal(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, mode, value, runID string) (why string, everyUnit bool) {
	switch {
	case in.ForceMaxDuration <= 0:
		return "the rollout.forceComplete block of the inferenceservice-config ConfigMap is absent or has no valid maxDuration", true
	case mode != constants.RolloutForceModeDisruptive:
		return "mode " + mode + " is not one this version of OME runs; it runs only " + constants.RolloutForceModeDisruptive, true
	case !in.RunActive || runID == "":
		return "no rollout run is open", true
	case value == "":
		return "it names no run in " + constants.RolloutForceRunAnnotation, true
	case value != runID:
		return "it names run " + value + ", but the open run is " + runID, true
	case cs.RolledBackRevisionHash != "" || rollbackRequestPending(in.ISVC):
		return "the canary is rolling back", false
	case !unitRetargeted(in):
		return "the open run leaves this canary group on its stable revision", false
	case forceSpent(cs, value):
		return "a force already ran for run " + value + " and ended (" + string(cs.Forced.EndReason) + "); a run is forced at most once", false
	case int(cs.CurrentStep) >= len(plan.Steps):
		return "the canary has already passed its last step", false
	}
	return "", false
}

// forceSpent reports that the unit already ran a force for runID that ended
// by any cause but the operator removing it. A request still visible after
// that end is the same request, left behind when its removal did not land,
// so it does not start a second force. A force that ended because a key was
// gone may be asked for again; one that ended because a key was edited may
// not, so correcting a mistyped key never replaces every Instance twice.
func forceSpent(cs *v1beta1.CanaryStatus, runID string) bool {
	return cs != nil && cs.Forced != nil && !cs.Forced.Running() &&
		cs.Forced.RunID == runID && cs.Forced.EndReason != v1beta1.CanaryForceEndedByRemoval
}

// forceSpendable reports whether this unit may remove a force request for
// runID: no other canary unit of the service still runs one for it, or can
// still start one. The request belongs to the whole service, and every unit
// reads it from the same object.
func forceSpendable(in ReconcileInputs, runID string) bool {
	for _, g := range rollout.CanaryGroups(in.ISVC, in.Policies) {
		if rollout.PrimaryOf(g) == in.Component || g.Canary == nil {
			continue
		}
		if forceOpenOn(in.ISVC, g, runID) {
			return false
		}
	}
	return true
}

// forceOpenOn reports whether canary group g runs a force for runID, or can
// still start one: it has steps left, is not rolling back, and has not spent
// a force on the run. A group the run leaves on its stable revision never
// uses the request, whatever record an earlier run left on it. A group with
// no canary record can start one only when the open run starts a canary on
// it, so a first rollout with no earlier revision never uses it either.
func forceOpenOn(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, runID string) bool {
	primary := rollout.PrimaryOf(g)
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	switch {
	case cs == nil:
		return rollout.RunStartsCanary(isvc, g)
	case cs.Forced.Running():
		return cs.Forced.RunID == runID
	case !v1beta1.RolloutRunRetargets(isvc, g.Components):
		return false
	case cs.RolledBackRevisionHash != "" || rollbackRequestPending(isvc):
		return false
	case forceSpent(cs, runID):
		return false
	}
	return int(cs.CurrentStep) < len(g.Canary.Steps)
}

// RefuseUnclaimedForce removes a force request no canary group of the
// service can act on: the service has no canary group, or the open run moves
// no group that has a canary record and starts a canary on none, as on a
// first rollout with no earlier version. Each group's own pass
// answers a request it can read; this covers the request no group reads, so
// it does not stay on the object after admission let it through. It returns
// the annotations to remove.
func RefuseUnclaimedForce(isvc *v1beta1.InferenceService, policies rollout.Policies, recorder record.EventRecorder) []string {
	if _, requested := isvc.Annotations[constants.RolloutForceModeAnnotation]; !requested {
		return nil
	}
	groups := rollout.CanaryGroups(isvc, policies)
	for _, g := range groups {
		primary := rollout.PrimaryOf(g)
		cs := rollout.CanaryStatusFor(&isvc.Status, primary)
		if (cs != nil && v1beta1.RolloutRunRetargets(isvc, g.Components)) || rollout.RunStartsCanary(isvc, g) {
			return nil
		}
	}
	why := "the open rollout run starts no canary (no canary group moves from an earlier version)"
	if len(groups) == 0 {
		why = "the service has no canary group, so its rollout has no canary to force"
	}
	emit(recorder, isvc, corev1.EventTypeWarning, EventReasonRolloutForceRefused,
		"%s: %s; the request is removed", constants.RolloutForceModeAnnotation, why)
	var keys []string
	for _, key := range forceKeys {
		if _, ok := isvc.Annotations[key]; ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// refuseForce says why a force request cannot run and removes it.
func refuseForce(in ReconcileInputs, why string, take func(string)) {
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonRolloutForceRefused,
		"%s on %s: %s; the request is removed", constants.RolloutForceModeAnnotation, in.Component, why)
	for _, key := range forceKeys {
		if _, ok := in.ISVC.Annotations[key]; ok {
			take(key)
		}
	}
}

// startForce records a force on the unit. A stop the force finds is cleared:
// the force exists to move a run its timers stopped.
func startForce(in ReconcileInputs, cs *v1beta1.CanaryStatus, runID string) {
	reason := in.ISVC.Annotations[constants.RolloutForceReasonAnnotation]
	cs.Forced = &v1beta1.CanaryForce{
		Mode:               v1beta1.CanaryForceDisruptive,
		Reason:             reason,
		RunID:              runID,
		CanaryRevisionHash: cs.CanaryRevisionHash,
		StartedAt:          metav1.Time{Time: in.Now},
	}
	cs.Failed = nil
	cs.CapacityWaitSince = nil
	cs.PreStepHold = false
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonRolloutForceStarted,
		"%s canary forced to 100%% (Disruptive) for run %s: every Instance still on revision %s is replaced at once, and the service serves nothing until revision %s is Ready; reason: %s",
		in.Component, runID, cs.StableRevisionHash, cs.CanaryRevisionHash, reason)
}

// endForce closes a running force with its cause and removes the request
// once no other unit has a use left for it. A force that ends before the last
// step hands the unit back to that step as a step not yet serving: the phase
// the force wrote would read as the final drain, so the unit reads Pending,
// waits for the step's capacity, and restarts the step's clock when its split
// serves again.
func endForce(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, cause v1beta1.CanaryForceEndReason, why string, take func(string)) {
	runID := cs.Forced.RunID
	cs.Forced.EndedAt = &metav1.Time{Time: in.Now}
	cs.Forced.EndReason = cause
	eventType := corev1.EventTypeNormal
	if cause != v1beta1.CanaryForceCompleted {
		eventType = corev1.EventTypeWarning
	}
	emit(in.Recorder, in.ISVC, eventType, EventReasonRolloutForceEnded,
		"%s canary force for run %s ended (%s) after %s: %s",
		in.Component, runID, cause, in.Now.Sub(cs.Forced.StartedAt.Time).Round(time.Second), why)
	if cause != v1beta1.CanaryForceCompleted && int(cs.CurrentStep) < len(plan.Steps) &&
		in.ISVC.Status.Components[in.Component].RolloutPhase == v1beta1.RolloutPhasePromoting {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePending)
	}
	// A request naming another run is not this force's: the operator may
	// already have asked to force the run that replaced it.
	if other, ok := in.ISVC.Annotations[constants.RolloutForceRunAnnotation]; ok && other != runID {
		return
	}
	if !forceSpendable(in, runID) {
		return
	}
	for _, key := range forceKeys {
		if _, ok := in.ISVC.Annotations[key]; ok {
			take(key)
		}
	}
}

// runDisruptive drives a running disruptive force. Every Instance of the unit
// is released at once (EffectivePartition reads the running force as
// partition 0, and the projector sets the update strategy's budget to 100%),
// and the traffic names the new revision alone, since nothing else is meant
// to serve. No timer runs: a slow boot waits until the force's time limit. A
// new pod that keeps dying after it served stops the canary as any crash
// does. The traffic then goes back to the old pods, unless some member
// keeps no old Instance, when it stays on the new revision.
func runDisruptive(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, take func(string)) *Result {
	cs.CapacityWaitSince = nil
	if in.Crash != nil {
		endForce(in, cs, plan, v1beta1.CanaryForceEndedByCrash, "a new pod keeps dying after it served; roll back or push a fixed revision", take)
		return parkOnCrash(in, cs)
	}
	// The record keeps the step's share, so a force that ends early hands
	// the step back with its own split and its own shift to 100%.
	weight := cs.ObservedTrafficWeight
	applyCutoverTraffic(in)
	cs.ObservedTrafficWeight = weight
	if !cutoverComplete(in) {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePromoting)
		return (&Result{Active: true, Partition: 0}).wake(in.Requeue)
	}
	endForce(in, cs, plan, v1beta1.CanaryForceCompleted, "every Instance serves the new revision", take)
	cs.CurrentStep = int32(len(plan.Steps))
	return finishCutover(in, cs, plan, false)
}

// forcedThisCanary reports whether a disruptive force drove the unit's
// current canary revision, running or ended. Another unit's push opens a new
// run but keeps this canary, so it still counts; a push of this unit toward
// another revision does not.
func forcedThisCanary(cs *v1beta1.CanaryStatus) bool {
	return cs != nil && cs.Forced != nil && cs.Forced.Mode == v1beta1.CanaryForceDisruptive &&
		cs.Forced.CanaryRevisionHash != "" && cs.Forced.CanaryRevisionHash == cs.CanaryRevisionHash
}

// forceUndoneByRollback marks a force as no longer driving the canary once
// the canary rolls back: the rollback brings the old Instances back, so a
// later canary on the same revision crashes as one no force drove.
func forceUndoneByRollback(cs *v1beta1.CanaryStatus) {
	if cs.Forced != nil {
		cs.Forced.CanaryRevisionHash = ""
	}
}

// disruptiveForceRunning reports whether a disruptive force is in effect on
// the unit whose record is cs.
func disruptiveForceRunning(cs *v1beta1.CanaryStatus) bool {
	return cs != nil && cs.Forced.Running() && cs.Forced.Mode == v1beta1.CanaryForceDisruptive
}

// pausedForce answers a force while the rollout is paused: a running force
// ends, and a request no force runs for is refused, so a request left in
// place does not delete every old Instance the moment the pause clears.
func pausedForce(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, take func(string), why string) {
	if cs != nil && cs.Forced.Running() {
		endForce(in, cs, plan, v1beta1.CanaryForceEndedByPause, "the operator paused the rollout", take)
		return
	}
	if _, requested := in.ISVC.Annotations[constants.RolloutForceModeAnnotation]; requested {
		refuseForce(in, why, take)
	}
}
