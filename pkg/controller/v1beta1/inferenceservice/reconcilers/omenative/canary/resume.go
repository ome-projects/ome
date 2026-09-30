package canary

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// terminalPhase reports whether a Component's phase is one of the two parked
// terminals the step machine never leaves on its own: a Failed step and a
// completed rollback both hold until an operator acts.
func terminalPhase(p v1beta1.RolloutPhase) bool {
	return p == v1beta1.RolloutPhaseFailed || p == v1beta1.RolloutPhaseRolledBack
}

// resumeRequest is a parsed ome.io/rollout-resume value: the parked canary
// revision hash to resume, and the Component it is scoped to (empty when the
// value is a bare hash).
type resumeRequest struct {
	component v1beta1.ComponentType
	hash      string
}

// parseResume reads an ome.io/rollout-resume value. Admission validates the
// shape; this parse degrades to "not a request" on anything malformed so a
// direct write that bypasses admission cannot reach the verb.
func parseResume(v string) (resumeRequest, bool) {
	if comp, hash, scoped := strings.Cut(v, "="); scoped {
		if comp == "" || hash == "" {
			return resumeRequest{}, false
		}
		return resumeRequest{component: v1beta1.ComponentType(comp), hash: hash}, true
	}
	if v == "" {
		return resumeRequest{}, false
	}
	return resumeRequest{hash: v}, true
}

// addressesUnit reports whether this verb is aimed at the canary unit driven
// through Component c, whose canary record is cs.
//
// A scoped value names one unit outright (a member names the unit it belongs
// to), so that unit owns the verb in every branch, including a rejection,
// because a verb that is never consumed retries forever. A bare hash is
// addressed to whichever unit's canary carries it; a unit the hash does not
// name leaves the annotation for the unit that owns it, so one canary unit's
// dispatch cannot eat a verb aimed at another's.
func (r resumeRequest) addressesUnit(c v1beta1.ComponentType, cs *v1beta1.CanaryStatus) bool {
	if r.component != "" {
		return rollout.CanaryUnit(r.component) == rollout.CanaryUnit(c)
	}
	return cs != nil && r.hash == cs.CanaryRevisionHash
}

// addressesAnyUnit reports whether some canary unit of the InferenceService
// owns this verb. A verb no unit owns has nothing to act on, now or later.
func (r resumeRequest) addressesAnyUnit(isvc *v1beta1.InferenceService) bool {
	for _, g := range rollout.CanaryGroups(isvc) {
		primary := rollout.PrimaryOf(g)
		if r.addressesUnit(primary, rollout.CanaryStatusFor(&isvc.Status, primary)) {
			return true
		}
	}
	return false
}

// handleResume applies the one-shot ome.io/rollout-resume verb: clear this
// Component's terminal RolloutPhase and re-enter the step machine at step 0
// against the revision that is already parked, without minting a revision.
//
// It reports whether it resumed. A resume does NOT fall through to the step
// machine on the same pass. Resuming a completed rollback clears the IR's
// rollback target, and until the IR republishes its status the observed
// target still names the STABLE revision — running the ladder against that
// would program the stable revision as its own canary. Returning here lets
// the dispatch layer clear the rollback signal and the IR's freshness gate
// hold the next passes until the target flips back. The Failed path has no
// such inversion, but takes the same one-requeue detour so the verb has a
// single behaviour.
//
// The verb itself is handed back through take and removed by the controller
// after the status flush that carries the re-arm: a lost flush then leaves
// the verb in place to be applied again, and a verb still visible after a
// successful flush finds nothing parked and is consumed with no effect.
func handleResume(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus, take func(string)) (bool, error) {
	value := in.ISVC.Annotations[constants.RolloutResumeAnnotation]
	req, ok := parseResume(value)
	if !ok {
		return false, nil
	}
	consume := func() error {
		take(constants.RolloutResumeAnnotation)
		return nil
	}
	if !req.addressesUnit(in.Component, cs) {
		if req.addressesAnyUnit(in.ISVC) {
			return false, nil
		}
		// A verb no canary carries is consumed and ignored rather than left
		// to fire on a later canary that happens to carry the same hash, a
		// park it was never meant for.
		recordResume(in.ISVC, in.Component, resumeIgnored)
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonRolloutResumeRejected,
			"resume rejected: %q addresses no canary, parked or in flight; the request is removed", value)
		return false, consume()
	}

	// The park is read from the status marker and the phase together, so a
	// Failed hold whose phase was dropped by an external write still resumes.
	phase := in.ISVC.Status.Components[in.Component].RolloutPhase
	state := rollout.StateOf(cs, phase, 0)
	if cs == nil || !state.Terminal() {
		// Nothing parked to clear. Consume rather than leave an addressed verb
		// on the object waiting to fire at the next terminal hold.
		return false, consume()
	}
	// Re-staging capacity and re-shifting traffic needs a pinned plan to do it
	// against. A completed rollback CLOSES its run, so a RolledBack hold has
	// none — and needs none: clearing the rejection is the whole job there,
	// because the IR's rollback target then drops, the Component's target
	// diverges from stable again, and the run layer opens a fresh run over the
	// same revision. A Failed hold keeps its run open by construction, so a
	// Failed hold WITHOUT one is the one combination where neither route
	// exists, and walking the live spec ladder unpinned is not a substitute.
	//
	// What makes the RolledBack handoff work is that the run layer's sticky
	// reject — the carve-out that stops a rejected target from re-opening a
	// run forever — is keyed on the live cs.RolledBackRevisionHash this verb
	// clears, NOT on the durable rolled-back run record, which stays behind.
	// A reject lookup that consulted the record first would leave the hold
	// asserted and strand the resume here, succeeding with no run to show for
	// it. Locked by the run layer's cleared-reject reopen test.
	if !in.RunActive && state != rollout.CanaryStateRolledBack {
		recordResume(in.ISVC, in.Component, resumeRejected)
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonRolloutResumeRejected,
			"resume rejected for %s: no rollout run is pinned, so there is no plan to re-enter", in.Component)
		return false, consume()
	}
	if req.hash != cs.CanaryRevisionHash {
		recordResume(in.ISVC, in.Component, resumeRejected)
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonRolloutResumeRejected,
			"resume rejected for %s: %q is not the parked canary revision %q", in.Component, req.hash, cs.CanaryRevisionHash)
		return false, consume()
	}

	// A live rollback verb would re-arm the hold on the very next pass, so the
	// resume takes it with the phase it is clearing. This one is removed
	// durably before the flush: the resume decision does not depend on it,
	// and a copy that outlived the flush would roll the resumed canary back.
	if err := consumeAnnotation(ctx, in.Client, in.ISVC, constants.RolloutRollbackAnnotation); err != nil {
		return false, err
	}
	// Re-arm toward the PARKED revision, not the observed target: a completed
	// rollback has already pointed the IR back at stable, so the observation
	// names the wrong revision until the rollback signal clears.
	resetCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, cs.CanaryRevisionHash, in.Now)
	recordResume(in.ISVC, in.Component, resumeApplied)
	emit(in.Recorder, in.ISVC, corev1.EventTypeNormal, EventReasonRolloutResumed,
		"resumed %s from %s: re-entering the canary ladder at step 0 on revision %s; every gate re-runs",
		in.Component, phase, cs.CanaryRevisionHash)
	return true, consume()
}

// emit is best-effort observability; a nil recorder (tests) is fine.
func emit(recorder record.EventRecorder, isvc *v1beta1.InferenceService, eventType, reason, messageFmt string, args ...interface{}) {
	if recorder == nil {
		return
	}
	recorder.Eventf(isvc, eventType, reason, messageFmt, args...)
}
