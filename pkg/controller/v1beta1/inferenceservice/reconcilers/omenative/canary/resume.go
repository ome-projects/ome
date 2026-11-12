package canary

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

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
func (r resumeRequest) addressesAnyUnit(isvc *v1beta1.InferenceService, policies rollout.Policies) bool {
	for _, g := range rollout.CanaryGroups(isvc, policies) {
		primary := rollout.PrimaryOf(g)
		if r.addressesUnit(primary, rollout.CanaryStatusFor(&isvc.Status, primary)) {
			return true
		}
	}
	return false
}

// memberTarget is one member of the canary unit with the revision its
// InferenceReplica reports as its target. A replica told to roll back keeps
// reporting the spec's revision as its target, so under a standing rollback
// hold the target is still the rejected revision; it moves only when the
// spec moves.
type memberTarget struct {
	component v1beta1.ComponentType
	revision  string
}

// memberTargets lists the unit's members with their observed targets, the
// primary first and the secondaries in name order, so a message reads the
// same on every pass.
type memberTargets []memberTarget

func observedMemberTargets(in ReconcileInputs) memberTargets {
	out := memberTargets{{component: in.Component, revision: in.CanaryRevisionHash}}
	secondaries := make([]v1beta1.ComponentType, 0, len(in.Secondaries))
	for c := range in.Secondaries {
		secondaries = append(secondaries, c)
	}
	sort.Slice(secondaries, func(i, j int) bool { return secondaries[i] < secondaries[j] })
	for _, c := range secondaries {
		out = append(out, memberTarget{component: c, revision: in.Secondaries[c].CanaryRevisionHash})
	}
	return out
}

// names reports whether some member targets the revision.
func (m memberTargets) names(hash string) bool {
	for _, t := range m {
		if t.revision != "" && t.revision == hash {
			return true
		}
	}
	return false
}

// String reads "engine targets abc, decoder targets def".
func (m memberTargets) String() string {
	parts := make([]string, 0, len(m))
	for _, t := range m {
		parts = append(parts, string(t.component)+" targets "+t.revision)
	}
	return strings.Join(parts, ", ")
}

// handleResume applies the one-shot ome.io/rollout-resume verb: clear this
// Component's terminal RolloutPhase and re-enter the step machine against
// the revision that is already parked, without minting a revision. A
// unit stopped with no rejection continues at the step the stop
// interrupted, or at a later step when the fleet already runs more of the
// canary revision than that step stages (resumeStep); a unit holding a
// rejection, settled or stopped for want of the stable revision, starts the
// steps over at step 0, raised the same way for Instances still on the
// rejected revision.
//
// It reports whether it resumed. A resume does NOT fall through to the step
// machine on the same pass: the pass ends with the re-armed record, the
// dispatch drops the replica's rollback target once the executor stops
// reporting the revert, and the next pass runs the steps from the record it
// reads back. The Failed path takes the same one-requeue detour so the verb
// has a single behaviour.
//
// The verb itself is handed back through take and removed by the controller
// after the status flush that carries the re-arm: a lost flush then leaves
// the verb in place to be applied again, and a verb still visible after a
// successful flush finds nothing parked and is consumed with no effect.
func handleResume(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, take func(string)) (bool, error) {
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
		if req.addressesAnyUnit(in.ISVC, in.Policies) {
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

	// The stop is read from the recorded failure and the phase together, as
	// the holds read it: a Failed hold whose phase was dropped by an external
	// write still resumes, and a finished unit whose phase was written Failed
	// from outside has nothing to resume.
	phase := in.ISVC.Status.Components[in.Component].RolloutPhase
	state := rollout.StateOf(cs, phase, len(plan.Steps))
	if cs == nil || !state.Terminal() {
		// Nothing parked to clear. Consume rather than leave an addressed verb
		// on the object waiting to fire at the next terminal hold.
		return false, consume()
	}
	if why := resumeRefusal(in, cs, req); why != "" {
		recordResume(in.ISVC, in.Component, resumeRejected)
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonRolloutResumeRejected, "resume rejected for %s: %s", in.Component, why)
		return false, consume()
	}

	// A live rollback verb would re-arm the hold on the very next pass, so the
	// resume takes it with the phase it is clearing. This one is removed
	// durably before the flush: the resume decision does not depend on it,
	// and a copy that outlived the flush would roll the resumed canary back.
	if err := consumeAnnotation(ctx, in.Client, in.ISVC, constants.RolloutRollbackAnnotation); err != nil {
		return false, err
	}
	applyResume(in, cs, plan, phase)
	return true, consume()
}

// resumeRefusal is why a resume addressed to a stopped unit cannot be taken,
// empty when it can: no run to re-enter, a value naming another revision, or
// a revision no member targets any more.
func resumeRefusal(in ReconcileInputs, cs *v1beta1.CanaryStatus, req resumeRequest) string {
	// Re-staging capacity and re-shifting traffic needs a pinned plan to do it
	// against. A stop that holds a rejection needs no open run to resume:
	// clearing the rejection is the whole job there, because the replica's
	// rollback target then drops, the Component's target diverges from
	// stable again, and on its next pass the run layer takes over the record
	// this verb writes (an adoption toward the same revision, not a fresh
	// run). That is the settled RolledBack hold, whose completed revert
	// closed its run, and the stop a rollback reaches when the stable
	// revision it would return to is not retained, which can be written
	// after the run closed. A stop with no rejection keeps its run open by
	// construction, so one WITHOUT a run is the one combination where
	// neither route exists, and walking the live spec's steps without a
	// pinned plan is not a substitute.
	//
	// What makes the handoff work is that the run layer's sticky reject,
	// the carve-out that stops a rejected target from re-opening a run
	// forever, is keyed on the live cs.RolledBackRevisionHash this verb
	// clears, NOT on the durable rolled-back run record, which stays behind.
	// A reject lookup that consulted the record first would leave the hold
	// asserted and strand the resume here, succeeding with no run to show for
	// it. Locked by the run layer's cleared-reject reopen test.
	if !in.RunActive && cs.RolledBackRevisionHash == "" {
		return "no rollout run is pinned, so there is no plan to re-enter"
	}
	if req.hash != cs.CanaryRevisionHash {
		return fmt.Sprintf("%q is not the parked canary revision %q", req.hash, cs.CanaryRevisionHash)
	}
	// A resume re-enters the steps toward the stopped canary's revision, so a member of
	// the unit must still target it. Once the spec went back to the stable
	// revision nothing is left to roll: a re-entry would record the stable
	// revision as its own canary and wait for a promote that moves nothing.
	if targets := observedMemberTargets(in); !targets.names(req.hash) {
		return fmt.Sprintf("no member of the unit targets revision %q any more (%s); the request is removed", req.hash, targets)
	}
	return ""
}

// applyResume starts the record again toward the revision it names and
// reports it. A stop with no rejection interrupted a step whose split the
// fleet already serves, so the steps continue from there with that split
// kept. A stop that holds a rejection is a rollback's end, whether the
// revert settled or could not run for want of the stable revision: the
// steps start over at step 0, raised only to cover the Instances still on
// the rejected revision, with no split to keep.
func applyResume(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, phase v1beta1.RolloutPhase) {
	step, weight, resumedAt := resumeStep(in, plan.Steps, 0), int32(0), "starts its steps again at"
	if cs.RolledBackRevisionHash == "" {
		step, weight, resumedAt = resumeStep(in, plan.Steps, cs.CurrentStep), cs.ObservedTrafficWeight, "continues at"
	}
	resumeCanaryStatus(in.ISVC, in.Component, cs, in.TargetID, step, weight, in.Now)
	recordResume(in.ISVC, in.Component, resumeApplied)
	emit(in.Recorder, in.ISVC, corev1.EventTypeNormal, EventReasonRolloutResumed,
		"resumed %s from %s: the canary %s step %d of %d (%s); the step's capacity wait, gate and ready timeout run again from now",
		in.Component, phase, resumedAt, step, len(plan.Steps), heldMembers(in))
}

// resumeStep is the step a stopped unit continues at: the step the stop
// interrupted, raised to the first later step whose staged count covers the
// Instances each member already runs on the canary revision, so a resumed
// step never stages less than the fleet serves. The final step covers
// everything a stop can leave behind.
func resumeStep(in ReconcileInputs, steps []v1beta1.RolloutGroupStep, stopped int32) int32 {
	last := int32(len(steps)) - 1
	step := stopped
	if step > last {
		step = last
	}
	raise := func(desired, onCanary int32) {
		for step < last && stepStagedCount(steps[step], desired) < onCanary {
			step++
		}
	}
	raise(in.DesiredReplicas, readyCanaryCapacity(in))
	for _, m := range in.Secondaries {
		if m.retargeted() {
			raise(m.DesiredReplicas, m.ReadyCanaryCapacity)
		}
	}
	return step
}

// resumeCanaryStatus starts a stopped or rolled-back record again at step
// toward the revision it already names, keeping the identity pair and the
// weight the split carries. The step's clock starts now and the capacity wait
// is clocked again by the first short pass; every other field belonged to the
// hold and goes with it.
func resumeCanaryStatus(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, cs *v1beta1.CanaryStatus, targetID string, step, weight int32, now time.Time) {
	resetCanaryStatus(isvc, c, cs, targetID, cs.CanaryRevisionHash, now)
	cs.CurrentStep, cs.ObservedTrafficWeight = step, weight
}

// emit is best-effort observability; a nil recorder (tests) is fine.
func emit(recorder record.EventRecorder, isvc *v1beta1.InferenceService, eventType, reason, messageFmt string, args ...interface{}) {
	if recorder == nil {
		return
	}
	recorder.Eventf(isvc, eventType, reason, messageFmt, args...)
}
