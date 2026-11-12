package canary

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// isRollbackRequested reports whether the operator requested rollback via
// ome.io/rollout-rollback. Only the value "true" is a request: the annotation
// is a documented boolean whose "false" is identical to absence, and any other
// value (possible via direct writes that bypass admission) must not trigger a
// production rollback.
func isRollbackRequested(isvc *v1beta1.InferenceService) bool {
	return isvc.Annotations[constants.RolloutRollbackAnnotation] == "true"
}

// rollbackRequestHeld reports whether a unit applied the visible rollback
// request and still holds the rollback it recorded. The request is that
// unit's: it stays on the object through the hold, and no other unit acts on
// it. The service-level record counts only while no unit has one of its own.
func rollbackRequestHeld(isvc *v1beta1.InferenceService) bool {
	holds := func(cs *v1beta1.CanaryStatus) bool {
		return cs != nil && cs.RollbackRequested && cs.RolledBackRevisionHash != ""
	}
	perUnit := false
	for _, c := range isvc.Status.Components {
		if c.Canary != nil {
			perUnit = true
			if holds(c.Canary) {
				return true
			}
		}
	}
	return !perUnit && holds(isvc.Status.Canary)
}

// rollbackRequestPending reports a rollback request no unit has applied.
func rollbackRequestPending(isvc *v1beta1.InferenceService) bool {
	return isRollbackRequested(isvc) && !rollbackRequestHeld(isvc)
}

// rollbackRequestActionable reports whether a unit of the service other
// than self carries a canary a rollback request can act on: started and not
// done, serving, held or resting Failed. A unit that is already rolling back or
// rolled back has nothing to reject. The dispatching unit answers for itself.
func rollbackRequestActionable(isvc *v1beta1.InferenceService, policies rollout.Policies, self v1beta1.ComponentType) bool {
	for _, g := range rollout.CanaryGroups(isvc, policies) {
		primary := rollout.PrimaryOf(g)
		if primary == self || g.Canary == nil {
			continue
		}
		cs := rollout.CanaryStatusFor(&isvc.Status, primary)
		if cs == nil {
			continue
		}
		switch rollout.StateOf(cs, isvc.Status.Components[primary].RolloutPhase, len(g.Canary.Steps)) {
		case rollout.CanaryStateDone, rollout.CanaryStateRollingBack, rollout.CanaryStateRolledBack:
		default:
			return true
		}
	}
	return false
}

// rollbackRequestSpendable reports whether the unit self, carrying the record
// cs, may remove the visible rollback request: the request is its own (it
// applied it and still holds the rollback), or no unit holds it and no other
// unit carries a canary it can act on. A request another unit holds, or one
// another unit's canary can still apply, is that unit's and is left for it.
func rollbackRequestSpendable(isvc *v1beta1.InferenceService, policies rollout.Policies, self v1beta1.ComponentType, cs *v1beta1.CanaryStatus) bool {
	if !isRollbackRequested(isvc) {
		return false
	}
	if cs != nil && cs.RollbackRequested && cs.RolledBackRevisionHash != "" {
		return true
	}
	return !rollbackRequestHeld(isvc) && !rollbackRequestActionable(isvc, policies, self)
}

// promoteKeys are the operator verbs that open a step's gate: the promote
// and its forced form. Both carry the canary revision hash and are recorded
// in PromotedThrough when applied.
var promoteKeys = []string{constants.RolloutPromoteAnnotation, constants.RolloutPromoteForceAnnotation}

// promoteMatches reports whether key names THIS canary revision. The hash
// guard prevents a stale value (left over from a prior rollout) from
// advancing a new one. A value already recorded in cs.PromotedThrough is
// inert: annotation removal is best-effort after the advance persists, so a
// lingering annotation must not re-apply a promotion that already advanced
// a step (one verb, one step).
func promoteMatches(isvc *v1beta1.InferenceService, cs *v1beta1.CanaryStatus, key string) bool {
	v, ok := isvc.Annotations[key]
	return ok && v == cs.CanaryRevisionHash && v != cs.PromotedThrough
}

// shouldAdvanceManual reports whether the operator promoted THIS canary
// revision, via ome.io/rollout-promote=<canaryHash> or its forced form.
func shouldAdvanceManual(isvc *v1beta1.InferenceService, cs *v1beta1.CanaryStatus) bool {
	return promoteMatches(isvc, cs, constants.RolloutPromoteAnnotation) ||
		promoteMatches(isvc, cs, constants.RolloutPromoteForceAnnotation)
}

// forceRequested reports whether the operator forced THIS canary revision
// past its step's capacity wait via ome.io/rollout-promote-force=<canaryHash>,
// under the promote's guards.
func forceRequested(isvc *v1beta1.InferenceService, cs *v1beta1.CanaryStatus) bool {
	return promoteMatches(isvc, cs, constants.RolloutPromoteForceAnnotation)
}

// recordPromotes records the pending promote value in PromotedThrough and
// hands every present key back for removal after the status flush. The value
// naming the live canary revision is the one recorded whenever a key carries
// it: a stale value beside it must not leave the applied verb unrecorded.
func recordPromotes(in ReconcileInputs, cs *v1beta1.CanaryStatus, take func(string)) {
	live := false
	for _, key := range promoteKeys {
		v, ok := in.ISVC.Annotations[key]
		if !ok {
			continue
		}
		if v == cs.CanaryRevisionHash {
			cs.PromotedThrough, live = v, true
		} else if !live {
			cs.PromotedThrough = v
		}
		take(key)
	}
}

// announceForce records a forced advance on the InferenceService: the step,
// the canary revision and the Ready canary capacity the operator advanced
// over.
func announceForce(in ReconcileInputs, cs *v1beta1.CanaryStatus, ready, staged int32) {
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryStepForced,
		"%s canary step %d forced: advancing on revision %s with %d of %d canary instances Ready; the step's capacity converges in the background and traffic follows the pods that serve",
		in.Component, cs.CurrentStep, cs.CanaryRevisionHash, ready, staged)
}

// shouldAdvanceAuto reports whether a timed step's Pause.Duration has
// elapsed since its soak began: the later of the split first serving and
// restartedAt, the newest moment a canary pod of the unit died or came back.
// A canary pod the kubelet or the restart policy brought back has served
// only since then, so the step moves once that pod has served a full soak;
// a restart dated before the step is not this step's. A step with no Pause,
// or a Pause with no Duration, advances immediately.
func shouldAdvanceAuto(cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep, restartedAt, now time.Time) bool {
	if step.Pause == nil || step.Pause.Duration == nil {
		return true
	}
	if cs == nil || cs.StepEnteredTime == nil {
		return false
	}
	return !now.Before(laterOf(cs.StepEnteredTime.Time, restartedAt).Add(step.Pause.Duration.Duration))
}

// stepDecision is the verdict for whether a gated step may move. Manual and Auto
// produce only decHold/decAdvance; Analysis — the only policy that can retreat
// without an operator — can also produce decRollback or decFailed.
type stepDecision int

const (
	decHold     stepDecision = iota // stay on this step (keep baking / paused)
	decAdvance                      // advance to the next step (or, on the final step, complete)
	decRollback                     // abort: roll back to the stable revision
	decFailed                       // analysis could not read health past the stall timeout
)

// stepIsAnalysis reports whether a step opts into metric-gated promotion (its own
// Analysis field is set). The gate is per-step; GroupCanary.Analysis only holds
// shared defaults and gates nothing by itself.
func stepIsAnalysis(step v1beta1.RolloutGroupStep) bool {
	return step.Analysis != nil
}

// stepGated reports whether a step holds before advancing: it is gated when it
// opts into analysis or carries a Pause. A bare step (neither) advances as soon
// as capacity + traffic converge.
func stepGated(step v1beta1.RolloutGroupStep) bool {
	return stepIsAnalysis(step) || step.Pause != nil
}

// stepWake is how soon to re-check a held step: the step's analysis Interval
// (so sampling re-runs on cadence) for an analysis step, else the configured
// step cadence.
func stepWake(in ReconcileInputs, step v1beta1.RolloutGroupStep) time.Duration {
	if step.Analysis != nil && step.Analysis.Interval.Duration > 0 {
		return step.Analysis.Interval.Duration
	}
	return in.Requeue
}

// evaluateStep decides whether a gated step may advance. A matching promote
// is the operator's decision on any gate: it opens a manual hold, skips a
// timed soak and overrides analysis alike. Otherwise the step's gate decides
// by field presence: Analysis (metric sampling — the only branch that can
// return decRollback/decFailed), a timed Pause (Duration elapsed), else a
// manual hold. The metrics source for analysis is GroupCanary.Prometheus,
// supplied to the sampler via ReconcileInputs.Prometheus.
func evaluateStep(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep) stepDecision {
	if shouldAdvanceManual(in.ISVC, cs) {
		return decAdvance
	}
	switch {
	case stepIsAnalysis(step):
		return evaluateAnalysisStep(ctx, in, step.Analysis, cs, step)
	case step.Pause != nil && step.Pause.Duration != nil:
		if shouldAdvanceAuto(cs, step, in.CanaryRestartedAt, in.Now) {
			return decAdvance
		}
		return decHold
	default:
		return decHold
	}
}

// evaluateAnalysisStep runs the metric gate for one reconcile: warm up, throttle
// to one evaluation per Interval, read one sample of every member the push
// moved, then decide. Sampling is non-blocking — a miss kicks a bounded
// background query (the slow Prometheus call never runs on the reconcile
// goroutine) and holds; the sampler's completion event, or the step requeue,
// re-reconciles to consume the result. The unit's sample is complete only
// when every sampled member has a fresh result: a gate with no sample, or a
// member still unread, never promotes. A matching promote overrides the gate;
// evaluateStep decides it first, and the gate repeats the check so it stays
// whole for a direct caller.
func evaluateAnalysisStep(ctx context.Context, in ReconcileInputs, a *v1beta1.RolloutAnalysis, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep) stepDecision {
	if shouldAdvanceManual(in.ISVC, cs) {
		return decAdvance
	}
	// Warm-up: no sampling until InitialDelay after the split first served
	// (StepEnteredTime is re-stamped when the step starts serving).
	if a.InitialDelay != nil && cs.StepEnteredTime != nil &&
		in.Now.Before(cs.StepEnteredTime.Time.Add(a.InitialDelay.Duration)) {
		return decHold
	}
	// Throttle: at most one evaluation per Interval. This both bounds failure
	// accrual (one count per interval, so a burst of reconciles can't spuriously
	// roll back) and paces the background query — an unrelated reconcile mid-interval
	// won't kick a fresh one.
	if cs.LastEvaluationTime != nil && in.Now.Before(cs.LastEvaluationTime.Time.Add(a.Interval.Duration)) {
		return decHold
	}
	if in.Sampler == nil {
		// Misconfigured (no sampler wired): hold rather than advance ungated.
		return decHold
	}
	// With no member on record as moved there is nothing whose canary pods a
	// query could read, so there is no sample to decide on.
	members := sampledMembers(in)
	if len(members) == 0 {
		return decHold
	}
	src, err := resolveSampleSource(ctx, in)
	if err != nil {
		// Source wiring failure (e.g. an unreadable auth secret) before any query:
		// treat as inconclusive — hold, or roll back per OnInconclusive — never read
		// it as a metric breach.
		return consumeSample(in, cs, step, a, []memberSample{{component: in.Component, result: inconclusiveResult("auth", err.Error())}}, in.Now)
	}
	// `since` is the last consumed sample's produced time, so Get returns only a
	// genuinely newer sample (and never re-counts one already consumed).
	var since time.Time
	if cs.LastEvaluationTime != nil {
		since = cs.LastEvaluationTime.Time
	}
	samples := make([]memberSample, 0, len(members))
	var at time.Time
	for _, m := range members {
		res, producedAt, ok := in.Sampler.Get(buildSampleRequest(in, src, m, a, cs.CurrentStep), since)
		if !ok {
			// No fresh sample for this member yet — a background query was
			// kicked (or is already in flight). The other members are still
			// read on this pass, so their queries run side by side.
			continue
		}
		samples = append(samples, memberSample{component: m.component, result: res})
		at = laterOf(at, producedAt)
	}
	if len(samples) < len(members) {
		// Hold until every member has answered; a completion event or the
		// step requeue re-reconciles.
		return decHold
	}
	// The unit's sample is dated by its newest member sample, so no member's
	// result reads as newer than the evaluation that consumed it.
	return consumeSample(in, cs, step, a, samples, at)
}

// memberSample is one member's result within the unit's sample.
type memberSample struct {
	component v1beta1.ComponentType
	result    analysis.Result
}

// unitResult folds the sampled members' results into the unit's verdict with
// the precedence one sample's metrics have: a breach on any member is the
// unit's breach, else a member that could not be read leaves the unit
// unreadable, else the unit passes. The primary's metrics keep their names;
// a secondary's are keyed "<component>/<name>", so two members' results for
// one metric stay distinct under the status list's name key and each entry
// says which member it read.
func unitResult(primary v1beta1.ComponentType, samples []memberSample) analysis.Result {
	out := analysis.Result{Outcome: analysis.Pass}
	for _, s := range samples {
		switch s.result.Outcome {
		case analysis.Fail:
			out.Outcome = analysis.Fail
		case analysis.Inconclusive:
			if out.Outcome == analysis.Pass {
				out.Outcome = analysis.Inconclusive
			}
		}
		for _, m := range s.result.Metrics {
			if s.component != primary {
				m.Name = string(s.component) + "/" + m.Name
			}
			out.Metrics = append(out.Metrics, m)
		}
	}
	return out
}

// consumeSample records one analysis sample of the unit, every sampled
// member's result folded into one verdict, into status and decides the
// step's fate: a breach accrues toward FailureLimit (cumulative per step,
// rollback at the limit); a pass advances once the bake window
// (Pause.Duration) elapses; an inconclusive sample holds, rolls back per
// OnInconclusive, or escalates to Failed past the stall timeout. `at` is the
// sample's produced time (or now for a wiring failure) and stamps the
// evaluation timestamps. No sample is no verdict: nothing is stamped and the
// step holds.
func consumeSample(in ReconcileInputs, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep, a *v1beta1.RolloutAnalysis, samples []memberSample, at time.Time) stepDecision {
	if len(samples) == 0 {
		return decHold
	}
	res := unitResult(in.Component, samples)
	cs.LastEvaluationTime = &metav1.Time{Time: at}
	cs.MetricResults = toStatusMetricResults(res.Metrics, at)
	dec := decHold
	// cause says, for a rollback, why the gate decided it; the deciding pass
	// records it on the object so the reason is not read from status alone.
	var cause string
	switch res.Outcome {
	case analysis.Fail:
		cs.LastConclusiveEvaluationTime = &metav1.Time{Time: at}
		cs.AnalysisFailedChecks++
		if cs.AnalysisFailedChecks >= a.FailureLimit {
			dec = decRollback
			cause = fmt.Sprintf("failing samples reached the limit (%d of %d)", cs.AnalysisFailedChecks, a.FailureLimit)
		}
	case analysis.Pass:
		cs.LastConclusiveEvaluationTime = &metav1.Time{Time: at}
		if shouldAdvanceAuto(cs, step, in.CanaryRestartedAt, in.Now) { // bake window (Pause.Duration) elapsed
			dec = decAdvance
		}
	default: // analysis.Inconclusive
		timeout := resolveReadyTimeout(in, effectiveCanaryPlan(in))
		if a.OnInconclusive != nil && *a.OnInconclusive == v1beta1.OnInconclusiveRollback {
			dec = decRollback
			cause = fmt.Sprintf("the sample could not be read and onInconclusive is %s", *a.OnInconclusive)
		} else if analysisStalled(cs, timeout, in.Now) {
			// A stall is terminal either way; RollbackOnStall reverts instead of
			// parking, so an unreadable gate cannot leave the fleet split.
			dec = decFailed
			if a.OnInconclusive != nil && *a.OnInconclusive == v1beta1.OnInconclusiveRollbackOnStall {
				dec = decRollback
				cause = fmt.Sprintf("no sample could be read for %s and onInconclusive is %s", timeout, *a.OnInconclusive)
			}
		}
	}
	// Each member's own result is published under its Component, so the
	// metrics name the member a value or a breach belongs to.
	for _, s := range samples {
		recordAnalysisSample(in.ISVC, s.component, s.result, cs.AnalysisFailedChecks)
	}
	if dec == decRollback {
		recordRollback(in.ISVC, in.Component, "analysis")
		emitAnalysisRollbackEvent(in, cs, res.Outcome, cause, samples)
	}
	return dec
}

// emitAnalysisRollbackEvent records the Warning for a rollback the metric
// gate decided: the step, the cause, and the checks of the deciding sample
// that failed or could not be read, each with the member it was read on,
// its value and its threshold. It is written on the deciding pass, whether
// the unit then reverts or stops because its stable revision runs nothing.
func emitAnalysisRollbackEvent(in ReconcileInputs, cs *v1beta1.CanaryStatus, outcome analysis.Outcome, cause string, samples []memberSample) {
	label := "failing checks"
	if outcome == analysis.Inconclusive {
		label = "unread checks"
	}
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryAnalysisRolledBack,
		"%s canary step %d: the metric gate decides a rollback, %s; %s: %s",
		in.Component, cs.CurrentStep, cause, label, describeChecks(samples))
}

// describeChecks lists every check of the sample that did not pass as
// "<member> <name>=<value> (threshold <operator> <limit>)". A check that
// could not be read carries its message in place of the threshold, and a
// check with no value recorded is named without one.
func describeChecks(samples []memberSample) string {
	parts := []string{}
	for _, s := range samples {
		for _, m := range s.result.Metrics {
			if m.Passed {
				continue
			}
			part := fmt.Sprintf("%s %s", s.component, m.Name)
			if m.Value != "" {
				part += "=" + m.Value
			}
			switch {
			case m.Message != "":
				part += " (" + m.Message + ")"
			case m.Threshold != "":
				part += fmt.Sprintf(" (threshold %s %s)", m.Operator, m.Threshold)
			}
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return "none recorded"
	}
	return strings.Join(parts, ", ")
}

// analysisStalled reports whether analysis has been unable to read health for
// longer than timeout, measured from the last conclusive sample (or step entry
// if none yet). A stall means "can't tell," not "bad": the caller parks Failed,
// it does not roll back, unless OnInconclusive is Rollback (handled earlier) or
// RollbackOnStall (handled at the stall edge).
func analysisStalled(cs *v1beta1.CanaryStatus, timeout time.Duration, now time.Time) bool {
	if timeout <= 0 {
		return false
	}
	anchor := cs.LastConclusiveEvaluationTime
	if anchor == nil {
		anchor = cs.StepEnteredTime
	}
	if anchor == nil {
		return false
	}
	return !now.Before(anchor.Time.Add(timeout))
}

// toStatusMetricResults converts evaluator results into the status shape,
// stamping each with the sample time.
func toStatusMetricResults(mrs []analysis.MetricResult, now time.Time) []v1beta1.AnalysisMetricResult {
	if len(mrs) == 0 {
		return nil
	}
	t := &metav1.Time{Time: now}
	out := make([]v1beta1.AnalysisMetricResult, 0, len(mrs))
	for _, m := range mrs {
		out = append(out, v1beta1.AnalysisMetricResult{
			Name:      m.Name,
			Value:     m.Value,
			Threshold: m.Threshold,
			Operator:  m.Operator,
			Passed:    m.Passed,
			Message:   m.Message,
			Time:      t,
		})
	}
	return out
}

// advanceStep moves to the next step: increment the index, stamp the entry time
// (Auto timing measures from here), and record any pending promote value in
// PromotedThrough — all one in-memory status mutation, persisted together by
// the controller's single status flush. The annotation is handed back for
// removal after that flush: metadata and status cannot be written atomically,
// and removing the annotation before the status flush lands would lose the
// promote if that flush then fails or the process dies. Until it is observed
// gone, the recorded value keeps the lingering annotation inert (see
// shouldAdvanceManual).
func advanceStep(in ReconcileInputs, take func(string)) {
	cs := rollout.CanaryStatusFor(&in.ISVC.Status, in.Component)
	recordPromotes(in, cs, take)
	cs.CurrentStep++
	cs.StepEnteredTime = &metav1.Time{Time: in.Now}
	cs.CapacityWaitSince = nil
	// The next step is staging until its own gate is met. Projecting Pending
	// here is what tells a later capacity loss apart from the step's initial
	// wait: a split that served and then dipped keeps its phase, a step that
	// has not served yet reads Pending.
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePending)
	// Reset the per-step analysis budget + sampling state: the failure budget is
	// scoped to each step (each traffic level gets its own tolerance), and the
	// next step samples fresh.
	cs.AnalysisFailedChecks = 0
	cs.LastEvaluationTime = nil
	cs.LastConclusiveEvaluationTime = nil
	cs.MetricResults = nil
}

// syncPromotedThrough converges the durable promote record with the live
// annotations, on passes AFTER the advance it records has persisted. While a
// key still carries the applied value, hand it back for removal again; until
// it is gone it stays inert (it matches PromotedThrough). Once no key carries
// the value, clear the record so a later promote of the same revision is
// honored again. The clear waits for observed absence because a stale cache
// can re-show the annotation after removal; clearing while it is still
// visible would re-apply the promotion. A key carrying another value is not
// the applied verb: it neither keeps the record nor is taken here.
func syncPromotedThrough(in ReconcileInputs, cs *v1beta1.CanaryStatus, take func(string)) {
	if cs.PromotedThrough == "" {
		return
	}
	lingering := false
	for _, key := range promoteKeys {
		if v, ok := in.ISVC.Annotations[key]; ok && v == cs.PromotedThrough {
			lingering = true
			take(key)
		}
	}
	if !lingering {
		cs.PromotedThrough = ""
	}
}

// consumeAnnotation removes an operator command annotation durably, before
// the status flush. It is the right tool only where the decision it serves
// does not depend on the annotation and is recomputed by the next pass if
// the flush is lost (the re-arm of a rolled-back unit, in the executor and
// at run open); every other verb is recorded in
// status and handed to the controller for removal after the flush. The
// patch targets a copy so the server response cannot clobber in-flight
// status mutations on the working object. A nil client consumes in-memory
// only.
func consumeAnnotation(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, key string) error {
	if _, ok := isvc.Annotations[key]; !ok {
		return nil
	}
	if c != nil {
		patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, key))
		if err := c.Patch(ctx, isvc.DeepCopy(), client.RawPatch(ktypes.MergePatchType, patch)); err != nil {
			return fmt.Errorf("consume annotation %s: %w", key, err)
		}
	}
	delete(isvc.Annotations, key)
	return nil
}
