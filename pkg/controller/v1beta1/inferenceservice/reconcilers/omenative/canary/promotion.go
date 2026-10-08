package canary

import (
	"context"
	"fmt"
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
// to one evaluation per Interval, read a sample, then decide. Sampling is
// non-blocking — a miss kicks a bounded background query (the slow Prometheus
// call never runs on the reconcile goroutine) and holds; the sampler's completion
// event, or the step requeue, re-reconciles to consume the result. A matching
// promote overrides the gate; evaluateStep decides it first, and the gate
// repeats the check so it stays whole for a direct caller.
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
	req, err := buildSampleRequest(ctx, in, a, cs.CurrentStep)
	if err != nil {
		// Source wiring failure (e.g. an unreadable auth secret) before any query:
		// treat as inconclusive — hold, or roll back per OnInconclusive — never read
		// it as a metric breach.
		return consumeSample(in, cs, step, a, inconclusiveResult("auth", err.Error()), in.Now)
	}
	// `since` is the last consumed sample's produced time, so Get returns only a
	// genuinely newer sample (and never re-counts one already consumed).
	var since time.Time
	if cs.LastEvaluationTime != nil {
		since = cs.LastEvaluationTime.Time
	}
	res, producedAt, ok := in.Sampler.Get(req, since)
	if !ok {
		// No fresh sample yet — a background query was kicked (or is already in
		// flight). Hold; a completion event or the step requeue re-reconciles.
		return decHold
	}
	return consumeSample(in, cs, step, a, res, producedAt)
}

// consumeSample records one analysis sample into status and decides the step's
// fate: a breach accrues toward FailureLimit (cumulative per step, rollback at the
// limit); a pass advances once the bake window (Pause.Duration) elapses; an
// inconclusive sample holds, rolls back per OnInconclusive, or escalates to Failed
// past the stall timeout. `at` is the sample's produced time (or now for a wiring
// failure) and stamps the evaluation timestamps.
func consumeSample(in ReconcileInputs, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep, a *v1beta1.RolloutAnalysis, res analysis.Result, at time.Time) stepDecision {
	cs.LastEvaluationTime = &metav1.Time{Time: at}
	cs.MetricResults = toStatusMetricResults(res.Metrics, at)
	dec := decHold
	switch res.Outcome {
	case analysis.Fail:
		cs.LastConclusiveEvaluationTime = &metav1.Time{Time: at}
		cs.AnalysisFailedChecks++
		if cs.AnalysisFailedChecks >= a.FailureLimit {
			dec = decRollback
		}
	case analysis.Pass:
		cs.LastConclusiveEvaluationTime = &metav1.Time{Time: at}
		if shouldAdvanceAuto(cs, step, in.CanaryRestartedAt, in.Now) { // bake window (Pause.Duration) elapsed
			dec = decAdvance
		}
	default: // analysis.Inconclusive
		if a.OnInconclusive != nil && *a.OnInconclusive == v1beta1.OnInconclusiveRollback {
			dec = decRollback
		} else if analysisStalled(cs, resolveReadyTimeout(in, effectiveCanaryPlan(in)), in.Now) {
			// A stall is terminal either way; RollbackOnStall reverts instead of
			// parking, so an unreadable gate cannot leave the fleet split.
			dec = decFailed
			if a.OnInconclusive != nil && *a.OnInconclusive == v1beta1.OnInconclusiveRollbackOnStall {
				dec = decRollback
			}
		}
	}
	recordAnalysisSample(in.ISVC, in.Component, res, cs.AnalysisFailedChecks)
	if dec == decRollback {
		recordRollback(in.ISVC, in.Component, "analysis")
	}
	return dec
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
