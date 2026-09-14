package canary

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

// isRollbackRequested reports whether the operator requested rollback via
// ome.io/rollout-rollback. Only the value "true" is a request: the annotation
// is a documented boolean whose "false" is identical to absence, and any other
// value (possible via direct writes that bypass admission) must not trigger a
// production rollback.
func isRollbackRequested(isvc *v1beta1.InferenceService) bool {
	return isvc.Annotations[constants.RolloutRollbackAnnotation] == "true"
}

// shouldAdvanceManual reports whether the operator promoted THIS canary revision
// via ome.io/rollout-promote=<canaryHash>. The hash guard prevents a stale
// promote (left over from a prior rollout) from advancing a new one. A value
// already recorded in cs.PromotedThrough is inert: annotation removal is
// best-effort after the advance persists, so a lingering annotation must not
// re-apply a promotion that already advanced a step (one promote, one step).
func shouldAdvanceManual(isvc *v1beta1.InferenceService, cs *v1beta1.CanaryStatus) bool {
	v := isvc.Annotations[constants.RolloutPromoteAnnotation]
	return v == cs.CanaryRevisionHash && v != cs.PromotedThrough
}

// shouldAdvanceAuto reports whether a paused step's Pause.Duration has elapsed
// since the step was entered. A step with no Pause, or a Pause with no Duration,
// advances immediately.
func shouldAdvanceAuto(cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep, now time.Time) bool {
	if step.Pause == nil || step.Pause.Duration == nil {
		return true
	}
	if cs == nil || cs.StepEnteredTime == nil {
		return false
	}
	return !now.Before(cs.StepEnteredTime.Time.Add(step.Pause.Duration.Duration))
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

// stepRequeue is how soon to re-check a held step: the step's analysis Interval
// (so sampling re-runs on cadence) for an analysis step, else the standard requeue.
func stepRequeue(step v1beta1.RolloutGroupStep) time.Duration {
	if step.Analysis != nil && step.Analysis.Interval.Duration > 0 {
		return step.Analysis.Interval.Duration
	}
	return reconcileRequeue
}

// evaluateStep decides whether a gated step may advance, dispatching on the step's
// gate by field presence: Analysis (metric sampling — the only branch that can
// return decRollback/decFailed), a timed Pause (Duration elapsed), else Manual
// (promote annotation). The metrics source for analysis is GroupCanary.Prometheus,
// supplied to the sampler via ReconcileInputs.Prometheus.
func evaluateStep(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep) stepDecision {
	switch {
	case stepIsAnalysis(step):
		return evaluateAnalysisStep(ctx, in, step.Analysis, cs, step)
	case step.Pause != nil && step.Pause.Duration != nil:
		if shouldAdvanceAuto(cs, step, in.Now) {
			return decAdvance
		}
		return decHold
	default:
		if shouldAdvanceManual(in.ISVC, cs) {
			return decAdvance
		}
		return decHold
	}
}

// evaluateAnalysisStep runs the metric gate for one reconcile: warm up, throttle
// to one evaluation per Interval, read a sample, then decide. Sampling is
// non-blocking — a miss kicks a bounded background query (the slow Prometheus
// call never runs on the reconcile goroutine) and holds; the sampler's completion
// event, or the step requeue, re-reconciles to consume the result. A matching
// promote annotation overrides the gate.
func evaluateAnalysisStep(ctx context.Context, in ReconcileInputs, a *v1beta1.RolloutAnalysis, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep) stepDecision {
	// Operator override: a matching promote annotation force-advances mid-bake.
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
		if shouldAdvanceAuto(cs, step, in.Now) { // bake window (Pause.Duration) elapsed
			dec = decAdvance
		}
	default: // analysis.Inconclusive
		if a.OnInconclusive != nil && *a.OnInconclusive == v1beta1.OnInconclusiveRollback {
			dec = decRollback
		} else if analysisStalled(cs, resolveReadyTimeout(in, effectiveCanaryPlan(in.ISVC)), in.Now) {
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
// the controller's single status flush. The annotation itself is NOT touched
// here: metadata and status cannot be written atomically, and removing the
// annotation before the status flush lands would lose the promote if that
// flush then fails or the process dies. Removal happens on a later pass, after
// the advance is durable (syncPromotedThrough); until then the recorded value
// keeps the lingering annotation inert (see shouldAdvanceManual).
func advanceStep(in ReconcileInputs) {
	cs := in.ISVC.Status.Canary
	if v, ok := in.ISVC.Annotations[constants.RolloutPromoteAnnotation]; ok {
		cs.PromotedThrough = v
	}
	cs.CurrentStep++
	cs.StepEnteredTime = &metav1.Time{Time: in.Now}
	// Reset the per-step analysis budget + sampling state: the failure budget is
	// scoped to each step (each traffic level gets its own tolerance), and the
	// next step samples fresh.
	cs.AnalysisFailedChecks = 0
	cs.LastEvaluationTime = nil
	cs.LastConclusiveEvaluationTime = nil
	cs.MetricResults = nil
}

// syncPromotedThrough converges the durable promote record with the live
// annotation, on passes AFTER the advance it records has persisted. While the
// applied annotation lingers, retry its removal — best-effort: a failure
// leaves it inert (it matches PromotedThrough) for the next pass. Once the
// annotation is observed gone, clear the record so a later promote of the
// same revision is honored again. The clear waits for observed absence
// because a stale cache can re-show the annotation after removal; clearing
// while it is still visible would re-apply the promotion.
func syncPromotedThrough(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus) {
	if cs.PromotedThrough == "" {
		return
	}
	v, ok := in.ISVC.Annotations[constants.RolloutPromoteAnnotation]
	if !ok {
		cs.PromotedThrough = ""
		return
	}
	if v == cs.PromotedThrough {
		_ = consumeAnnotation(ctx, in.Client, in.ISVC, constants.RolloutPromoteAnnotation)
	}
}

// consumeAnnotation removes an operator command annotation, persisting the
// removal to the apiserver (metadata merge patch) before the in-memory delete.
// The controller only writes the status subresource, so an in-memory delete
// alone would leave the annotation live and re-consumed on every later pass —
// one ome.io/rollout-promote would advance every subsequent step. The patch
// targets a copy so the server response cannot clobber in-flight status
// mutations on the working object. A nil client consumes in-memory only.
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
