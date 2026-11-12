package canary

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A rollback the metric gate decides is said on the object: one Warning on
// the deciding pass names the step, why the gate decided, and every failing
// or unread check of the deciding sample with the member it was read on,
// its value and its threshold. A breach below the limit writes nothing: the
// step holds and status alone carries the count.

// valuedAnswer is one member's scripted read: the outcome, the value the
// step's check returned and the message of a check that could not be read.
type valuedAnswer struct {
	outcome analysis.Outcome
	value   string
	message string
}

// valuedScript answers each member's read from a script keyed by the
// request's Component; a member the script does not name has no sample.
type valuedScript struct {
	answers map[string]valuedAnswer
	at      time.Time
}

func (s *valuedScript) Get(req SampleRequest, _ time.Time) (analysis.Result, time.Time, bool) {
	a, ok := s.answers[req.Key.Component]
	if !ok {
		return analysis.Result{}, time.Time{}, false
	}
	res := analysis.Result{Outcome: a.outcome}
	for _, m := range req.Analysis.Metrics {
		res.Metrics = append(res.Metrics, analysis.MetricResult{
			Name: m.Name, Value: a.value, Threshold: m.Threshold, Operator: m.Operator,
			Passed: a.outcome == analysis.Pass, Message: a.message,
		})
	}
	return res, s.at, true
}

// gateRollbackEvents drains the recorder and keeps the gate's rollback
// events, in order.
func gateRollbackEvents(rec *record.FakeRecorder) []string {
	var out []string
	for _, e := range eventsFrom(rec) {
		if strings.Contains(e, EventReasonCanaryAnalysisRolledBack) {
			out = append(out, e)
		}
	}
	return out
}

// wantOneWarningNaming checks that exactly one Warning was recorded and
// that its message carries every given part.
func wantOneWarningNaming(t *testing.T, events []string, parts ...string) {
	t.Helper()
	if len(events) != 1 || !strings.HasPrefix(events[0], "Warning") {
		t.Fatalf("want one Warning, got %v", events)
	}
	for _, p := range parts {
		if !strings.Contains(events[0], p) {
			t.Fatalf("the event must name %q, got %q", p, events[0])
		}
	}
}

// Three breaches an interval apart on an engine and decoder unit with a
// failure limit of three: the first two hold the step and write nothing,
// the third rolls the unit back and the Warning names the step, the count
// against the limit and each member's failing check with its value and
// threshold.
func TestReconcile_AnalysisRollbackAtTheLimitNamesTheChecks(t *testing.T) {
	steps := memberGate(0)
	steps[0].Analysis.FailureLimit = 3
	script := &valuedScript{answers: map[string]valuedAnswer{
		"engine":  {outcome: analysis.Fail, value: "0.07"},
		"decoder": {outcome: analysis.Fail, value: "0.09"},
	}}
	isvc, in := memberUnit(t, steps, true, script)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	for pass := int32(1); pass <= 2; pass++ {
		script.at = in.Now
		res := mustReconcile(t, isvc, in)
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if res.RolledBack || cs.AnalysisFailedChecks != pass {
			t.Fatalf("pass %d: a breach below the limit holds the step, got rolledBack=%v failing checks %d", pass, res.RolledBack, cs.AnalysisFailedChecks)
		}
		if got := gateRollbackEvents(rec); len(got) != 0 {
			t.Fatalf("pass %d: a breach below the limit writes no event, got %v", pass, got)
		}
		in.Now = in.Now.Add(time.Minute)
	}
	script.at = in.Now
	res := mustReconcile(t, isvc, in)
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the third breach rolls the unit back, got %+v phase %q", res, phaseOf(isvc))
	}
	wantOneWarningNaming(t, gateRollbackEvents(rec),
		"engine canary step 0",
		"failing samples reached the limit (3 of 3)",
		"engine err=0.07 (threshold LTE 0.05)",
		"decoder err=0.09 (threshold LTE 0.05)")
}

// A sample one member could not read, under onInconclusive Rollback, rolls
// the unit back on the first read: the Warning names the policy and the
// unread check alone; the member that passed is not listed.
func TestReconcile_AnalysisRollbackOnAnUnreadSampleNamesThePolicy(t *testing.T) {
	steps := memberGate(0)
	policy := v1beta1.OnInconclusiveRollback
	steps[0].Analysis.OnInconclusive = &policy
	script := &valuedScript{answers: map[string]valuedAnswer{
		"engine":  {outcome: analysis.Pass, value: "0.01"},
		"decoder": {outcome: analysis.Inconclusive, message: "no data"},
	}}
	isvc, in := memberUnit(t, steps, true, script)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	script.at = in.Now
	res := mustReconcile(t, isvc, in)
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("an unread sample under the Rollback policy rolls the unit back, got %+v phase %q", res, phaseOf(isvc))
	}
	events := gateRollbackEvents(rec)
	wantOneWarningNaming(t, events,
		"engine canary step 0",
		"onInconclusive is Rollback",
		"unread checks: decoder err (no data)")
	if strings.Contains(events[0], "engine err") {
		t.Fatalf("a check that passed is not listed, got %q", events[0])
	}
}

// No member can be read for the ready timeout under onInconclusive
// RollbackOnStall: the read inside the timeout holds and writes nothing,
// the read past it rolls the unit back and the Warning names the timeout,
// the policy and both unread checks.
func TestReconcile_AnalysisRollbackOnAStallNamesTheTimeout(t *testing.T) {
	steps := memberGate(0)
	policy := v1beta1.OnInconclusiveRollbackOnStall
	steps[0].Analysis.OnInconclusive = &policy
	script := &valuedScript{answers: map[string]valuedAnswer{
		"engine":  {outcome: analysis.Inconclusive, message: "no data"},
		"decoder": {outcome: analysis.Inconclusive, message: "no data"},
	}}
	isvc, in := memberUnit(t, steps, true, script)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	script.at = in.Now
	if res := mustReconcile(t, isvc, in); res.RolledBack {
		t.Fatalf("an unread sample inside the timeout holds the step, got %+v", res)
	}
	if got := gateRollbackEvents(rec); len(got) != 0 {
		t.Fatalf("a held step writes no rollback event, got %v", got)
	}
	in.Now = in.Now.Add(in.DefaultReadyTimeout)
	script.at = in.Now
	res := mustReconcile(t, isvc, in)
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("a stall under RollbackOnStall rolls the unit back, got %+v phase %q", res, phaseOf(isvc))
	}
	wantOneWarningNaming(t, gateRollbackEvents(rec),
		"engine canary step 0",
		"no sample could be read for 15m0s",
		"onInconclusive is RollbackOnStall",
		"engine err (no data)",
		"decoder err (no data)")
}

// emptyStableUnit is a four-Instance engine unit serving the first step of
// a metric-gated roll with every pod on the canary revision, so the stable
// revision the gate would return it to runs nothing; the sample fails.
func emptyStableUnit(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{breachingStep(), {Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", StableRevisionHash: "old", StepEnteredTime: &metav1.Time{Time: t0}}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)
	rec := record.NewFakeRecorder(8)
	pods := map[string]int32{"new": 4}
	in := baseInputs(isvc, pods)
	in.Now = t0
	in.Recorder = rec
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: pods}
	in.Sampler = &countingSample{outcome: analysis.Fail, at: t0}
	return isvc, in, rec
}

// The gate decides a rollback on a unit whose stable revision runs no
// Instance: the unit stops Failed instead, and the operator still reads
// why the gate decided, before the stop's own events.
func TestReconcile_AnalysisRollbackToAnEmptyStableStillNamesTheBreach(t *testing.T) {
	isvc, in, rec := emptyStableUnit(t)
	mustReconcile(t, isvc, in)
	if isvc.Status.Canary.Failed == nil || isvc.Status.Canary.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing {
		t.Fatalf("the unit stops Failed with the stable revision unavailable, got %+v", isvc.Status.Canary)
	}
	events := eventsFrom(rec)
	if len(events) < 2 || !strings.Contains(events[0], EventReasonCanaryAnalysisRolledBack) || !strings.Contains(events[1], EventReasonCanaryStableRevisionEmpty) {
		t.Fatalf("the gate's decision is said first, then the stop, got %v", events)
	}
	wantOneWarningNaming(t, events[:1], "engine canary step 0", "failing samples reached the limit (1 of 1)", "engine err")
}
