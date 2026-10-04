package canary

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

func onInconclusivePtr(v v1beta1.OnInconclusive) *v1beta1.OnInconclusive { return &v }

// countingSample is a fake stepSampler that records how many times it was read
// and returns a fixed outcome as an already-available (ok=true) sample — so a test
// can assert that warm-up, throttle, and promote short-circuit BEFORE any sampling.
type countingSample struct {
	outcome analysis.Outcome
	at      time.Time
	calls   int
}

func (c *countingSample) Get(_ SampleRequest, _ time.Time) (analysis.Result, time.Time, bool) {
	c.calls++
	return analysis.Result{Outcome: c.outcome, Metrics: []analysis.MetricResult{{Name: "err"}}}, c.at, true
}

func TestEvaluateAnalysisStep(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	const canaryHash = "rev-canary"
	stepNoPause := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}
	stepBake := v1beta1.RolloutGroupStep{
		Capacity: intstr.FromString("50%"), Traffic: 50,
		Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 10 * time.Minute}},
	}
	baseAnalysis := func() *v1beta1.RolloutAnalysis {
		return &v1beta1.RolloutAnalysis{
			Interval:     metav1.Duration{Duration: time.Minute},
			FailureLimit: 3,
			Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
		}
	}

	tests := []struct {
		name        string
		mutate      func(a *v1beta1.RolloutAnalysis)
		step        v1beta1.RolloutGroupStep
		cs          *v1beta1.CanaryStatus
		outcome     analysis.Outcome
		annotations map[string]string
		wantDec     stepDecision
		wantCalls   int
		wantFailed  int32
	}{
		{
			name:      "pass no-pause advances",
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}},
			outcome:   analysis.Pass,
			wantDec:   decAdvance,
			wantCalls: 1,
		},
		{
			name:      "pass before bake holds",
			step:      stepBake,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}},
			outcome:   analysis.Pass,
			wantDec:   decHold,
			wantCalls: 1,
		},
		{
			name:      "pass after bake advances",
			step:      stepBake,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now.Add(-11 * time.Minute)}},
			outcome:   analysis.Pass,
			wantDec:   decAdvance,
			wantCalls: 1,
		},
		{
			name:       "fail below limit holds and counts",
			step:       stepNoPause,
			cs:         &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}},
			outcome:    analysis.Fail,
			wantDec:    decHold,
			wantCalls:  1,
			wantFailed: 1,
		},
		{
			name:       "fail at limit rolls back",
			step:       stepNoPause,
			cs:         &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}, AnalysisFailedChecks: 2},
			outcome:    analysis.Fail,
			wantDec:    decRollback,
			wantCalls:  1,
			wantFailed: 3,
		},
		{
			name:      "inconclusive holds by default",
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}, LastConclusiveEvaluationTime: &metav1.Time{Time: now.Add(-time.Minute)}},
			outcome:   analysis.Inconclusive,
			wantDec:   decHold,
			wantCalls: 1,
		},
		{
			name:      "inconclusive rolls back when OnInconclusive=Rollback",
			mutate:    func(a *v1beta1.RolloutAnalysis) { a.OnInconclusive = onInconclusivePtr(v1beta1.OnInconclusiveRollback) },
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}},
			outcome:   analysis.Inconclusive,
			wantDec:   decRollback,
			wantCalls: 1,
		},
		{
			name:      "inconclusive past stall fails",
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now.Add(-20 * time.Minute)}, LastConclusiveEvaluationTime: &metav1.Time{Time: now.Add(-20 * time.Minute)}},
			outcome:   analysis.Inconclusive,
			wantDec:   decFailed,
			wantCalls: 1,
		},
		{
			name: "inconclusive holds before stall when OnInconclusive=RollbackOnStall",
			mutate: func(a *v1beta1.RolloutAnalysis) {
				a.OnInconclusive = onInconclusivePtr(v1beta1.OnInconclusiveRollbackOnStall)
			},
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}, LastConclusiveEvaluationTime: &metav1.Time{Time: now.Add(-time.Minute)}},
			outcome:   analysis.Inconclusive,
			wantDec:   decHold,
			wantCalls: 1,
		},
		{
			name: "inconclusive past stall rolls back when OnInconclusive=RollbackOnStall",
			mutate: func(a *v1beta1.RolloutAnalysis) {
				a.OnInconclusive = onInconclusivePtr(v1beta1.OnInconclusiveRollbackOnStall)
			},
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now.Add(-20 * time.Minute)}, LastConclusiveEvaluationTime: &metav1.Time{Time: now.Add(-20 * time.Minute)}},
			outcome:   analysis.Inconclusive,
			wantDec:   decRollback,
			wantCalls: 1,
		},
		{
			name:      "warmup holds without sampling",
			mutate:    func(a *v1beta1.RolloutAnalysis) { a.InitialDelay = &metav1.Duration{Duration: 5 * time.Minute} },
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}},
			outcome:   analysis.Pass,
			wantDec:   decHold,
			wantCalls: 0,
		},
		{
			name:      "throttle holds without sampling",
			step:      stepNoPause,
			cs:        &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now.Add(-time.Hour)}, LastEvaluationTime: &metav1.Time{Time: now.Add(-30 * time.Second)}},
			outcome:   analysis.Pass,
			wantDec:   decHold,
			wantCalls: 0,
		},
		{
			name:        "promote annotation overrides analysis",
			step:        stepBake, // bake not elapsed and sample would fail, but promote wins
			cs:          &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}},
			outcome:     analysis.Fail,
			annotations: map[string]string{constants.RolloutPromoteAnnotation: canaryHash},
			wantDec:     decAdvance,
			wantCalls:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := baseAnalysis()
			if tt.mutate != nil {
				tt.mutate(a)
			}
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns", Annotations: tt.annotations}}
			isvc.Status.Canary = tt.cs
			cnt := &countingSample{outcome: tt.outcome, at: now}
			in := ReconcileInputs{
				ISVC:               isvc,
				Component:          v1beta1.EngineComponent,
				CanaryRevisionHash: canaryHash,
				Now:                now,
				Sampler:            cnt,
				// The stall bound is operator-configured; the stall case's 20m
				// timestamps are written against this value.
				DefaultReadyTimeout: 15 * time.Minute,
			}
			got := evaluateAnalysisStep(context.Background(), in, a, tt.cs, tt.step)
			if got != tt.wantDec {
				t.Errorf("decision = %v, want %v", got, tt.wantDec)
			}
			if cnt.calls != tt.wantCalls {
				t.Errorf("sample calls = %d, want %d", cnt.calls, tt.wantCalls)
			}
			if tt.wantFailed != 0 && tt.cs.AnalysisFailedChecks != tt.wantFailed {
				t.Errorf("AnalysisFailedChecks = %d, want %d", tt.cs.AnalysisFailedChecks, tt.wantFailed)
			}
		})
	}
}

// TestEvaluateAnalysisStep_StalePlanResultNotConsumed drives the step gate
// against a real Sampler: a passing result that a since-edited plan's query
// produces late must never advance the edited plan — the edited plan holds,
// runs its own query, and consumes only its own result.
func TestEvaluateAnalysisStep_StalePlanResultNotConsumed(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	const canaryHash = "rev-canary"
	step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}
	newPlan := func(threshold string) *v1beta1.RolloutAnalysis {
		return &v1beta1.RolloutAnalysis{
			Interval:     metav1.Duration{Duration: time.Minute},
			FailureLimit: 3,
			Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: threshold}},
		}
	}
	const oldThreshold, editedThreshold = "0.05", "0.01"

	events := make(chan event.GenericEvent, 4)
	releaseOld := make(chan struct{})
	// The original plan's query is slow and would PASS; any other plan's query
	// completes immediately and FAILS.
	s := NewSampler(func(_ context.Context, req SampleRequest) analysis.Result {
		if req.Analysis.Metrics[0].Threshold == oldThreshold {
			<-releaseOld
			return analysis.Result{Outcome: analysis.Pass, Metrics: []analysis.MetricResult{{Name: "err", Passed: true}}}
		}
		return analysis.Result{Outcome: analysis.Fail, Metrics: []analysis.MetricResult{{Name: "err"}}}
	}, events, 4, time.Minute)
	s.now = func() time.Time { return now }

	cs := &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: now}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"}}
	isvc.Status.Canary = cs
	in := ReconcileInputs{
		ISVC:                     isvc,
		Component:                v1beta1.EngineComponent,
		CanaryRevisionHash:       canaryHash,
		Now:                      now,
		Sampler:                  s,
		BundledPrometheusAddress: "http://prometheus.example.com:9090",
	}

	// The original plan kicks its (slow) query and holds.
	if got := evaluateAnalysisStep(context.Background(), in, newPlan(oldThreshold), cs, step); got != decHold {
		t.Fatalf("original plan should hold while its query is in flight, got %v", got)
	}
	// The plan is edited, then the old query lands with a Pass.
	close(releaseOld)
	<-events
	// The stale Pass must not advance the edited plan: it holds and kicks a
	// fresh query, consuming nothing.
	if got := evaluateAnalysisStep(context.Background(), in, newPlan(editedThreshold), cs, step); got != decHold {
		t.Fatalf("stale Pass from the old plan must not advance the edited plan, got %v", got)
	}
	if cs.LastEvaluationTime != nil {
		t.Fatal("edited plan must not have consumed any sample yet")
	}
	<-events // the edited plan's own query lands (Fail)
	if got := evaluateAnalysisStep(context.Background(), in, newPlan(editedThreshold), cs, step); got != decHold {
		t.Fatalf("edited plan should hold on its own first failure (limit 3), got %v", got)
	}
	if cs.AnalysisFailedChecks != 1 {
		t.Fatalf("edited plan should have consumed its OWN failing sample, AnalysisFailedChecks = %d", cs.AnalysisFailedChecks)
	}
}

// consumedSamples reads the per-sample evaluation counter for one ISVC and
// Component across every outcome; it moves once per sample the gate consumed.
func consumedSamples(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) float64 {
	var total float64
	for _, outcome := range []analysis.Outcome{analysis.Pass, analysis.Fail, analysis.Inconclusive} {
		total += testutil.ToFloat64(canaryAnalysisEvaluations.WithLabelValues(
			isvc.Namespace, isvc.Name, string(component), outcomeLabel(outcome)))
	}
	return total
}

// storedStatus writes a canary status the way the apiserver keeps it and
// reads it back: its metav1.Time fields survive at whole seconds.
func storedStatus(t *testing.T, cs *v1beta1.CanaryStatus) *v1beta1.CanaryStatus {
	t.Helper()
	raw, err := json.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	stored := &v1beta1.CanaryStatus{}
	if err := json.Unmarshal(raw, stored); err != nil {
		t.Fatal(err)
	}
	return stored
}

// TestEvaluateAnalysisStep_CountsEachSampleOnce drives the gate over one
// interval per scripted sample against a real Sampler, a fake clock carrying
// sub-second detail like a wall clock, and a fake query, with the status
// written and read back between intervals as the apiserver keeps it. Each
// interval runs one new query and counts its sample once: the gate rolls back
// on the FailureLimit-th failing sample and never earlier, a breach that lifts
// before the limit lets the step pass, an unreadable source is read again, and
// the status stamps advance with every sample.
func TestEvaluateAnalysisStep_CountsEachSampleOnce(t *testing.T) {
	const (
		interval     = 20 * time.Second
		failureLimit = 4
		canaryHash   = "rev-canary"
	)
	tests := []struct {
		name          string
		samples       []analysis.Outcome
		wantDecisions []stepDecision
		wantFailed    []int32
	}{
		{
			name:          "the limit is reached on the fourth failing sample and not before",
			samples:       []analysis.Outcome{analysis.Fail, analysis.Fail, analysis.Fail, analysis.Fail},
			wantDecisions: []stepDecision{decHold, decHold, decHold, decRollback},
			wantFailed:    []int32{1, 2, 3, 4},
		},
		{
			name:          "a breach that lifts before the limit passes",
			samples:       []analysis.Outcome{analysis.Fail, analysis.Fail, analysis.Pass},
			wantDecisions: []stepDecision{decHold, decHold, decAdvance},
			wantFailed:    []int32{1, 2, 2},
		},
		{
			name:          "an unreadable source is read again after the interval",
			samples:       []analysis.Outcome{analysis.Inconclusive, analysis.Pass},
			wantDecisions: []stepDecision{decHold, decAdvance},
			wantFailed:    []int32{0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := time.Date(2026, 1, 1, 12, 0, 0, 350*int(time.Millisecond), time.UTC)
			var queries int32
			events := make(chan event.GenericEvent, 8)
			s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
				outcome := tt.samples[min(int(atomic.AddInt32(&queries, 1))-1, len(tt.samples)-1)]
				return analysis.Result{Outcome: outcome, Metrics: []analysis.MetricResult{
					{Name: "err", Value: "0.9", Passed: outcome == analysis.Pass},
				}}
			}, events, 4, time.Hour)
			s.now = func() time.Time { return clock }

			a := &v1beta1.RolloutAnalysis{
				Interval:     metav1.Duration{Duration: interval},
				FailureLimit: failureLimit,
				Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
			}
			step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 50}
			cs := &v1beta1.CanaryStatus{CanaryRevisionHash: canaryHash, StepEnteredTime: &metav1.Time{Time: clock}}
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "count-once-svc", Namespace: "ns"}}
			isvc.Status.Canary = cs
			in := ReconcileInputs{
				ISVC:                     isvc,
				Component:                v1beta1.EngineComponent,
				CanaryRevisionHash:       canaryHash,
				Sampler:                  s,
				BundledPrometheusAddress: "http://prometheus.example.com:9090",
				DefaultReadyTimeout:      15 * time.Minute,
			}

			var previousStamp time.Time
			for i := range tt.samples {
				in.Now = clock
				consumedBefore := consumedSamples(isvc, in.Component)
				// The interval's first pass finds no sample it has not consumed:
				// it kicks a query and holds without counting anything.
				if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != decHold {
					t.Fatalf("sample %d: the pass that kicks the query decided %v, want hold", i+1, got)
				}
				if consumedSamples(isvc, in.Component) != consumedBefore {
					t.Fatalf("sample %d: the pass that kicks the query consumed a sample again (AnalysisFailedChecks = %d)",
						i+1, cs.AnalysisFailedChecks)
				}
				select {
				case <-events:
				case <-time.After(10 * time.Second):
					t.Fatalf("sample %d: the source was not queried again after the interval", i+1)
				}
				if got := atomic.LoadInt32(&queries); got != int32(i+1) {
					t.Fatalf("sample %d: %d queries ran, want one per interval", i+1, got)
				}
				if got := evaluateAnalysisStep(context.Background(), in, a, cs, step); got != tt.wantDecisions[i] {
					t.Fatalf("sample %d: decision = %v, want %v", i+1, got, tt.wantDecisions[i])
				}
				if cs.AnalysisFailedChecks != tt.wantFailed[i] {
					t.Fatalf("sample %d: AnalysisFailedChecks = %d, want %d", i+1, cs.AnalysisFailedChecks, tt.wantFailed[i])
				}
				if cs.LastEvaluationTime == nil || cs.LastEvaluationTime.Time.Before(previousStamp.Add(interval)) {
					t.Fatalf("sample %d: LastEvaluationTime = %v, want the new sample's time, an interval after %v",
						i+1, cs.LastEvaluationTime, previousStamp)
				}
				if len(cs.MetricResults) != 1 || !cs.MetricResults[0].Time.Time.Equal(cs.LastEvaluationTime.Time) {
					t.Fatalf("sample %d: MetricResults = %+v, want the new sample stamped with its own time", i+1, cs.MetricResults)
				}
				previousStamp = cs.LastEvaluationTime.Time

				// The status is written and read back before the next interval.
				cs = storedStatus(t, cs)
				isvc.Status.Canary = cs
				clock = clock.Add(interval)
			}
		})
	}
}
