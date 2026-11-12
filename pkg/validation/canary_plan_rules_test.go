package validation

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// planStep is one canary step of a plan under test.
func planStep(capacity intstr.IntOrString, traffic int32) v1beta1.RolloutGroupStep {
	return v1beta1.RolloutGroupStep{Capacity: capacity, Traffic: traffic}
}

// percentStep is a step with a percentage capacity.
func percentStep(capacity string, traffic int32) v1beta1.RolloutGroupStep {
	return planStep(intstr.FromString(capacity), traffic)
}

// finalStep is the last step every valid plan ends with.
func finalStep() v1beta1.RolloutGroupStep {
	return percentStep("100%", 100)
}

// planOf builds a canary plan from steps.
func planOf(steps ...v1beta1.RolloutGroupStep) *v1beta1.GroupCanary {
	return &v1beta1.GroupCanary{Steps: steps}
}

// planAnalysis is a complete metric-gated step body; edits make one rule fail.
func planAnalysis(edit func(*v1beta1.RolloutAnalysis)) *v1beta1.RolloutAnalysis {
	a := &v1beta1.RolloutAnalysis{
		Interval:     metav1.Duration{Duration: time.Minute},
		FailureLimit: 3,
		Metrics:      []v1beta1.AnalysisMetric{{Name: "errors", Query: "rate(errors[1m])", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
	}
	if edit != nil {
		edit(a)
	}
	return a
}

// TestValidateCanaryPlanRefusalsNameTheFieldAndTheRule holds the whole text of
// every refusal the plan body rules give, with the field path the caller
// passes in front.
func TestValidateCanaryPlanRefusalsNameTheFieldAndTheRule(t *testing.T) {
	for _, tt := range []struct {
		name string
		plan *v1beta1.GroupCanary
		want string
	}{
		{name: "no steps", plan: planOf(), want: "spec.canary.steps must not be empty (CanaryInvalid)"},
		{name: "a readyTimeout of zero", plan: func() *v1beta1.GroupCanary {
			p := planOf(finalStep())
			p.ReadyTimeout = &metav1.Duration{}
			return p
		}(), want: "spec.canary.readyTimeout must be > 0 when set (CanaryInvalid)"},
		{name: "a negative readyTimeout", plan: func() *v1beta1.GroupCanary {
			p := planOf(finalStep())
			p.ReadyTimeout = &metav1.Duration{Duration: -time.Second}
			return p
		}(), want: "spec.canary.readyTimeout must be > 0 when set (CanaryInvalid)"},
		{name: "a final step below full traffic", plan: planOf(percentStep("100%", 50)),
			want: "spec.canary final step traffic must be 100, got 50 (CanaryInvalid)"},
		{name: "a quoted count is not a percentage", plan: planOf(percentStep("3", 10), finalStep()),
			want: `spec.canary.steps[0].capacity: capacity "3" is not a percentage — use "<N>%" for a fraction of desired replicas, or an unquoted integer for an absolute count (CanaryInvalid)`},
		{name: "a percentage that is not a number", plan: planOf(percentStep("ten%", 10), finalStep()),
			want: `spec.canary.steps[0].capacity: capacity "ten%" is not a valid percentage — use "<N>%" with integer N in [0,100] (CanaryInvalid)`},
		{name: "a negative percentage", plan: planOf(percentStep("-10%", 10), finalStep()),
			want: `spec.canary.steps[0].capacity: capacity "-10%" must not be negative (CanaryInvalid)`},
		{name: "a percentage above 100", plan: planOf(percentStep("150%", 10), finalStep()),
			want: `spec.canary.steps[0].capacity: capacity "150%" must not exceed 100% (capacity is a fraction of desired replicas) (CanaryInvalid)`},
		{name: "a negative count", plan: planOf(planStep(intstr.FromInt(-1), 10), finalStep()),
			want: "spec.canary.steps[0].capacity: capacity -1 must not be negative (CanaryInvalid)"},
		{name: "traffic above 100", plan: planOf(percentStep("50%", 101), finalStep()),
			want: "spec.canary.steps[0].traffic: traffic 101 must be in [0,100] (CanaryInvalid)"},
		{name: "negative traffic", plan: planOf(percentStep("50%", -1), finalStep()),
			want: "spec.canary.steps[0].traffic: traffic -1 must be in [0,100] (CanaryInvalid)"},
		{name: "traffic that falls", plan: planOf(percentStep("50%", 50), percentStep("60%", 30), finalStep()),
			want: "spec.canary.steps[1].traffic: traffic 30 must be >= the previous step's 50 (canary weights are non-decreasing) (CanaryInvalid)"},
		{name: "traffic to a zero count", plan: planOf(planStep(intstr.FromInt(0), 10), finalStep()),
			want: `spec.canary.steps[0].capacity: capacity "0" resolves to zero new capacity but traffic is 10 — a step cannot send traffic to zero capacity (CanaryInvalid)`},
		{name: "traffic to zero percent", plan: planOf(percentStep("0%", 10), finalStep()),
			want: `spec.canary.steps[0].capacity: capacity "0%" resolves to zero new capacity but traffic is 10 — a step cannot send traffic to zero capacity (CanaryInvalid)`},
		{name: "a final percentage below 100", plan: planOf(percentStep("10%", 10), percentStep("50%", 100)),
			want: `spec.canary.steps[1].capacity: final step capacity must be 100% (rollout completes at full capacity), got "50%" (CanaryInvalid)`},
		{name: "an analysis interval of zero", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Interval = metav1.Duration{} })}),
			want: "spec.canary.steps[0].analysis: interval must be > 0 (AnalysisInvalid)"},
		{name: "an analysis failure limit of zero", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.FailureLimit = 0 })}),
			want: "spec.canary.steps[0].analysis: failureLimit must be >= 1 (AnalysisInvalid)"},
		{name: "an analysis without metrics", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics = nil })}),
			want: "spec.canary.steps[0].analysis: at least one metric is required (AnalysisInvalid)"},
		{name: "a metric without a name", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Name = "" })}),
			want: "spec.canary.steps[0].analysis.metrics[0].name must not be empty (AnalysisInvalid)"},
		{name: "two metrics with one name", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics = append(a.Metrics, a.Metrics[0]) })}),
			want: `spec.canary.steps[0].analysis.metrics[1]: duplicate metric name "errors" (AnalysisInvalid)`},
		{name: "a metric with a blank query", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Query = "  " })}),
			want: `spec.canary.steps[0].analysis.metrics[0] ("errors"): query must not be empty (AnalysisInvalid)`},
		{name: "a metric whose threshold is not a number", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Threshold = "five" })}),
			want: `spec.canary.steps[0].analysis.metrics[0] ("errors"): threshold "five" must be numeric (AnalysisInvalid)`},
		{name: "a metric with an unknown operator", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Operator = "EQ" })}),
			want: `spec.canary.steps[0].analysis.metrics[0] ("errors"): operator "EQ" must be one of LT/LTE/GT/GTE (AnalysisInvalid)`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCanaryPlan("spec.canary", tt.plan)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("ValidateCanaryPlan = %v\nwant %s", err, tt.want)
			}
		})
	}
}

// TestValidateCanaryPlanChecksTheRulesInOrder gives a plan two faults at once
// and holds which refusal the operator reads: the whole plan's rules first
// (steps, every analysis, readyTimeout, the final traffic), then step by step
// the capacity form, the traffic range, the traffic order, traffic to zero
// capacity, and last the final percentage.
func TestValidateCanaryPlanChecksTheRulesInOrder(t *testing.T) {
	badInterval := planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Interval = metav1.Duration{} })
	withTimeout := func(p *v1beta1.GroupCanary) *v1beta1.GroupCanary {
		p.ReadyTimeout = &metav1.Duration{}
		return p
	}
	for _, tt := range []struct {
		name string
		plan *v1beta1.GroupCanary
		want string
	}{
		{name: "no steps before the readyTimeout", plan: withTimeout(planOf()), want: "spec.canary.steps must not be empty (CanaryInvalid)"},
		{name: "a later step's analysis before the readyTimeout",
			plan: withTimeout(planOf(percentStep("10%", 10), v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: badInterval})),
			want: "spec.canary.steps[1].analysis: interval must be > 0 (AnalysisInvalid)"},
		{name: "a later step's analysis before an earlier step's capacity",
			plan: planOf(percentStep("abc", 10), v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: badInterval}),
			want: "spec.canary.steps[1].analysis: interval must be > 0 (AnalysisInvalid)"},
		{name: "the readyTimeout before the final traffic", plan: withTimeout(planOf(percentStep("100%", 50))),
			want: "spec.canary.readyTimeout must be > 0 when set (CanaryInvalid)"},
		{name: "the final traffic before an earlier step's capacity", plan: planOf(percentStep("abc", 10), percentStep("100%", 50)),
			want: "spec.canary final step traffic must be 100, got 50 (CanaryInvalid)"},
		{name: "the capacity form before the traffic range", plan: planOf(percentStep("abc", 101), finalStep()),
			want: `spec.canary.steps[0].capacity: capacity "abc" is not a percentage — use "<N>%" for a fraction of desired replicas, or an unquoted integer for an absolute count (CanaryInvalid)`},
		{name: "the traffic range before the traffic order", plan: planOf(percentStep("50%", 50), percentStep("60%", -1), finalStep()),
			want: "spec.canary.steps[1].traffic: traffic -1 must be in [0,100] (CanaryInvalid)"},
		{name: "the traffic order before traffic to zero capacity", plan: planOf(percentStep("50%", 50), percentStep("0%", 30), finalStep()),
			want: "spec.canary.steps[1].traffic: traffic 30 must be >= the previous step's 50 (canary weights are non-decreasing) (CanaryInvalid)"},
		{name: "traffic to zero capacity before the final percentage", plan: planOf(percentStep("10%", 10), percentStep("0%", 100)),
			want: `spec.canary.steps[1].capacity: capacity "0%" resolves to zero new capacity but traffic is 100 — a step cannot send traffic to zero capacity (CanaryInvalid)`},
		{name: "an earlier step before a later one", plan: planOf(percentStep("50%", 101), percentStep("abc", 100)),
			want: "spec.canary.steps[0].traffic: traffic 101 must be in [0,100] (CanaryInvalid)"},
		{name: "the interval before the failure limit",
			plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Interval, a.FailureLimit = metav1.Duration{}, 0 })}),
			want: "spec.canary.steps[0].analysis: interval must be > 0 (AnalysisInvalid)"},
		{name: "the failure limit before the metrics",
			plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.FailureLimit, a.Metrics = 0, nil })}),
			want: "spec.canary.steps[0].analysis: failureLimit must be >= 1 (AnalysisInvalid)"},
		{name: "a metric's name before its query",
			plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Name, a.Metrics[0].Query = "", "" })}),
			want: "spec.canary.steps[0].analysis.metrics[0].name must not be empty (AnalysisInvalid)"},
		{name: "a metric's query before its threshold",
			plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Query, a.Metrics[0].Threshold = "", "x" })}),
			want: `spec.canary.steps[0].analysis.metrics[0] ("errors"): query must not be empty (AnalysisInvalid)`},
		{name: "a metric's threshold before its operator",
			plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) { a.Metrics[0].Threshold, a.Metrics[0].Operator = "x", "EQ" })}),
			want: `spec.canary.steps[0].analysis.metrics[0] ("errors"): threshold "x" must be numeric (AnalysisInvalid)`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCanaryPlan("spec.canary", tt.plan)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("ValidateCanaryPlan = %v\nwant %s", err, tt.want)
			}
		})
	}
}

// TestValidateCanaryPlanAcceptsTheFormsTheResolverReads holds the plans that
// pass: percentages, counts, a count as the final step, zero capacity with
// zero traffic, equal traffic on consecutive steps, a positive readyTimeout
// and an analysis with each comparison operator.
func TestValidateCanaryPlanAcceptsTheFormsTheResolverReads(t *testing.T) {
	for _, tt := range []struct {
		name string
		plan *v1beta1.GroupCanary
	}{
		{name: "one full step", plan: planOf(finalStep())},
		{name: "percentages", plan: planOf(percentStep("25%", 10), percentStep("50%", 50), finalStep())},
		{name: "counts", plan: planOf(planStep(intstr.FromInt(1), 10), planStep(intstr.FromInt(5), 50), finalStep())},
		{name: "a count as the final step", plan: planOf(percentStep("50%", 50), planStep(intstr.FromInt(5), 100))},
		{name: "a count above 100 as the final step", plan: planOf(percentStep("50%", 50), planStep(intstr.FromInt(150), 100))},
		{name: "zero capacity with zero traffic", plan: planOf(planStep(intstr.FromInt(0), 0), percentStep("0%", 0), finalStep())},
		{name: "equal traffic on consecutive steps", plan: planOf(percentStep("10%", 10), percentStep("20%", 10), finalStep())},
		{name: "a positive readyTimeout", plan: func() *v1beta1.GroupCanary {
			p := planOf(finalStep())
			p.ReadyTimeout = &metav1.Duration{Duration: time.Nanosecond}
			return p
		}()},
		{name: "an analysis with every operator", plan: planOf(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100,
			Analysis: planAnalysis(func(a *v1beta1.RolloutAnalysis) {
				a.FailureLimit = 1
				a.Metrics = []v1beta1.AnalysisMetric{
					{Name: "lt", Query: "q", Operator: v1beta1.ComparisonLT, Threshold: "1"},
					{Name: "lte", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "-1.5"},
					{Name: "gt", Query: "q", Operator: v1beta1.ComparisonGT, Threshold: "1e3"},
					{Name: "gte", Query: "q", Operator: v1beta1.ComparisonGTE, Threshold: "0"},
				}
			})})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateCanaryPlan("spec.canary", tt.plan); err != nil {
				t.Fatalf("ValidateCanaryPlan = %v, want nil", err)
			}
		})
	}
}

// TestValidateCanaryPlanRefusalsOpenWithTheCallersPath holds that the path the
// caller passes opens every refusal, including the composed form the run open
// uses and an empty path.
func TestValidateCanaryPlanRefusalsOpenWithTheCallersPath(t *testing.T) {
	for _, tt := range []struct {
		where string
		plan  *v1beta1.GroupCanary
		want  string
	}{
		{where: "spec.rollout.groups[2].canary", plan: planOf(percentStep("10%", 10), percentStep("50%", 100)),
			want: `spec.rollout.groups[2].canary.steps[1].capacity: final step capacity must be 100% (rollout completes at full capacity), got "50%" (CanaryInvalid)`},
		{where: "groups[0].canary (composed)", plan: planOf(percentStep("100%", 60)),
			want: "groups[0].canary (composed) final step traffic must be 100, got 60 (CanaryInvalid)"},
		{where: "", plan: planOf(), want: ".steps must not be empty (CanaryInvalid)"},
	} {
		t.Run(tt.where, func(t *testing.T) {
			err := ValidateCanaryPlan(tt.where, tt.plan)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("ValidateCanaryPlan = %v\nwant %s", err, tt.want)
			}
		})
	}
}
