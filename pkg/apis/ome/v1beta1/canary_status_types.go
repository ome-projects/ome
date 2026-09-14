package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// CanaryStatus tracks progress of a spec.rollout.groups[].canary rollout. It is the
// executor's persistent state machine: which step is active, when it was
// entered (Auto promotion measures Pause.Duration from here), and which
// revision is the canary. Absent when no canary is in progress.
type CanaryStatus struct {
	// TargetID identifies the canary group's pinned Component target set. A new
	// target set re-arms the canary even when the externally routed Component's
	// revision did not change. It is scoped to the canary group rather than the
	// whole rollout run, so unrelated groups cannot reset canary progress.
	// +optional
	TargetID string `json:"targetID,omitempty"`

	// CanaryRevisionHash is the revision hash being rolled out.
	// +optional
	CanaryRevisionHash string `json:"canaryRevisionHash,omitempty"`

	// StableRevisionHash is the revision hash that was serving as stable when
	// the canary began — the rollback target. It is preserved across mid-canary
	// retargets so a rollback always returns to the pre-canary stable revision,
	// never to a partially-rolled intermediate. Cleared when the canary
	// completes (the canary revision becomes the new stable).
	// +optional
	StableRevisionHash string `json:"stableRevisionHash,omitempty"`

	// CurrentStep is the zero-based index into spec.rollout.groups[i].canary.steps.
	CurrentStep int32 `json:"currentStep"`

	// StepEnteredTime is when CurrentStep was entered. Auto promotion measures
	// Pause.Duration from this timestamp.
	// +optional
	StepEnteredTime *metav1.Time `json:"stepEnteredTime,omitempty"`

	// ObservedTrafficWeight is the external traffic weight currently programmed
	// for the canary revision (mirrors the active step's TrafficWeight once
	// applied).
	ObservedTrafficWeight int32 `json:"observedTrafficWeight"`

	// PromotedThrough is the ome.io/rollout-promote value the most recent step
	// advance applied. It is stamped in the same status write as the advance —
	// the durable record of the promotion, since metadata (the annotation) and
	// status cannot be written atomically. While set, a lingering annotation
	// with this value is inert (already applied; removal is retried
	// best-effort). While the rollout is in progress it clears once the
	// annotation is observed removed, re-arming manual promotion for later
	// steps.
	// +optional
	PromotedThrough string `json:"promotedThrough,omitempty"`

	// PreStepHold holds the canary's traffic at its currently-programmed
	// weight after a repin whose clamped step would raise exposure: a repin
	// may only hold or tighten, so the raise waits for an explicit
	// ome.io/rollout-promote (value = the canary revision hash). Capacity may
	// still converge to the clamped step — capacity ahead of traffic is the
	// supported warm-up pattern; only traffic and step advance hold.
	// +optional
	PreStepHold bool `json:"preStepHold,omitempty"`

	// RolledBackRevisionHash is set when a rollback (ome.io/rollout-rollback)
	// abandons a canary: it records the primary Component's rejected revision
	// hash. While set, the group is held on its stable revisions and the rejected
	// target set is NOT retried — even after the annotation is cleared. The
	// rollout re-arms only when a different target set appears.
	// +optional
	RolledBackRevisionHash string `json:"rolledBackRevisionHash,omitempty"`

	// AnalysisFailedChecks counts failing analysis samples in the CURRENT step
	// (reset to zero on each advance). Auto-rollback fires when it reaches
	// Analysis.FailureLimit. Set only under Promotion=Analysis.
	// +optional
	AnalysisFailedChecks int32 `json:"analysisFailedChecks,omitempty"`

	// LastEvaluationTime is when the analysis metrics were last sampled. The
	// controller throttles sampling to at most once per Analysis.Interval using
	// this timestamp, so a burst of unrelated reconciles cannot over-count
	// failures. Set only under Promotion=Analysis.
	// +optional
	LastEvaluationTime *metav1.Time `json:"lastEvaluationTime,omitempty"`

	// LastConclusiveEvaluationTime is when analysis last produced a conclusive
	// sample (pass or fail — i.e. Prometheus answered). The stall timeout is
	// measured from here: a long run of inconclusive samples escalates to Failed.
	// Set only under Promotion=Analysis.
	// +optional
	LastConclusiveEvaluationTime *metav1.Time `json:"lastConclusiveEvaluationTime,omitempty"`

	// MetricResults is the most recent per-metric evaluation, for observability
	// (kubectl get isvc shows why a step held or rolled back). Set only under
	// Promotion=Analysis.
	// +optional
	// +listType=map
	// +listMapKey=name
	MetricResults []AnalysisMetricResult `json:"metricResults,omitempty"`
}

// AnalysisMetricResult is the last observed evaluation of one AnalysisMetric.
// Value is a string (not a float) because the Kubernetes API convention avoids
// floating-point fields; it is for display only.
type AnalysisMetricResult struct {
	// Name matches the AnalysisMetric.Name this result is for.
	Name string `json:"name"`

	// Value is the query result at the last sample, formatted for display
	// (e.g. "0.012"). Empty when the sample was inconclusive.
	// +optional
	Value string `json:"value,omitempty"`

	// Threshold echoes the bound that was applied, so the result is
	// self-describing in status.
	// +optional
	Threshold string `json:"threshold,omitempty"`

	// Operator echoes the comparison that was applied.
	// +optional
	Operator ComparisonOperator `json:"operator,omitempty"`

	// Passed is whether this metric satisfied its condition at the last sample.
	Passed bool `json:"passed"`

	// Message carries the reason when a metric did not pass or could not be
	// evaluated (e.g. "no data", a query error).
	// +optional
	Message string `json:"message,omitempty"`

	// Time is when this result was recorded.
	// +optional
	Time *metav1.Time `json:"time,omitempty"`
}
