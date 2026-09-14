// Package v1beta1 — rollout types.
//
// spec.rollout is the single home for InferenceService rollout configuration.
package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// RolloutSpec is the single rollout surface for an InferenceService: an ordered
// list of rollout groups. There is no coordination-vs-canary fork — sequencing
// is the list order, and canary is one progression a group may choose.
type RolloutSpec struct {
	// Groups is the ORDERED list of rollout groups. Sequence is the list
	// position: group N reaches completion before group N+1 begins. Components
	// listed together in one group roll together (a coupled unit); Components not
	// listed in any group roll independently (the default). To roll Components
	// one-at-a-time, put each in its own group, in order — a group needs only its
	// components; the progression may be omitted and defaults to blueGreen.
	//
	// Cross-group sequencing is enforced only for a run of single-Component
	// blueGreen groups (the classic one-at-a-time shape). Admission rejects any
	// other multi-group list — a rollingUpdate, canary, or multi-Component group
	// in an ordered list would run concurrently, and an ordering the engine does
	// not enforce is rejected rather than accepted. The rejection applies on
	// create and on any update that changes spec.rollout, so stored objects
	// keep reconciling until their rollout is next edited.
	//
	// Only meaningful for OMENative-managed Components.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=3
	Groups []RolloutGroup `json:"groups,omitempty"`

	// PairingProtocol declares the engine↔decoder wire-compatibility contract
	// for prefill-decode pairing (e.g. a KV-transfer protocol generation, such
	// as "nixl-v2"). It is an opaque, operator-owned token compared only for
	// equality; bump it ONLY when the engine↔decoder wire contract breaks.
	// The value rides each engine and decoder revision: changing it mints new
	// revisions for both Components (a rollout), stamps ome.io/pairing-protocol
	// on their pods and per-revision Services, and is published on each traffic
	// target so routing pairs only engine and decoder revisions whose values
	// are equal. Empty or unset pairs with anything, so setting or clearing the
	// token is upgrade-neutral. Changing it between two non-empty values
	// requires engine and decoder to roll as one blueGreen or canary group.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`
	PairingProtocol *string `json:"pairingProtocol,omitempty"`
}

// RolloutGroup is a set of Components that roll together, advancing through at
// most one progression. The progression is a presence-based one-of: set Canary,
// BlueGreen, or RollingUpdate — or none, which defaults to BlueGreen. Omitting it
// is the ergonomic spelling for "just roll this group" and for a single-Component
// group in a sequence (`components: [decoder]` then `components: [engine]`),
// where the empty `blueGreen: {}` selector would be pure ceremony.
//
// Failure mode if mis-set: two progressions is ambiguous (which one drives
// traffic?) and is rejected by the rule below.
// +kubebuilder:validation:XValidation:rule="(has(self.canary)?1:0) + (has(self.blueGreen)?1:0) + (has(self.rollingUpdate)?1:0) <= 1",message="at most one of canary, blueGreen, or rollingUpdate may be set on a rollout group"
// +kubebuilder:validation:XValidation:rule="!has(self.order) || self.order.all(c, c in self.components)",message="order must reference only Components listed in this group"
type RolloutGroup struct {
	// Components lists the Component names in this group (they roll together).
	// Valid values: "router", "engine", "decoder".
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +listType=atomic
	Components []ComponentType `json:"components"`

	// Order optionally pins the sequence in which this group's Components roll.
	// No progression applies it — the Components in a group advance together —
	// so admission rejects a non-empty order (on create and on any update that
	// changes spec.rollout) rather than accept a sequence the engine does not
	// enforce. To roll Components one at a time, declare one single-Component
	// blueGreen group per Component, in the desired list order. Must reference
	// only Components in this group.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=3
	Order []ComponentType `json:"order,omitempty"`

	// Canary advances the group through explicit capacity + service-level-traffic
	// steps with optional gates. One-of progression.
	// +optional
	Canary *GroupCanary `json:"canary,omitempty"`

	// BlueGreen surges the full new set, then flips traffic atomically. One-of
	// progression.
	// +optional
	BlueGreen *GroupBlueGreen `json:"blueGreen,omitempty"`

	// RollingUpdate replaces pods gradually within the surge/unavailable budget,
	// tolerating mixed revisions during the window. One-of progression.
	// +optional
	RollingUpdate *GroupRollingUpdate `json:"rollingUpdate,omitempty"`

	// PolicyRef names a same-namespace RolloutPolicy that supplies this
	// group's progression. A sibling of the progression one-of, not an arm of
	// it: the ref may coexist with one inline progression, and the inline
	// block wins (the preview/rollback mechanism) — the shadowed policy's
	// would-pin digest is published in status.rollout.groups[]. A group whose
	// only progression is an unresolvable ref PARKS at run open (fail-closed);
	// it never falls back to the default blueGreen, because silently removing
	// a declared gate is the failure this API exists to prevent.
	// +optional
	PolicyRef *RolloutPolicyRef `json:"policyRef,omitempty"`

	// Soak is the wait AFTER this group completes, before the next group begins.
	// It is honored only when the rollout is a sequence of single-Component
	// blueGreen groups (the run that collapses to the internal Sequential state
	// machine); ignored on the last group. Admission rejects a soak the engine
	// would silently drop — on a canary, multi-Component, or rollingUpdate group,
	// or on a rollout that does not collapse to that sequence.
	// +optional
	Soak *metav1.Duration `json:"soak,omitempty"`

	// MaintainRatio guards the cross-Component replica ratio while this group
	// rolls. Meaningful only on multi-Component blueGreen/rollingUpdate groups
	// (e.g. a PD engine+decoder pair); ignored on single-Component groups and
	// rejected by admission on canary groups (the canary engine does not enforce
	// it).
	// +optional
	MaintainRatio *MaintainRatio `json:"maintainRatio,omitempty"`
}

// GroupCanary is the stepped progression: bring up new-revision capacity and
// shift service-level traffic through an ordered list of steps. How the canary
// advances is a PER-STEP property (see RolloutGroupStep): each step is
// metric-gated (analysis), timed, manual, or immediate. Prometheus here is the
// shared metrics SOURCE every analysis step queries — declared once, not on each
// step.
type GroupCanary struct {
	// Steps is the ordered progression of capacity + service-level traffic. The
	// final step must reach 100% traffic for the rollout to complete.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=20
	Steps []RolloutGroupStep `json:"steps,omitempty"`

	// Prometheus is the metrics source (connection) shared by every analysis step
	// in this canary: where to query and how to authenticate. When omitted (or when
	// its serverAddress is empty), the operator-configured default source
	// (canaryAnalysis.bundledPrometheusAddress) is used; if that is also unset the
	// analysis has no source and its samples read inconclusive. It is only the source — WHAT to
	// check is each step's analysis — and it gates nothing by itself.
	// +optional
	Prometheus *AnalysisPrometheus `json:"prometheus,omitempty"`

	// ScaleDownDelaySeconds is the wait between shifting traffic off the old
	// revision and scaling its pods down, to drain in-flight requests.
	// +optional
	ScaleDownDelaySeconds *int32 `json:"scaleDownDelaySeconds,omitempty"`

	// ReadyTimeout bounds how long a step's capacity gate may stay
	// unsatisfied before the canary is marked Failed (stable keeps serving —
	// no traffic has shifted). Unset falls back to the operator-configured
	// default (the rollout block of the operator's ConfigMap); the
	// ome.io/rollout-ready-timeout annotation still overrides both.
	// +optional
	ReadyTimeout *metav1.Duration `json:"readyTimeout,omitempty"`
}

// RolloutGroupStep is one stage of a canary progression. Capacity and traffic
// are INDEPENDENT: capacity is the per-Component new-revision pod count; traffic
// is the service-level percentage of requests on the new revision. The
// independence allows "capacity ahead of traffic" (run 50% pods, send 10%
// traffic) for warm-up.
//
// How the step advances (its gate) is resolved in order:
//   - Analysis set → metric-gated (queries GroupCanary.Prometheus); Pause.Duration,
//     if any, is the minimum bake before a passing sample advances.
//   - else Pause with Duration → timed: advance once Duration elapses.
//   - else Pause → manual: hold for the ome.io/rollout-promote annotation.
//   - else → advance as soon as capacity + traffic converge.
type RolloutGroupStep struct {
	// Capacity is the fraction of desired replicas to run on the new revision at
	// this step. Percentage ("25%") or absolute ("3"). Per-Component.
	// +kubebuilder:validation:XIntOrString
	Capacity intstr.IntOrString `json:"capacity"`

	// Traffic is the service-level percentage of requests routed to the new
	// revision at this step (0-100). Not per-Component.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Traffic int32 `json:"traffic"`

	// Pause holds the step: with a Duration it is a timed advance (or, when
	// Analysis is set, the bake window); without one it is a manual hold for the
	// ome.io/rollout-promote annotation. Absent (and no Analysis) advances
	// immediately.
	// +optional
	Pause *RolloutPause `json:"pause,omitempty"`

	// Analysis opts this step into metric-gated promotion: it carries the checks
	// (metrics) and policy (interval, failureLimit, onInconclusive) for THIS step.
	// The metrics source is GroupCanary.Prometheus, shared by all analysis steps.
	// Absent leaves the step manual/timed/immediate per Pause.
	// +optional
	Analysis *RolloutAnalysis `json:"analysis,omitempty"`
}

// GroupBlueGreen surges the full new set alongside the old, waits for Ready, then
// flips traffic atomically. It is also the default progression a group falls back
// to when none of canary/blueGreen/rollingUpdate is set. It carries no traffic
// steps (the flip is all-or-nothing); surge sequencing across Components is the
// group-level Order field, not a blue-green-specific knob. Empty; reserved for
// blue-green-specific options (e.g. a pre-flip verification window).
type GroupBlueGreen struct{}

// GroupRollingUpdate replaces pods gradually, paced by the surge/unavailable
// budget. Mixed revisions are tolerated during the window.
type GroupRollingUpdate struct {
	// MaxSurge is the maximum pods (fraction or absolute) above desired during
	// the roll. Default 25%.
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

	// MaxUnavailable is the maximum pods (fraction or absolute) that may be
	// not-Ready at once during the roll. Default 25% (matches the Kubernetes
	// Deployment convention; a default of 0 would deadlock single-replica rolls).
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// MaintainRatio bounds how far a multi-Component group may drift from the
// cross-Component replica ratio observed at rollout start (e.g. prefill:decode).
type MaintainRatio struct {
	// Tolerance is the maximum percentage drift from the starting ratio. A roll
	// step that would exceed it is paused until the pools rebalance. An explicit
	// 0 means zero drift. When omitted, the operator-configured default (the
	// coordination block of the operator's ConfigMap) applies; if the operator
	// configures no default either, the group rolls with no drift bound.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Tolerance *int32 `json:"tolerance,omitempty"`
}

// RolloutPause holds a rollout at a step boundary; its meaning depends on the
// step's gate (see RolloutGroupStep):
//   - manual step (Pause, no Duration): wait for the ome.io/rollout-promote
//     annotation.
//   - timed step (Pause with Duration, no Analysis): advance after Duration.
//   - analysis step (Analysis set): Duration is the minimum bake time before a
//     passing sample may advance.
type RolloutPause struct {
	// Duration to pause at this step; see the type doc for how each gate uses it.
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`
}

// RolloutAnalysis is a step's metric gate: the checks (metrics) and
// policy (interval, failureLimit, onInconclusive, initialDelay) that decide
// advance, hold, or rollback for the step it is set on. It does NOT carry the
// metrics source — that is GroupCanary.Prometheus, shared by all analysis steps.
type RolloutAnalysis struct {
	// Interval is the sampling cadence — how often the controller re-evaluates the
	// metrics while a step bakes. It also bounds the requeue while holding.
	Interval metav1.Duration `json:"interval"`

	// InitialDelay is a warm-up after a step's pods become Ready and traffic is
	// applied, before the first sample. New pods need a moment before their metrics
	// mean anything; sampling immediately would read cold-start noise.
	// +optional
	InitialDelay *metav1.Duration `json:"initialDelay,omitempty"`

	// FailureLimit is the number of failing samples, counted PER STEP (reset on
	// advance), that triggers auto-rollback. FailureLimit=1 rolls back on the first
	// bad sample.
	// +kubebuilder:validation:Minimum=1
	FailureLimit int32 `json:"failureLimit"`

	// OnInconclusive selects what to do when a sample cannot be completed
	// (Prometheus unreachable, query error, or empty result) past the stall
	// timeout — distinct from a metric breach.
	//   - Hold (default): keep traffic at the current step and escalate to Failed
	//     for an operator decision. A monitoring outage is not evidence the canary
	//     is bad.
	//   - Rollback: fail-safe — treat "can't tell" as "assume bad" and roll back
	//     on the first inconclusive sample, without waiting out the stall timeout.
	//   - RollbackOnStall: ride out transient gaps like Hold, but roll back
	//     instead of parking once the stall timeout expires.
	// +kubebuilder:validation:Enum=Hold;Rollback;RollbackOnStall
	// +optional
	OnInconclusive *OnInconclusive `json:"onInconclusive,omitempty"`

	// Metrics are the success conditions. A sample passes only when EVERY metric
	// passes (AND). At least one is required; an analysis with no metrics could
	// never fail and would gate nothing.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=10
	// +listType=map
	// +listMapKey=name
	Metrics []AnalysisMetric `json:"metrics"`
}

// AnalysisPrometheus locates and authenticates to the Prometheus the analysis
// queries.
//
// +kubebuilder:validation:XValidation:rule="!(has(self.providerRef) && has(self.serverAddress))",message="at most one of providerRef and serverAddress may be set"
type AnalysisPrometheus struct {
	// ProviderRef names a logical metric provider bound per cluster in the
	// operator's metricProviders configuration (shared with AutoscalerPolicy
	// triggers), which supplies the server address, auth secret, and default
	// headers. In a RolloutPolicy body this is the ONLY admitted spelling — a
	// raw cluster-local URL inside a fleet-portable object defeats
	// portability and is an SSRF surface. An unbound name is a deterministic
	// config error: the rollout parks at run open rather than starting a roll
	// whose gate cannot sample.
	// +optional
	ProviderRef *MetricProviderRef `json:"providerRef,omitempty"`

	// ServerAddress is the base URL of the Prometheus HTTP API
	// (e.g. http://prometheus.monitoring.svc:9090). Optional: when empty (and
	// no ProviderRef is set), the operator-configured default source
	// (canaryAnalysis.bundledPrometheusAddress)
	// is used; if that is also unset the analysis has no source and its samples read
	// inconclusive. Admission does not check the source — an unreachable or missing
	// source surfaces controller-side as an inconclusive sample, not a rejection.
	// +optional
	ServerAddress string `json:"serverAddress,omitempty"`

	// AuthRef points at a Secret key (in the InferenceService's namespace) holding
	// a bearer token sent as "Authorization: Bearer <token>". Omit for an
	// unauthenticated Prometheus (the bundled one). TLS/mTLS is not supported.
	// +optional
	AuthRef *corev1.SecretKeySelector `json:"authRef,omitempty"`

	// Headers are extra HTTP headers sent on every Prometheus query — e.g.
	// "X-Scope-OrgID" for a multi-tenant Cortex/Thanos/Mimir front-end that scopes
	// queries by tenant. Without the tenant header such a backend returns no data,
	// which the gate reads as a permanent stall. Plaintext (not secret); use
	// AuthRef for the bearer token.
	// +optional
	Headers map[string]string `json:"headers,omitempty"`
}

// AnalysisMetric is one success condition: a PromQL query whose scalar result is
// compared against Threshold with Operator. A multi-series result is reduced to
// the worst series for the operator before comparison — the maximum for LT/LTE,
// the minimum for GT/GTE — so the metric passes only when every series passes.
type AnalysisMetric struct {
	// Name identifies the metric in status and events; unique within Metrics.
	Name string `json:"name"`

	// Query is a PromQL expression, templated with the rollout's revision context
	// before execution. Available variables: {{.Namespace}}, {{.ISVCName}},
	// {{.Component}}, {{.CanaryService}}, {{.StableService}}, {{.CanaryRevision}},
	// {{.StableRevision}}. A result with multiple series is reduced to the worst
	// series for Operator before comparison.
	Query string `json:"query"`

	// Operator is the comparison applied as "result <Operator> Threshold" to
	// decide whether this metric passes.
	// +kubebuilder:validation:Enum=LT;LTE;GT;GTE
	Operator ComparisonOperator `json:"operator"`

	// Threshold is the bound the query result is compared against. Parsed as a
	// float (e.g. "0.05", "200", "1.5"); admission rejects a non-numeric value.
	Threshold string `json:"threshold"`
}

// ComparisonOperator is how an AnalysisMetric result is compared to its
// Threshold. "result <op> threshold" being true means the metric is healthy.
type ComparisonOperator string

const (
	// ComparisonLT passes when result < threshold.
	ComparisonLT ComparisonOperator = "LT"
	// ComparisonLTE passes when result <= threshold (e.g. error-rate <= 0.05).
	ComparisonLTE ComparisonOperator = "LTE"
	// ComparisonGT passes when result > threshold.
	ComparisonGT ComparisonOperator = "GT"
	// ComparisonGTE passes when result >= threshold (e.g. success-rate >= 0.99).
	ComparisonGTE ComparisonOperator = "GTE"
)

// OnInconclusive selects the behavior when analysis cannot read health past the
// stall timeout (NOT a metric breach).
type OnInconclusive string

const (
	// OnInconclusiveHold keeps traffic at the current step and escalates to Failed
	// for an operator decision. The conservative default: do not revert on a
	// monitoring outage.
	OnInconclusiveHold OnInconclusive = "Hold"
	// OnInconclusiveRollback treats "can't tell" as "assume bad" and rolls back
	// on the first inconclusive sample.
	OnInconclusiveRollback OnInconclusive = "Rollback"
	// OnInconclusiveRollbackOnStall keeps sampling through transient gaps as Hold
	// does, then rolls back rather than parking once analysis has been unable to
	// read health for the stall timeout. A scrape blip does not revert a healthy
	// canary, and a gate that never recovers does not leave the fleet split.
	OnInconclusiveRollbackOnStall OnInconclusive = "RollbackOnStall"
)
