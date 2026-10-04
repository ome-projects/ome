// Package v1beta1 — traffic-management status types.
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TrafficStatus reflects the resolved backend traffic policy and the
// emitted policy resource (e.g. Envoy Gateway BackendTrafficPolicy)
// for an InferenceService. Lives at InferenceServiceStatus.Traffic.
// Populated only when traffic intent is declared via spec.traffic or
// any ome.io/* traffic annotation; otherwise nil so older clients see
// nothing.
type TrafficStatus struct {
	// Algorithm reflects the resolved load-balancing algorithm. One
	// of RoundRobin | LeastRequest | Random | ConsistentHash | Default.
	// "Default" means no Algorithm was set on spec.traffic and the
	// Gateway implementation default applies.
	// +optional
	Algorithm string `json:"algorithm,omitempty"`

	// BackendPolicyResource references the OME-emitted backend policy
	// (typically a gateway.envoyproxy.io/v1alpha1.BackendTrafficPolicy).
	// Empty when no policy was emitted (e.g. translator deferred to a
	// conflicting hand-authored policy).
	// +optional
	BackendPolicyResource *BackendPolicyRef `json:"backendPolicyResource,omitempty"`

	// TargetedHTTPRoutes lists the HTTPRoute names the emitted policy
	// targets. Operators read this to understand which routes the
	// policy applies to without inspecting the emitted resource.
	// Empty when no policy was emitted.
	// +optional
	// +listType=atomic
	TargetedHTTPRoutes []string `json:"targetedHTTPRoutes,omitempty"`

	// Conditions surface translation, conflict, and gateway-acceptance
	// state. The well-known condition type is BackendPolicyReady.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// BackendPolicyRef identifies the OME-emitted backend policy resource
// sibling to the InferenceService.
type BackendPolicyRef struct {
	// APIVersion of the resource (e.g. "gateway.envoyproxy.io/v1alpha1").
	APIVersion string `json:"apiVersion"`
	// Kind of the resource (e.g. "BackendTrafficPolicy").
	Kind string `json:"kind"`
	// Name of the resource, in the same namespace as the
	// InferenceService.
	Name string `json:"name"`
}

// Well-known TrafficStatus condition types.
const (
	// TrafficConditionBackendPolicyReady is True when the emitted
	// backend policy resource has been acknowledged by the gateway
	// controller; False with a Reason explaining the situation
	// otherwise; Unknown while emission is in flight.
	TrafficConditionBackendPolicyReady = "BackendPolicyReady"

	// TrafficConditionBackendPolicyUnsupportedFields is added (with
	// Status=True, Reason=UnsupportedField) when the operator
	// declared typed spec.traffic fields or ome.io/* traffic
	// annotations the active translator does not honor — for example,
	// an endpointOverride or an ome.io/dr.* (Istio pass-through)
	// annotation on an Envoy-Gateway cluster. The message names the
	// active translator and lists every dropped field and key so
	// operators see exactly what was ignored without inspecting the
	// policy resource.
	//
	// The condition is omitted entirely when nothing was dropped
	// (positive-polarity convention: absence = nothing to worry
	// about). It is also omitted on noop / TranslationFailed
	// branches, where BackendPolicyReady already explains the
	// situation and listing dropped keys is redundant noise.
	TrafficConditionBackendPolicyUnsupportedFields = "BackendPolicyUnsupportedFields"
)

// Well-known TrafficConditionBackendPolicyReady reasons.
const (
	TrafficReasonAcceptedByGateway     = "AcceptedByGateway"
	TrafficReasonConflictingPolicy     = "ConflictingPolicy"
	TrafficReasonUnsupportedField      = "UnsupportedField"
	TrafficReasonNoTranslatorAvailable = "NoTranslatorAvailable"
	TrafficReasonGatewayRejected       = "GatewayRejected"
	TrafficReasonPending               = "Pending"
	// TrafficReasonTranslationFailed means the active translator
	// errored while turning the resolved intent into a backend policy
	// resource (e.g. an intent shape the implementation can't honor).
	// The condition message carries the translator's error string.
	TrafficReasonTranslationFailed = "TranslationFailed"
)

// RolloutPhase is the canary step machine's projected state for a Component
// that a spec.rollout canary group governs. The canary executor writes it on
// the Component it drives (the unit's entrypoint) and nothing clears it, so
// it persists between runs. Lives on ComponentStatusSpec.RolloutPhase.
// Components that blueGreen/rollingUpdate groups drive carry no phase; their
// state is under status.rolloutCoordination.
type RolloutPhase string

const (
	// RolloutPhaseStable indicates one revision owns the Component's traffic
	// and no canary is in flight: the Component before its first canary and
	// after a completed one (status.canary then records the finished run).
	RolloutPhaseStable RolloutPhase = "Stable"
	// RolloutPhaseCanarying indicates two revisions are live: the step's
	// canary capacity is Ready, its traffic weight (0-99%) is programmed and
	// no gate holds the step. A step at 0% traffic is the capacity-ahead
	// warm-up, where the canary is validated in-cluster before it takes
	// traffic.
	RolloutPhaseCanarying RolloutPhase = "Canarying"
	// RolloutPhasePending indicates a new revision is being
	// materialized; canary pods are not yet Ready. Transient.
	RolloutPhasePending RolloutPhase = "Pending"
	// RolloutPhasePaused indicates a step's split is programmed and its gate
	// holds: waiting for the ome.io/rollout-promote annotation (manual
	// promotion, or a pre-step hold after a repin), for the step's
	// Pause.Duration to elapse, or for its analysis window to pass.
	RolloutPhasePaused RolloutPhase = "Paused"
	// RolloutPhasePromoting indicates the final step has shifted 100% of the
	// traffic to the canary and the stable revision is draining. Transient.
	RolloutPhasePromoting RolloutPhase = "Promoting"
	// RolloutPhaseRollingBack indicates the canary is scaling down
	// and the stable revision is scaling back to full. Transient.
	RolloutPhaseRollingBack RolloutPhase = "RollingBack"
	// RolloutPhaseRolledBack indicates a rollback completed: the
	// component is fully back on the stable revision and is HELD there,
	// rejecting the rolled-back revision. The rollout re-arms only when a
	// new (different) target revision appears. Terminal until then.
	RolloutPhaseRolledBack RolloutPhase = "RolledBack"
	// RolloutPhaseFailed indicates the canary is parked at its current step
	// with the stable revision still serving: the capacity gate stayed unmet
	// past the ready timeout, analysis stayed inconclusive past the stall
	// timeout, or a rollback found no stable revision to return to
	// (status.canary.failed names the reason). The canary is preserved for
	// diagnosis; the operator must roll back or resume explicitly.
	RolloutPhaseFailed RolloutPhase = "Failed"
)
