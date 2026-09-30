// Package v1beta1 — RolloutPolicy types.
//
// A RolloutPolicy is a reusable rollout progression: exactly one canary /
// blueGreen / rollingUpdate body that InferenceService rollout groups consume
// by reference (RolloutGroup.PolicyRef). The policy carries behavior only —
// never components, order, soak, maintainRatio, or pairingProtocol, which are
// consumer shape and state. That restriction is what lets one policy serve
// every component topology in a fleet.
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RolloutProgressionKind names which progression a RolloutPolicy carries. A
// RolloutPolicyRef declares it so ISVC admission can validate every
// shape-dependent rollout rule (entrypoint, collapse, soak arity) without
// dereferencing the policy; a declaration the policy body contradicts parks
// the rollout at run open instead of mis-executing.
// +kubebuilder:validation:Enum=canary;blueGreen;rollingUpdate
type RolloutProgressionKind string

const (
	RolloutProgressionCanary        RolloutProgressionKind = "canary"
	RolloutProgressionBlueGreen     RolloutProgressionKind = "blueGreen"
	RolloutProgressionRollingUpdate RolloutProgressionKind = "rollingUpdate"
)

// RolloutPolicySpec carries exactly one progression body, reusing the
// spec.rollout group progression types verbatim so a composed plan is a
// literal RolloutGroup fragment and run-open re-validation can run the same
// checks ISVC admission runs. Policy-only restrictions (percent-only
// capacities, providerRef-only metrics source) are admission rules on this
// kind, not type differences.
//
// +kubebuilder:validation:XValidation:rule="(has(self.canary)?1:0) + (has(self.blueGreen)?1:0) + (has(self.rollingUpdate)?1:0) == 1",message="exactly one of canary, blueGreen, or rollingUpdate must be set"
type RolloutPolicySpec struct {
	// Canary is a stepped capacity+traffic progression with optional analysis
	// gates. One-of.
	// +optional
	Canary *GroupCanary `json:"canary,omitempty"`

	// BlueGreen surges the full new set, then flips traffic atomically. One-of.
	// +optional
	BlueGreen *GroupBlueGreen `json:"blueGreen,omitempty"`

	// RollingUpdate replaces pods gradually within the surge/unavailable
	// budget. One-of.
	// +optional
	RollingUpdate *GroupRollingUpdate `json:"rollingUpdate,omitempty"`
}

// Condition types and reasons reported on RolloutPolicy.status.conditions.
const (
	// RolloutPolicyReadyCondition is True when the progression body passes the
	// same plan validation an inline block faces (plus the policy-only
	// restrictions), so a Ready policy composes into an admissible group for
	// any conforming consumer.
	RolloutPolicyReadyCondition = "Ready"
	// RolloutPolicyInUseCondition is True while at least one rollout group
	// references this policy — the same signal the DELETE-denial webhook uses.
	RolloutPolicyInUseCondition = "InUse"

	RolloutPolicyReasonBodyValid       = "BodyValid"
	RolloutPolicyReasonBodyInvalid     = "BodyInvalid"
	RolloutPolicyReasonProviderUnbound = "ProviderUnbound"
	RolloutPolicyReasonPlanTooLarge    = "PlanTooLarge"
	RolloutPolicyReasonAttached        = "Attached"
	RolloutPolicyReasonNoConsumers     = "NoConsumers"
)

// RolloutPolicyStatus is deliberately bounded: counts and digests, never
// consumer name lists (a popular policy would otherwise grow status with its
// fleet).
type RolloutPolicyStatus struct {
	// ObservedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// PortableDigest is the hash of the defaulted, canonicalized spec
	// ("rp1:..."): equal across clusters iff the specs match, so fleet drift
	// is one field-compare away.
	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`

	// AttachedGroups counts rollout groups currently referencing this policy
	// across the namespace.
	// +optional
	AttachedGroups int32 `json:"attachedGroups,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// RolloutPolicy is a namespaced, reusable rollout progression consumed by
// InferenceService rollout groups via RolloutGroup.PolicyRef.
// Alpha. The API may change without notice.
//
// RolloutPolicy is off by default. To turn it on, set
// `ome.rolloutPolicy.enabled` to true on both the ome-crd and ome-resources
// Helm charts, which install the CRD and its validating webhook. The manager
// looks for the CRD only when it starts.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=rolloutpolicies,singular=rolloutpolicy
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Digest",type="string",JSONPath=".status.portableDigest"
// +kubebuilder:printcolumn:name="Refs",type="integer",JSONPath=".status.attachedGroups"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +ome:since=v1.3
type RolloutPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RolloutPolicySpec   `json:"spec,omitempty"`
	Status RolloutPolicyStatus `json:"status,omitempty"`
}

// RolloutPolicyList contains a list of RolloutPolicy.
//
// +kubebuilder:object:root=true
type RolloutPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RolloutPolicy `json:"items"`
}

// RolloutPolicyRef names a same-namespace RolloutPolicy that supplies a
// rollout group's progression. Deliberately a sibling of the inline
// progression one-of, not an arm of it: a ref and one inline progression may
// coexist, and the inline block wins — the preview/rollback mechanism.
type RolloutPolicyRef struct {
	// Name of the RolloutPolicy in the InferenceService's namespace.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Progression declares the referenced policy's kind so shape-dependent
	// admission rules evaluate without dereferencing the policy. A mismatch
	// with the policy's actual body parks the rollout at run open
	// (ProgressionMismatch); it cannot mis-execute.
	Progression RolloutProgressionKind `json:"progression"`

	// Kind is reserved: v1 admission accepts only "RolloutPolicy";
	// "ClusterRolloutPolicy" is the reserved cluster-scoped twin.
	// +kubebuilder:default=RolloutPolicy
	// +optional
	Kind string `json:"kind,omitempty"`
}

func init() {
	SchemeBuilder.Register(&RolloutPolicy{}, &RolloutPolicyList{})
}

// Progression returns the single progression body set on the spec, with its
// declared kind. Returns ("" , nil-equivalent zero) when none is set (an
// invalid object admission would have rejected).
func (s *RolloutPolicySpec) Progression() (RolloutProgressionKind, bool) {
	switch {
	case s.Canary != nil:
		return RolloutProgressionCanary, true
	case s.BlueGreen != nil:
		return RolloutProgressionBlueGreen, true
	case s.RollingUpdate != nil:
		return RolloutProgressionRollingUpdate, true
	}
	return "", false
}
