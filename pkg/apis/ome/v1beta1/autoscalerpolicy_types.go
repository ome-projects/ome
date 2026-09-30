package v1beta1

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PolicyEnforcement is the enforcement tier of an AutoscalerPolicy.
// Alpha. The API may change without notice.
//
// +kubebuilder:validation:Enum=Default;Required
type PolicyEnforcement string

const (
	// PolicyEnforcementDefault means consumers opt in per component and an
	// inline autoscaler block on the consumer always outranks the policy.
	PolicyEnforcementDefault PolicyEnforcement = "Default"
	// PolicyEnforcementRequired is a reserved shape: admission rejects it, so
	// an old controller can never misread a Required policy as Default.
	PolicyEnforcementRequired PolicyEnforcement = "Required"
)

// ComponentBoundsField names a replica bound on the consuming component's
// ComponentExtensionSpec that a ReplicaValueSource resolves against.
//
// +kubebuilder:validation:Enum=MaxReplicas;MinReplicas
type ComponentBoundsField string

const (
	// BoundsFieldMaxReplicas resolves to the component's effective maxReplicas.
	BoundsFieldMaxReplicas ComponentBoundsField = "MaxReplicas"
	// BoundsFieldMinReplicas resolves to the component's effective minReplicas.
	BoundsFieldMinReplicas ComponentBoundsField = "MinReplicas"
)

// ReplicaValueSource derives an int32 replica count for a rendered scaler
// field. Exactly one of Value / FromComponent is set. kedav1.Fallback.Replicas
// is an int32; a typed source avoids round-tripping numbers through string
// templates, which would add a parse failure mode for no expressiveness.
//
// +kubebuilder:validation:XValidation:rule="(has(self.value) ? 1 : 0) + (has(self.fromComponent) ? 1 : 0) == 1",message="exactly one of value or fromComponent must be set"
type ReplicaValueSource struct {
	// Value is a fixed replica count.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Value *int32 `json:"value,omitempty"`

	// FromComponent resolves against the consuming component's effective
	// replica bounds — after defaulting and, on a placement-derived
	// InferenceService, after the per-home bounds rewrite.
	// +optional
	FromComponent *ComponentBoundsField `json:"fromComponent,omitempty"`
}

// FallbackTemplate renders a kedav1.Fallback with the replica count derived
// per consuming component instead of authored as a literal.
type FallbackTemplate struct {
	// FailureThreshold is the number of consecutive failed metric evaluations
	// after which KEDA serves the fallback (it trips on the threshold+1-th
	// consecutive failure).
	// +kubebuilder:validation:Minimum=1
	FailureThreshold int32 `json:"failureThreshold"`

	// Replicas is the fallback replica count.
	Replicas ReplicaValueSource `json:"replicas"`
}

// MetricProviderRef names a logical metric provider. The name is bound to a
// cluster-local endpoint (and optional credentials) by the operator's
// autoscalerPolicy configuration block; endpoints are not representable in
// the policy itself, so one policy object serves every cluster and a policy
// author can never point a scaler at an arbitrary endpoint.
type MetricProviderRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// KedaTriggerTemplate is a parameterized KEDA trigger. Metadata string values
// may contain {{ .Var }} templates over a closed, controller-derived variable
// set (Namespace, ISVCName, Component, MinReplicas, MaxReplicas, TargetName).
type KedaTriggerTemplate struct {
	// Type is the KEDA trigger type ("prometheus" in v1). Admission requires
	// ProviderRef for network-endpoint trigger types.
	// +kubebuilder:validation:MinLength=1
	Type string `json:"type"`

	// ProviderRef binds the trigger's endpoint by logical provider name.
	// +optional
	ProviderRef *MetricProviderRef `json:"providerRef,omitempty"`

	// MetricType is forwarded to the rendered trigger.
	// +kubebuilder:validation:Enum=AverageValue;Value
	// +optional
	MetricType autoscalingv2.MetricTargetType `json:"metricType,omitempty"`

	// QueryReturnsDesiredReplicas declares that the trigger's query already
	// computes the desired replica count. When true, admission requires
	// MetricType=AverageValue: the HPA's Value math is
	// ceil((metric/threshold) x readyPods), which breaks desired-count queries.
	// +optional
	QueryReturnsDesiredReplicas bool `json:"queryReturnsDesiredReplicas,omitempty"`

	// Metadata is the KEDA trigger metadata. Admission rejects the keys
	// "serverAddress" and "authModes" (the provider binding owns them), and
	// requires "ignoreNullValues" to be explicit on prometheus triggers.
	Metadata map[string]string `json:"metadata"`
}

// KedaPolicyTemplate is the parameterized counterpart of KedaAutoscaler.
// Everything except trigger metadata templates and the typed fallback
// replicas is passed through verbatim to the rendered block.
type KedaPolicyTemplate struct {
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Triggers []KedaTriggerTemplate `json:"triggers"`

	// Advanced ScaledObject configuration, verbatim.
	// +optional
	Advanced *kedav1.AdvancedConfig `json:"advanced,omitempty"`

	// +optional
	PollingInterval *int32 `json:"pollingInterval,omitempty"`

	// +optional
	CooldownPeriod *int32 `json:"cooldownPeriod,omitempty"`

	// +optional
	IdleReplicaCount *int32 `json:"idleReplicaCount,omitempty"`

	// +optional
	Fallback *FallbackTemplate `json:"fallback,omitempty"`
}

// AutoscalerPolicySpec carries exactly one parameterized autoscaler template.
// A policy that needs different behavior for different components is two
// policies; components compose by each choosing a ref.
//
// +kubebuilder:validation:XValidation:rule="self.class == 'KEDA' ? (has(self.keda) && !has(self.hpa)) : !has(self.keda)",message="class KEDA requires keda and forbids hpa; class HPA forbids keda"
type AutoscalerPolicySpec struct {
	// Enforcement tier. v1 admits only "Default"; "Required" is a reserved
	// shape rejected at admission.
	// +kubebuilder:validation:Enum=Default;Required
	// +kubebuilder:default=Default
	// +optional
	Enforcement PolicyEnforcement `json:"enforcement,omitempty"`

	// Class selects the template. External and None are inline-only:
	// "someone else scales this component" is a per-service statement, not a
	// reusable behavior.
	// +kubebuilder:validation:Enum=HPA;KEDA
	Class AutoscalerClass `json:"class"`

	// Keda holds the parameterized KEDA template. Required when Class=KEDA.
	// +optional
	Keda *KedaPolicyTemplate `json:"keda,omitempty"`

	// HPA holds HorizontalPodAutoscaler configuration, verbatim (no
	// templating). Optional when Class=HPA; if absent the rendered block
	// gets the default CPU=80% metric downstream, exactly like an inline
	// block with a nil HPA field.
	// +optional
	HPA *HPAAutoscaler `json:"hpa,omitempty"`
}

// Condition types and reasons reported on AutoscalerPolicy.status.conditions.
const (
	// AutoscalerPolicyReadyCondition is True when every template parses,
	// passes the structural allowlist, sample-renders, and names a resolvable
	// provider shape.
	AutoscalerPolicyReadyCondition = "Ready"
	// AutoscalerPolicyInUseCondition is True while at least one component
	// references the policy.
	AutoscalerPolicyInUseCondition = "InUse"

	AutoscalerPolicyReasonTemplatesValid       = "TemplatesValid"
	AutoscalerPolicyReasonParseError           = "ParseError"
	AutoscalerPolicyReasonForbiddenNode        = "ForbiddenTemplateNode"
	AutoscalerPolicyReasonForbiddenMetadataKey = "ForbiddenMetadataKey"
	AutoscalerPolicyReasonProviderUnknown      = "ProviderUnknown"
	AutoscalerPolicyReasonPromQLInvalid        = "PromQLInvalid"
	AutoscalerPolicyReasonAttached             = "Attached"
	AutoscalerPolicyReasonNoConsumers          = "NoConsumers"
)

// AutoscalerResolvedCondition reports, per component on the consuming
// InferenceService, whether the autoscaler resolution chain produced a live
// block. Written only for components that carry a policy ref. False is the
// fail-closed state: the last-known-good scaler stands, and the reconcile
// keeps succeeding (degraded, not an error).
const (
	AutoscalerResolvedCondition = "AutoscalerResolved"

	AutoscalerResolvedReasonRenderedFromPolicy = "RenderedFromPolicy"
	// InlinePrecedence: the ref is shadowed by an inline block — resolution
	// succeeded deterministically; the shadow detail lives in the
	// shadowedPolicyRef status field.
	AutoscalerResolvedReasonInlinePrecedence = "InlinePrecedence"
	AutoscalerResolvedReasonPolicyNotFound   = "PolicyNotFound"
	AutoscalerResolvedReasonPolicyInvalid    = "PolicyInvalid"
	AutoscalerResolvedReasonAuthNotFound     = "AuthNotFound"
	AutoscalerResolvedReasonClassUnavailable = "ClassUnavailable"
	AutoscalerResolvedReasonUnsupportedMode  = "UnsupportedDeploymentMode"
)

// Source-ISVC conditions for the multi-cluster path.
const (
	// PlacementPolicyPreflightCondition is written by the placement
	// controller before fan-out; False blocks placement.
	PlacementPolicyPreflightCondition = "PlacementPolicyPreflight"

	PlacementPolicyPreflightReasonPassed                = "Passed"
	PlacementPolicyPreflightReasonUnboundedSplitCeiling = "UnboundedSplitCeiling"
	PlacementPolicyPreflightReasonDigestMismatch        = "DigestMismatch"
	PlacementPolicyPreflightReasonPolicyMissing         = "PolicyMissingOnMember"
	PlacementPolicyPreflightReasonMemberGetTimeout      = "MemberGetTimeout"
	PlacementPolicyPreflightReasonCapabilityMissing     = "CapabilityMissing"
	// InvalidRef: the source carries an autoscalerPolicyRef shape every
	// member admission webhook would deny (e.g. a reserved ref kind), so
	// fan-out could never converge.
	PlacementPolicyPreflightReasonInvalidRef = "InvalidRef"
	// InvalidPolicy: the control-plane anchor policy fails spec validation
	// (reserved enforcement, template or query errors), so every member
	// would reject the rendered scaler.
	PlacementPolicyPreflightReasonInvalidPolicy = "InvalidPolicy"

	// AutoscalerPolicyAggregateCondition ("AutoscalerPolicyReady") is the
	// watch-funnel's aggregate over homes: False when any home fails to
	// report a resolved digest within the skew deadline, reverts a pruned
	// ref, or fails closed.
	AutoscalerPolicyAggregateCondition = "AutoscalerPolicyReady"

	AutoscalerPolicyAggregateReasonAllHomesResolved  = "AllHomesResolved"
	AutoscalerPolicyAggregateReasonResolveTimeout    = "ResolveTimeout"
	AutoscalerPolicyAggregateReasonFieldPruned       = "FieldPruned"
	AutoscalerPolicyAggregateReasonMemberFailedClose = "MemberFailedClosed"
)

// AutoscalerPolicyStatus is bounded: counts and digests, never consumer name
// lists — per-consumer truth lives on each InferenceService's own status.
type AutoscalerPolicyStatus struct {
	// ObservedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// PortableDigest is a digest of the spec after API defaulting, in a
	// canonical encoding. Equal across clusters iff the specs are
	// semantically identical; the multi-cluster preflight compares it
	// against the control plane's copy.
	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`

	// AttachedComponents counts the components in this namespace that
	// currently reference the policy.
	// +optional
	AttachedComponents int32 `json:"attachedComponents,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// AutoscalerPolicy is a reusable, parameterized autoscaler template.
// Components attach individually via spec.<component>.autoscalerPolicyRef on
// the InferenceService; the referenced template is rendered per component at
// reconcile time into a verbatim ComponentAutoscaler and fed to the existing
// autoscaler dispatch. Creating a policy actuates nothing by itself: the
// per-component ref is the only attachment mechanism.
// Alpha. The API may change without notice.
//
// AutoscalerPolicy is off by default. To turn it on, set
// `ome.autoscalerPolicy.enabled` to true on both the ome-crd and
// ome-resources Helm charts, which install the CRD and its validating
// webhook. The manager looks for the CRD only when it starts.
//
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=autoscalerpolicies,singular=autoscalerpolicy
// +kubebuilder:printcolumn:name="Class",type="string",JSONPath=".spec.class"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Attached",type="integer",JSONPath=".status.attachedComponents"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +ome:since=v1.3
type AutoscalerPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AutoscalerPolicySpec   `json:"spec,omitempty"`
	Status AutoscalerPolicyStatus `json:"status,omitempty"`
}

// AutoscalerPolicyList contains a list of AutoscalerPolicy.
//
// +kubebuilder:object:root=true
type AutoscalerPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AutoscalerPolicy `json:"items"`
}

// AutoscalerPolicyRef names a same-namespace AutoscalerPolicy that renders
// this component's autoscaler. Deliberately a sibling of the inline
// Autoscaler block, not a field inside it: ref and inline block may coexist —
// the inline block wins — and that coexistence is the preview/rollback
// mechanism.
type AutoscalerPolicyRef struct {
	// Name of an AutoscalerPolicy in the InferenceService's namespace.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Kind of the referenced policy. "ClusterAutoscalerPolicy" is a reserved
	// shape: admission rejects it until the cluster-scoped twin ships.
	// +kubebuilder:validation:Enum=AutoscalerPolicy;ClusterAutoscalerPolicy
	// +kubebuilder:default=AutoscalerPolicy
	// +optional
	Kind string `json:"kind,omitempty"`
}

// AutoscalerPolicyProvenance reports, on the consuming InferenceService,
// which policy produced a component's live autoscaler.
type AutoscalerPolicyProvenance struct {
	// Name of the AutoscalerPolicy in the InferenceService's namespace.
	Name string `json:"name"`

	// ObservedGeneration is the policy generation the rendered block came from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// PortableDigest is the policy's canonical spec digest — equal across
	// clusters iff the policy specs match.
	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`

	// ResolvedDigest is a digest of the rendered ComponentAutoscaler plus the
	// effective bounds and bound provider endpoint. It differs per cluster by
	// design; its job is per-home provenance ("did my policy edit land here?").
	// +optional
	ResolvedDigest string `json:"resolvedDigest,omitempty"`
}

// ShadowedAutoscalerPolicy reports a policy ref that is currently shadowed by
// an inline autoscaler block (inline outranks policy). The shadow render is
// the in-cluster preview surface: it shows what the policy would produce
// without changing behavior.
type ShadowedAutoscalerPolicy struct {
	// Name of the shadowed AutoscalerPolicy.
	Name string `json:"name"`

	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`

	// WouldRenderDigest is the resolved digest the policy would produce for
	// this component if the inline block were removed.
	// +optional
	WouldRenderDigest string `json:"wouldRenderDigest,omitempty"`
}

func init() {
	SchemeBuilder.Register(&AutoscalerPolicy{}, &AutoscalerPolicyList{})
}
