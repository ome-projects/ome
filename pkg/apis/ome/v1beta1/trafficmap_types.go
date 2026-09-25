package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
)

// TrafficMapPublished is the condition type reporting whether the active
// publisher has realized this TrafficMap onto a concrete data plane (e.g. a
// Gateway API HTTPRoute).
const TrafficMapPublished = "Published"

// TrafficMapRoutable is the condition type reporting whether this map carries a
// target a gateway can send traffic to. It is owned by the routing controller,
// which writes it on every pass so an empty table always explains itself: the
// map exists for the whole life of a routed InferenceService, and emptiness is a
// state of the map rather than its absence.
const TrafficMapRoutable = "Routable"

// TrafficMapOverrideActive reports whether source traffic-drain annotation IDs
// currently override one or more route-arm weights.
const TrafficMapOverrideActive = "OverrideActive"

// Reasons for the OverrideActive condition.
const (
	TrafficMapReasonOverridesApplied = "OverridesApplied"
	TrafficMapReasonOverridesPending = "OverridesPending"
	TrafficMapReasonNoOverrides      = "NoOverrides"
)

// TrafficMapCapacityFallback is the condition type reporting whether one or
// more homes are using the control-plane allocation because configured
// endpoint capacity could not produce a usable ceiling. It is owned by the
// routing controller.
const TrafficMapCapacityFallback = "CapacityFallback"

const (
	// MaxTrafficMapPublisherNameLength bounds a registered publisher identifier
	// without constraining its implementation-specific naming scheme.
	MaxTrafficMapPublisherNameLength = 128

	// MaxTrafficMapPublisherTargets bounds the durable publisher claim set.
	MaxTrafficMapPublisherTargets = 128

	// MaxTrafficMapPublisherTargetLength bounds one opaque canonical target.
	MaxTrafficMapPublisherTargetLength = 512
)

// Reasons for the Routable condition. NotPlaced and NoAddressableHome accompany
// an empty table. AllHomesUnready and NoRoutableCapacity accompany an all-zero
// table. AllHomesProbeFailed accompanies either the all-zero Drain policy or a
// positive PreserveTraffic fallback among homes with ready capacity.
// TrafficDrain identifies an all-zero result caused by manual overrides.
const (
	TrafficMapReasonRoutable            = "Routable"
	TrafficMapReasonNotPlaced           = "NotPlaced"
	TrafficMapReasonNoAddressableHome   = "NoAddressableHome"
	TrafficMapReasonAllHomesUnready     = "AllHomesUnready"
	TrafficMapReasonNoRoutableCapacity  = "NoRoutableCapacity"
	TrafficMapReasonAllHomesProbeFailed = "AllHomesProbeFailed"
	TrafficMapReasonTrafficDrain        = "TrafficDrain"
)

// Reasons for the CapacityFallback condition. The condition has abnormal-true
// polarity: True means at least one home has fallen open to the control-plane
// allocation; False means fallback is inactive, including when polling is
// disabled or there are no homes to observe.
const (
	TrafficMapReasonCapacityPollingDisabled   = "CapacityPollingDisabled"
	TrafficMapReasonNoCapacityTargets         = "NoCapacityTargets"
	TrafficMapReasonEndpointCapacityAvailable = "EndpointCapacityAvailable"
	TrafficMapReasonEndpointCapacityFallback  = "EndpointCapacityUnavailable"
)

// CapacitySource names which input produced an entry's Allocated count.
// +kubebuilder:validation:Enum=ControlPlane;Endpoint
type CapacitySource string

const (
	// CapacitySourceControlPlane means Allocated came from the control plane's
	// own view — status.placement intersected with quota. The default.
	CapacitySourceControlPlane CapacitySource = "ControlPlane"

	// CapacitySourceEndpoint means the home reported a lower servable capacity
	// than the plan and that report became Allocated. A reported value is only
	// ever a ceiling, so this source implies Allocated is below the plan.
	CapacitySourceEndpoint CapacitySource = "Endpoint"
)

// ProbeResult is the outcome of the most recent end-to-end health probe.
// +kubebuilder:validation:Enum=Passing;Failing;Unknown
type ProbeResult string

const (
	// ProbeResultPassing means the last probe reached the endpoint and got an
	// accepted status.
	ProbeResultPassing ProbeResult = "Passing"

	// ProbeResultFailing means the last probe reached a verdict that counts
	// against the home: a gating status code, a transport error, or a timeout.
	// It gates the weight only once the consecutive-failure threshold is met.
	ProbeResultFailing ProbeResult = "Failing"

	// ProbeResultUnknown means no verdict was reached — the probe has not run
	// yet, or it could not run at all (missing credentials, blocked egress, a
	// misconfigured prober). Unknown never gates: that condition is uniform
	// across homes, so treating it as failure would zero the whole fleet at once.
	ProbeResultUnknown ProbeResult = "Unknown"
)

// TrafficMap is the capacity-aware routing projection of one multi-cluster
// InferenceService: the per-home traffic split a gateway consumes. Placement
// decides WHERE an ISVC's replicas run and quota decides HOW MUCH each home may
// run; TrafficMap decides the WEIGHTS. It is generated by a control-plane
// routing controller from status.placement plus accelerator-quota allocation,
// never hand-edited, and consumed by a gateway-neutral publisher.
//
// One TrafficMap exists per routed ISVC, named after it and owned by it (so it
// is garbage-collected with the ISVC), for as long as that ISVC is routed --
// including while no home is routable, when the table is empty or all-zero and
// the Routable condition says why. The routing controller owns spec, SourceUID,
// and the Routable, CapacityFallback, and OverrideActive conditions; the publisher owns the
// rest of status.
// +genclient
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tm;tmap
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Published",type=string,JSONPath=`.status.conditions[?(@.type=="Published")].status`
// +kubebuilder:printcolumn:name="Routable",type=string,JSONPath=`.status.conditions[?(@.type=="Routable")].status`
// +kubebuilder:printcolumn:name="Override",type=string,JSONPath=`.status.conditions[?(@.type=="OverrideActive")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Routable")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type TrafficMap struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TrafficMapSpec   `json:"spec,omitempty"`
	Status TrafficMapStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type TrafficMapList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TrafficMap `json:"items"`
}

// TrafficMapSpec is the computed routing table. It is a machine-written
// declarative artifact — the routing controller recomputes it whenever
// status.placement or the home's quota allocation changes; the publisher
// consumes it as desired state. A validating webhook may reject writes from
// non-controller users.
type TrafficMapSpec struct {
	// Service is the routed logical service — the InferenceService name. Equal to
	// metadata.name in v1; a distinct field leaves room for a future
	// many-ISVC-to-one-service map.
	// +required
	Service string `json:"service"`

	// Mode mirrors the ISVC's spec.placement.mode (Single/All/Split) so a
	// consumer can read the routing intent without reading the ISVC.
	// +optional
	Mode PlacementMode `json:"mode,omitempty"`

	// Entries is the routing table: one entry per serving home, each with its
	// endpoint, final apply-verbatim weight, and the capacity/health provenance
	// of that weight. Consumers apply the weights directly — no gating, fallback,
	// or normalization is left for them to redo.
	// +optional
	// +listType=map
	// +listMapKey=cluster
	Entries []TrafficMapEntry `json:"entries,omitempty"`

	// ObservedISVCGeneration is the InferenceService generation this table was
	// computed from, so a consumer can detect staleness relative to the source.
	// +optional
	ObservedISVCGeneration int64 `json:"observedISVCGeneration,omitempty"`
}

// TrafficMapEntry is one serving home's routing target and the provenance of its
// weight.
type TrafficMapEntry struct {
	// Cluster is the WorkloadCluster serving this home.
	// +required
	Cluster string `json:"cluster"`

	// Endpoint is the home's externally-addressable URL, taken from
	// status.placement.candidates[].endpoint.
	// +optional
	Endpoint *apis.URL `json:"endpoint,omitempty"`

	// Weight is the home's final, normalized, capacity-planned, health-gated
	// traffic share as a relative integer from 0 through 1,000,000. The upper
	// bound matches the Gateway API backendRef weight limit, so a publisher can
	// apply it verbatim; another consumer may divide by the sum of weights for a
	// percentage. Unready and zero-capacity homes always have weight 0. When every
	// probe conclusively fails, PreserveTraffic may retain capacity-derived
	// weights for ready homes; Drain writes an authoritative all-zero table.
	// Manual DrainRefs can force any arm to zero after these automatic inputs.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000000
	Weight int32 `json:"weight"`

	// Healthy is the readiness-plus-probe result for this home. An unready home is
	// always weighted 0; a probe-unhealthy home may retain a positive weight only
	// under a fleet-wide PreserveTraffic fallback. A manual DrainRefs override
	// can set a healthy home to zero without changing its health evidence.
	// +optional
	Healthy bool `json:"healthy"`

	// Capacity is the provenance of Weight: weight is proportional to
	// min(allocated, ready) * factor, then gated by the active probe.
	// +optional
	Capacity *TrafficMapCapacity `json:"capacity,omitempty"`

	// Probe is the active end-to-end health probe's contribution to Healthy,
	// present only when probing is configured. Healthy alone does not say
	// whether readiness or reachability failed, and those are different faults
	// with different owners: readiness is the workload's, reachability is the
	// path's (ingress, DNS, certificate, route).
	// +optional
	Probe *TrafficMapProbe `json:"probe,omitempty"`

	// DrainRefs names the independent override IDs in the source
	// InferenceService's ome.io/traffic-drain annotation that force this
	// entry's weight to zero. Empty means no manual override contributed.
	// +optional
	// +listType=set
	DrainRefs []string `json:"drainRefs,omitempty"`
}

// TrafficMapProbe records what the active end-to-end probe observed for one
// home. The probe targets the entry's Endpoint — the URL a client uses — so it
// covers the whole serving path rather than pod readiness inside the home.
type TrafficMapProbe struct {
	// PolicyDigest identifies the effective probe policy that produced this
	// observation. A controller may restore hysteresis state after a restart only
	// when this digest and the entry identity still match.
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	PolicyDigest string `json:"policyDigest,omitempty"`

	// Result is the most recent probe attempt's verdict. Failing gates only after
	// the configured threshold. Unknown does not change the previous gate.
	// +optional
	Result ProbeResult `json:"result,omitempty"`

	// Gated is the current hysteresis gate. It remains true through an Unknown
	// attempt until enough consecutive passing attempts reopen the home.
	// +optional
	Gated bool `json:"gated,omitempty"`

	// LastProbeTime is when the latest probe attempt completed, so a stalled
	// prober is visible rather than being read as a steady pass.
	// +optional
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`

	// ConsecutiveFailures counts unbroken Failing verdicts. The weight is gated
	// only once this reaches the configured failure threshold: a single dropped
	// packet must not move a large traffic share, and the reprogramming churn
	// from flapping would itself be the outage.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`

	// Message explains the current Result — the observed status code, the
	// transport error, or why the probe could not run.
	// +optional
	Message string `json:"message,omitempty"`
}

// TrafficMapCapacity records why an entry's weight is what it is.
type TrafficMapCapacity struct {
	// Allocated is the effective allocation ceiling for this home: the
	// control-plane-admitted count, optionally lowered by a capacity report.
	// Ready independently caps it when computing Weight.
	// +optional
	Allocated int32 `json:"allocated,omitempty"`

	// Ready is the home's live ready-replica count. It caps Allocated when
	// computing Weight; zero health-gates the entry to weight 0.
	// +optional
	Ready int32 `json:"ready,omitempty"`

	// Factor is the per-replica relative serving capacity of this home's
	// accelerator (baseline "1", higher = faster), so heterogeneous hardware is
	// weighted by capacity rather than raw replica count: weight is proportional
	// to min(allocated, ready) * factor. Defaults to "1" (homogeneous) when unset — the
	// multiplicative identity, i.e. no scaling. It is operator-supplied config,
	// never derived from raw hardware FLOPS.
	// +optional
	Factor *resource.Quantity `json:"factor,omitempty"`

	// Source names which input produced Allocated. FallbackReason separately
	// distinguishes an intentionally control-plane-only allocation from a
	// configured endpoint source that could not supply a usable ceiling.
	// +optional
	// +kubebuilder:default=ControlPlane
	Source CapacitySource `json:"source,omitempty"`

	// Reported is the home's own servable-capacity figure, recorded whenever the
	// endpoint source answered — including when it was at or above the plan and
	// therefore did not lower Allocated. Keeping it visible in that case is what
	// makes the ceiling auditable: Allocated alone cannot show that a report was
	// received and found non-binding.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Reported *int32 `json:"reported,omitempty"`

	// FallbackReason explains why a configured endpoint capacity source did not
	// produce an applied ceiling and the control-plane allocation was retained.
	// Empty means capacity polling is disabled or its latest result was usable.
	// +optional
	FallbackReason string `json:"fallbackReason,omitempty"`
}

// TrafficMapStatus reports whether the active publisher has realized this map
// onto a concrete data plane, and whether the map has anything to realize.
// Two writers share it: the publisher owns Published, GatewayRef,
// ObservedTrafficMapGeneration, Publisher, and the Published condition; the
// routing controller owns SourceUID plus the Routable and CapacityFallback
// conditions. Conditions is keyed on type, so each writer must patch only its
// own fields and conditions.
type TrafficMapStatus struct {
	// SourceUID is the UID of the InferenceService that generated this map. The
	// routing controller writes it through its own status field manager before a
	// publisher may create external effects. It remains available if orphan
	// deletion propagation removes the owner reference.
	// +optional
	SourceUID types.UID `json:"sourceUID,omitempty"`

	// Published reports whether the active publisher has realized this map onto
	// its data plane.
	// +optional
	Published bool `json:"published,omitempty"`

	// GatewayRef identifies the data plane object realizing this map (e.g. the
	// HTTPRoute the Gateway API publisher created). Empty until published.
	// +optional
	GatewayRef *TrafficMapGatewayRef `json:"gatewayRef,omitempty"`

	// ObservedTrafficMapGeneration is the spec generation the publisher last
	// realized, so lag between generation and realization is observable.
	// +optional
	ObservedTrafficMapGeneration int64 `json:"observedTrafficMapGeneration,omitempty"`

	// Publisher records the durable target claims needed to recover or reverse
	// data-plane mutations made while realizing this map.
	// +optional
	Publisher *TrafficMapPublisherStatus `json:"publisher,omitempty"`

	// Conditions carry the routing controller's Routable and CapacityFallback
	// conditions and the publisher's Published condition.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// TrafficMapPublisherStatus is the durable cleanup journal for one publisher.
// Claims are persisted before external mutation and retained until their
// targets have been cleaned up.
type TrafficMapPublisherStatus struct {
	// PublisherName identifies the publisher implementation whose cleanup
	// semantics apply. An implementation upgraded under the same name must remain
	// able to clean target identifiers written by its prior versions.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	PublisherName string `json:"publisherName"`

	// ClaimedTargets are publisher-defined canonical identifiers owned by this
	// map. The publisher records the complete claim set before mutating any target.
	// Identifiers are status-visible and must not contain credentials or secrets.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=512
	ClaimedTargets []string `json:"claimedTargets,omitempty"`

	// ObservedOptionsDigest is the SHA-256 digest of the effective safe publisher
	// options used by the most recent successful reconciliation. It is absent
	// before the first successful reconciliation and does not determine target
	// ownership.
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	ObservedOptionsDigest string `json:"observedOptionsDigest,omitempty"`
}

// TrafficMapGatewayRef points at the data plane object a publisher created to
// realize a TrafficMap.
type TrafficMapGatewayRef struct {
	// Group is the API group of the realizing object (e.g.
	// "gateway.networking.k8s.io"). Empty means the core group.
	// +optional
	Group string `json:"group,omitempty"`

	// Kind is the object kind (e.g. "HTTPRoute").
	// +required
	Kind string `json:"kind"`

	// Name is the object name.
	// +required
	Name string `json:"name"`

	// Namespace is the object namespace. Empty means the TrafficMap's namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

func init() {
	SchemeBuilder.Register(&TrafficMap{}, &TrafficMapList{})
}
