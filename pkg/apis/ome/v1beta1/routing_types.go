package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RoutingSpec configures cross-cluster traffic distribution for an
// InferenceService. Fields that are unset inherit the operator-level routing
// configuration.
type RoutingSpec struct {
	// Enabled overrides routing for this InferenceService. Nil inherits the
	// operator-level setting; false opts this InferenceService out. True opts in
	// only when routing is enabled for the installation and cannot bypass a
	// disabled installation gate.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// CapacityFactors overrides the per-replica relative serving capacity of
	// named workload clusters. Every quantity must be greater than zero. A
	// cluster absent from the map uses the identity factor 1.
	// +optional
	CapacityFactors map[string]resource.Quantity `json:"capacityFactors,omitempty"`

	// Probe overrides the operator-level endpoint health probe. The block is a
	// complete replacement; set disabled to explicitly turn probing off.
	// +optional
	Probe *RoutingProbeSpec `json:"probe,omitempty"`

	// Capacity overrides the operator-level endpoint capacity poll. The block is
	// a complete replacement; set disabled to explicitly turn polling off.
	// +optional
	Capacity *RoutingCapacitySpec `json:"capacity,omitempty"`

	// Publisher supplies behavior options to the operator-selected publisher.
	// Publisher identity, transport, and credentials remain operator-level
	// configuration.
	// +optional
	Publisher *RoutingPublisherSpec `json:"publisher,omitempty"`
}

// RoutingProbeSpec configures an active end-to-end health probe for each
// serving home's externally addressable endpoint.
// +kubebuilder:validation:XValidation:rule="!(has(self.disabled) && self.disabled) || (!has(self.path) && !has(self.method) && !has(self.acceptStatuses) && !has(self.gateStatuses) && !has(self.period) && !has(self.timeout) && !has(self.failureThreshold) && !has(self.successThreshold) && !has(self.allFailedPolicy))",message="disabled must not be combined with probe configuration fields"
// +kubebuilder:validation:XValidation:rule="(has(self.disabled) && self.disabled) || (has(self.path) && has(self.method) && has(self.acceptStatuses) && has(self.gateStatuses) && has(self.period) && has(self.timeout) && has(self.failureThreshold) && has(self.successThreshold) && has(self.allFailedPolicy))",message="an enabled probe must specify path, method, acceptStatuses, gateStatuses, period, timeout, failureThreshold, successThreshold, and allFailedPolicy"
// +kubebuilder:validation:XValidation:rule="!has(self.period) || duration(self.period) > duration('0s')",message="period must be positive"
// +kubebuilder:validation:XValidation:rule="!has(self.timeout) || duration(self.timeout) > duration('0s')",message="timeout must be positive"
// +kubebuilder:validation:XValidation:rule="!has(self.period) || !has(self.timeout) || duration(self.timeout) < duration(self.period)",message="timeout must be shorter than period"
// +kubebuilder:validation:XValidation:rule="!has(self.gateStatuses) || !self.gateStatuses.exists(s, s == 401 || s == 403 || s == 429)",message="gateStatuses must not contain 401, 403, or 429"
type RoutingProbeSpec struct {
	// Disabled explicitly disables probing. No other field may be set when true.
	// +optional
	Disabled bool `json:"disabled,omitempty"`

	// Path is appended to each serving home's endpoint and must start with "/".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^/`
	// +optional
	Path string `json:"path,omitempty"`

	// Method is the HTTP method used by the probe.
	// +kubebuilder:validation:Enum=GET;HEAD;POST
	// +optional
	Method string `json:"method,omitempty"`

	// AcceptStatuses are response codes counted as a successful probe.
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:items:Minimum=100
	// +kubebuilder:validation:items:Maximum=599
	// +optional
	AcceptStatuses []int32 `json:"acceptStatuses,omitempty"`

	// GateStatuses are response codes counted as a failed probe. A code in
	// neither status list is inconclusive.
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=500
	// +kubebuilder:validation:items:Minimum=100
	// +kubebuilder:validation:items:Maximum=599
	// +optional
	GateStatuses []int32 `json:"gateStatuses,omitempty"`

	// Period is how often each serving home is probed.
	// +optional
	Period metav1.Duration `json:"period,omitempty"`

	// Timeout bounds one probe request and must be shorter than Period.
	// +optional
	Timeout metav1.Duration `json:"timeout,omitempty"`

	// FailureThreshold is the number of consecutive failures required to gate a
	// serving home.
	// +kubebuilder:validation:Minimum=1
	// +optional
	FailureThreshold int32 `json:"failureThreshold,omitempty"`

	// SuccessThreshold is the number of consecutive successes required to
	// restore a gated serving home.
	// +kubebuilder:validation:Minimum=1
	// +optional
	SuccessThreshold int32 `json:"successThreshold,omitempty"`

	// AllFailedPolicy controls the result when every serving home has crossed
	// the failure threshold.
	// +optional
	AllFailedPolicy RoutingAllFailedPolicy `json:"allFailedPolicy,omitempty"`
}

// RoutingAllFailedPolicy controls whether a complete, conclusive probe failure
// may remove every route arm.
// +kubebuilder:validation:Enum=PreserveTraffic;Drain
type RoutingAllFailedPolicy string

const (
	// RoutingAllFailedPolicyPreserveTraffic ignores only the probe gate when every
	// serving home fails its probe. Admitted, ready, and reported capacity still
	// determine the fallback weights.
	RoutingAllFailedPolicyPreserveTraffic RoutingAllFailedPolicy = "PreserveTraffic"

	// RoutingAllFailedPolicyDrain preserves all-zero weights when every serving
	// home fails its probe.
	RoutingAllFailedPolicyDrain RoutingAllFailedPolicy = "Drain"
)

// RoutingCapacitySpec configures polling each serving home for its current
// servable capacity.
// +kubebuilder:validation:XValidation:rule="!(has(self.disabled) && self.disabled) || (!has(self.path) && !has(self.method) && !has(self.format) && !has(self.options) && !has(self.period) && !has(self.timeout) && !has(self.samples) && !has(self.quorum) && !has(self.maxAge))",message="disabled must not be combined with capacity configuration fields"
// +kubebuilder:validation:XValidation:rule="(has(self.disabled) && self.disabled) || (has(self.path) && has(self.method) && has(self.format) && has(self.period) && has(self.timeout) && has(self.samples) && has(self.quorum) && has(self.maxAge))",message="an enabled capacity poll must specify path, method, format, period, timeout, samples, quorum, and maxAge"
// +kubebuilder:validation:XValidation:rule="!has(self.period) || duration(self.period) > duration('0s')",message="period must be positive"
// +kubebuilder:validation:XValidation:rule="!has(self.timeout) || duration(self.timeout) > duration('0s')",message="timeout must be positive"
// +kubebuilder:validation:XValidation:rule="!has(self.period) || !has(self.timeout) || duration(self.timeout) < duration(self.period)",message="timeout must be shorter than period"
// +kubebuilder:validation:XValidation:rule="!has(self.period) || !has(self.maxAge) || duration(self.maxAge) >= duration(self.period)",message="maxAge must be at least period"
// +kubebuilder:validation:XValidation:rule="!has(self.samples) || !has(self.quorum) || self.quorum <= self.samples",message="quorum must not exceed samples"
type RoutingCapacitySpec struct {
	// Disabled explicitly disables capacity polling. No other field may be set
	// when true.
	// +optional
	Disabled bool `json:"disabled,omitempty"`

	// Path is appended to each serving home's endpoint and must start with "/".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^/`
	// +optional
	Path string `json:"path,omitempty"`

	// Method is the HTTP method used by the capacity request.
	// +kubebuilder:validation:Enum=GET;HEAD;POST
	// +optional
	Method string `json:"method,omitempty"`

	// Format names the response shape returned by the capacity endpoint.
	// +kubebuilder:validation:MinLength=1
	// +optional
	Format string `json:"format,omitempty"`

	// Options contains format-specific settings interpreted by the configured
	// capacity format.
	// +optional
	Options map[string]string `json:"options,omitempty"`

	// Period is how often each serving home is polled.
	// +optional
	Period metav1.Duration `json:"period,omitempty"`

	// Timeout bounds one capacity request and must be shorter than Period.
	// +optional
	Timeout metav1.Duration `json:"timeout,omitempty"`

	// Samples is the number of recent readings in the capacity window.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Samples int32 `json:"samples,omitempty"`

	// Quorum is the number of readings required to corroborate a lower capacity.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Quorum int32 `json:"quorum,omitempty"`

	// MaxAge is the maximum accepted age of a capacity report.
	// +optional
	MaxAge metav1.Duration `json:"maxAge,omitempty"`
}

// RoutingPublisherSpec provides per-InferenceService behavior options to the
// single publisher selected by the operator.
type RoutingPublisherSpec struct {
	// Options are interpreted and validated by the operator-selected publisher.
	// +kubebuilder:validation:MaxProperties=32
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) > 0 && size(k) <= 128 && size(self[k]) > 0 && size(self[k]) <= 4096)",message="publisher option keys must contain 1-128 characters and values must contain 1-4096 characters"
	// +optional
	Options map[string]string `json:"options,omitempty"`
}

const (
	// MaxRoutingPublisherOptions is a stable API safety limit that bounds
	// admission and persisted-object work; it is not a routing behavior default.
	MaxRoutingPublisherOptions = 32

	// MaxRoutingPublisherOptionKeyBytes is a stable API safety limit that bounds
	// one option key in admission and persisted objects.
	MaxRoutingPublisherOptionKeyBytes = 128

	// MaxRoutingPublisherOptionValueBytes is a stable API safety limit that
	// bounds one option value in admission and persisted objects.
	MaxRoutingPublisherOptionValueBytes = 4096
)
