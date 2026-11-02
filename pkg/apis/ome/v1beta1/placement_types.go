package v1beta1

import (
	"cmp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlacementMode is the cardinality of a multi-cluster placement: how many
// workload clusters end up serving the InferenceService.
// +kubebuilder:validation:Enum=Single;All;Split;SplitByCapacity
type PlacementMode string

const (
	// PlacementModeSingle places the InferenceService on exactly one workload
	// cluster (the winner of the fan-out race) and sweeps the rest.
	PlacementModeSingle PlacementMode = "Single"

	// PlacementModeAll places the InferenceService on every candidate cluster
	// that admits it; none are swept and each autoscales locally against its own
	// Kueue quota. The endpoint fans out across all serving homes. For
	// redundancy / serve-everywhere.
	PlacementModeAll PlacementMode = "All"

	// PlacementModeSplit distributes the requested floor in exact weighted shares.
	PlacementModeSplit PlacementMode = "Split"

	// PlacementModeSplitByCapacity apportions the floor using verified nominal
	// whole-replica hardware capacity, independently of quota and utilization.
	PlacementModeSplitByCapacity PlacementMode = "SplitByCapacity"
)

// PlacementPolicy selects the matching and allocation contract.
// +kubebuilder:validation:Enum=ClusterAffinity
type PlacementPolicy string

const (
	// PlacementPolicyClusterAffinity uses affinity and persisted allocation plans.
	PlacementPolicyClusterAffinity PlacementPolicy = "ClusterAffinity"
)

// PlacementSpec declares explicit multi-cluster matching and allocation intent.
// +kubebuilder:validation:XValidation:rule="!has(self.replacementTimeout) || (has(self.policy) && self.policy == 'ClusterAffinity' && has(self.mode) && self.mode == 'Single')",message="replacementTimeout requires Single placement"
// +kubebuilder:validation:XValidation:rule="!has(self.clusterAffinity) || !(self.clusterAffinity.exists(t, has(t.weights) && (has(t.weights.defaultReplicas) || has(t.weights.engineReplicas) || has(t.weights.decoderReplicas) || has(t.weights.routerReplicas))) && self.clusterAffinity.exists(t, has(t.weights) && (has(t.weights.default) || has(t.weights.engine) || has(t.weights.decoder) || has(t.weights.router))))",message="clusterAffinity weights use either default/engine/decoder/router or the *Replicas ratios, not both"
// +kubebuilder:validation:XValidation:rule="!has(self.clusterAffinity) || !self.clusterAffinity.exists(t, has(t.weights) && (has(t.weights.defaultReplicas) || has(t.weights.engineReplicas) || has(t.weights.decoderReplicas) || has(t.weights.routerReplicas))) || (self.mode == 'Split' && !has(self.split))",message="*Replicas ratios are allowed only in Split and replace spec.placement.split; remove split"
type PlacementSpec struct {
	// Policy selects affinity matching and persisted allocation plans.
	// +required
	Policy PlacementPolicy `json:"policy"`

	// Mode selects the placement cardinality and replica allocation strategy.
	// +required
	Mode PlacementMode `json:"mode"`

	// ClusterAffinity ORs terms whose requirements are ANDed. The engine's
	// clusters are every component's clusters; in Split, each term's weights
	// set the components' shares. Omission matches every registration.
	// An explicit empty or null list is invalid.
	// +optional
	// +nullable
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	ClusterAffinity []ClusterAffinityTerm `json:"clusterAffinity,omitempty"`

	// Deprecated: MaxSurge is ignored. A placement move may hold one extra full
	// copy of the desired allocation; admission warns when this is set.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxSurge *int32 `json:"maxSurge,omitempty"`

	// ReplacementTimeout bounds a non-admitting probe during a healthy Single
	// move. Omission retains pending probes; a positive duration allows rotation.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="replacementTimeout must be a positive duration"
	ReplacementTimeout *metav1.Duration `json:"replacementTimeout,omitempty"`

	// Split provides the requested floor and optional per-home ceiling for
	// Split and SplitByCapacity. ClusterAffinity rejects it in other modes.
	// +optional
	Split *SplitSpec `json:"split,omitempty"`
}

// SplitSpec declares the fleet floor and optional local ceilings. The floor
// falls back only to an explicitly declared positive engine.minReplicas.
type SplitSpec struct {
	// Replicas is the fleet-wide desired replica count to distribute across homes.
	// Unset falls back to the engine component's minReplicas (the guaranteed
	// floor) — the count OME actually guarantees running and thus the one worth
	// spreading. maxReplicas is deliberately NOT used (it is an autoscaling
	// ceiling that stays a per-home local concern).
	// +optional
	// +kubebuilder:validation:Minimum=1
	Replicas *int32 `json:"replicas,omitempty"`

	// MaxPerCluster caps how many replicas of each component one cluster runs.
	// A plan that gives a component more than its cap is held.
	// +optional
	MaxPerCluster *SplitCeilings `json:"maxPerCluster,omitempty"`
}

// SplitCeilings holds one cap per component. A component takes its own key,
// then default. A component with neither takes the primary component's cap,
// scaled by its ratio of minimums and rounded up; the primary component is
// the engine, or the decoder when there is no engine. A component with no cap
// is fixed at its share.
// +kubebuilder:validation:MinProperties=1
type SplitCeilings struct {
	// Default caps every component this map does not name.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Default *int32 `json:"default,omitempty"`

	// Engine caps the engine.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Engine *int32 `json:"engine,omitempty"`

	// Decoder caps the decoder.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Decoder *int32 `json:"decoder,omitempty"`

	// Router caps the router. It applies only when Split divides the routers.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Router *int32 `json:"router,omitempty"`
}

// Ceiling returns the cap the source declares for a component on one cluster:
// its maxPerCluster key, else the map's default. Zero means none is declared;
// a secondary component then takes the primary's cap in its ratio.
func (s *SplitSpec) Ceiling(component ComponentType) int32 {
	if s == nil || s.MaxPerCluster == nil {
		return 0
	}
	c := s.MaxPerCluster
	var key *int32
	switch component {
	case EngineComponent:
		key = c.Engine
	case DecoderComponent:
		key = c.Decoder
	case RouterComponent:
		key = c.Router
	}
	if key = cmp.Or(key, c.Default); key != nil {
		return *key
	}
	return 0
}

// UsesClusterAffinity reports the explicit opt-in, without inferring policy
// from selector presence, mode, or member state.
func (p *PlacementSpec) UsesClusterAffinity() bool {
	return p != nil && p.Policy == PlacementPolicyClusterAffinity
}
