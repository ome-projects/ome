package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
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

	// PlacementModeSplit distributes the requested floor. ClusterAffinity uses
	// exact weighted shares; Legacy uses admission-driven Packed or spread targets.
	PlacementModeSplit PlacementMode = "Split"

	// PlacementModeSplitByCapacity apportions the floor using verified nominal
	// whole-replica hardware capacity, independently of quota and utilization.
	PlacementModeSplitByCapacity PlacementMode = "SplitByCapacity"
)

// PlacementPolicy selects the matching and allocation contract.
// +kubebuilder:validation:Enum=Legacy;ClusterAffinity
type PlacementPolicy string

const (
	// PlacementPolicyLegacy uses selector strings and admission-driven allocation.
	PlacementPolicyLegacy PlacementPolicy = "Legacy"
	// PlacementPolicyClusterAffinity uses affinity and persisted allocation plans.
	PlacementPolicyClusterAffinity PlacementPolicy = "ClusterAffinity"
)

// PlacementSpec declares multi-cluster intent. Policy omission preserves the
// legacy selector and allocation contract, including its mode defaults.
// +kubebuilder:validation:XValidation:rule="!has(self.policy) || self.policy != 'ClusterAffinity' || has(self.mode)",message="ClusterAffinity requires an explicit mode"
// +kubebuilder:validation:XValidation:rule="!has(self.replacementTimeout) || (has(self.policy) && self.policy == 'ClusterAffinity' && has(self.mode) && self.mode == 'Single')",message="replacementTimeout requires ClusterAffinity Single placement"
type PlacementSpec struct {
	// Policy explicitly opts into ClusterAffinity semantics. Omission is Legacy.
	// ClusterAffinity cannot be removed from an existing service; migrating back
	// requires draining and recreating the source and its derived workloads.
	// +optional
	Policy PlacementPolicy `json:"policy,omitempty"`

	// Mode is required for ClusterAffinity. Legacy omission means Single.
	// +optional
	Mode PlacementMode `json:"mode,omitempty"`

	// ClusterAffinity ORs terms whose requirements are ANDed. Requires the
	// ClusterAffinity policy; omission then matches every registration.
	// An explicit empty or null list is invalid.
	// +optional
	// +nullable
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	ClusterAffinity []ClusterAffinityTerm `json:"clusterAffinity,omitempty"`

	// MaxSurge is the whole-replica allowance shared by placement transitions
	// and local rollout surge. Omission blocks disruptive movement between homes.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxSurge *int32 `json:"maxSurge,omitempty"`

	// ReplacementTimeout bounds a non-admitting probe during a healthy Single
	// move. Omission retains pending probes; a positive duration allows rotation.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="replacementTimeout must be a positive duration"
	ReplacementTimeout *metav1.Duration `json:"replacementTimeout,omitempty"`

	// LegacyFields retains explicit zero-valued obsolete fields during JSON round trips.
	// It is serialization bookkeeping and is not a wire field.
	LegacyFields PlacementLegacyFields `json:"-"`

	// Requirements is a Legacy label selector, ANDed with ClusterSelector.
	// Deprecated: opt into ClusterAffinity and use clusterAffinity.
	// +optional
	// +nullable
	Requirements string `json:"requirements,omitempty"`

	// ClusterSelector is a Legacy selector over labels and virtual metadata.name.
	// Deprecated: opt into ClusterAffinity and use clusterAffinity.
	// +optional
	// +nullable
	ClusterSelector string `json:"clusterSelector,omitempty"`

	// Split provides the requested floor and optional per-home ceiling for
	// Split and SplitByCapacity. ClusterAffinity rejects it in other modes.
	// +optional
	Split *SplitSpec `json:"split,omitempty"`

	// CapacityFactors is the Legacy alias for routing capacity factors.
	// Deprecated: use spec.routing.capacityFactors.
	// +optional
	// +nullable
	CapacityFactors map[string]resource.Quantity `json:"capacityFactors,omitempty"`
}

// SplitSpec declares the fleet floor and optional local ceiling. The floor
// falls back only to an explicitly declared positive engine.minReplicas.
type SplitSpec struct {
	// LegacyFields retains explicit zero-valued obsolete fields during JSON round trips.
	// It is serialization bookkeeping and is not a wire field.
	LegacyFields SplitLegacyFields `json:"-"`

	// Replicas is the fleet-wide desired replica count to distribute across homes.
	// Unset falls back to the engine component's minReplicas (the guaranteed
	// floor) — the count OME actually guarantees running and thus the one worth
	// spreading. maxReplicas is deliberately NOT used (it is an autoscaling
	// ceiling that stays a per-home local concern).
	// +optional
	// +kubebuilder:validation:Minimum=1
	Replicas *int32 `json:"replicas,omitempty"`

	// Spread requests ceil(replicas/candidates) on each Legacy candidate.
	// False uses admission-driven packing in candidate name order.
	// Deprecated: ClusterAffinity uses exact shares and optional affinity weights.
	// +optional
	// +nullable
	Spread bool `json:"spread,omitempty"`

	// MaxReplicasPerCluster is an optional local ceiling. ClusterAffinity holds
	// plans exceeding it; Legacy clips requests to it. Zero leaves it uncapped.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxReplicasPerCluster int32 `json:"maxReplicasPerCluster,omitempty"`

	// MinReplicasPerCluster discards Legacy homes admitted below this count.
	// Deprecated: ClusterAffinity exact shares cannot discard a small admission.
	// +optional
	// +nullable
	MinReplicasPerCluster int32 `json:"minReplicasPerCluster,omitempty"`
}

// UsesClusterAffinity reports the explicit opt-in, without inferring policy
// from selector presence, mode, or member state.
func (p *PlacementSpec) UsesClusterAffinity() bool {
	return p != nil && p.Policy == PlacementPolicyClusterAffinity
}

// EffectiveMode resolves only the Legacy default. ClusterAffinity requires an
// explicit mode so missing intent cannot acquire allocation authority.
func (p *PlacementSpec) EffectiveMode() PlacementMode {
	if p == nil || (!p.UsesClusterAffinity() && p.Mode == "") {
		return PlacementModeSingle
	}
	return p.Mode
}
