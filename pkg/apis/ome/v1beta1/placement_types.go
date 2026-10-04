package v1beta1

import (
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
// +kubebuilder:validation:XValidation:rule="!has(self.replacementTimeout) || (has(self.policy) && self.policy == 'ClusterAffinity' && has(self.mode) && self.mode == 'Single')",message="replacementTimeout requires ClusterAffinity Single placement"
type PlacementSpec struct {
	// Policy selects ClusterAffinity matching and persisted allocation plans.
	// +required
	Policy PlacementPolicy `json:"policy"`

	// Mode selects the placement cardinality and replica allocation strategy.
	// +required
	Mode PlacementMode `json:"mode"`

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

	// Split provides the requested floor and optional per-home ceiling for
	// Split and SplitByCapacity. ClusterAffinity rejects it in other modes.
	// +optional
	Split *SplitSpec `json:"split,omitempty"`
}

// SplitSpec declares the fleet floor and optional local ceiling. The floor
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

	// MaxReplicasPerCluster is an optional local ceiling. Plans exceeding it
	// are held; zero follows the assigned allocation without an extra ceiling.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxReplicasPerCluster int32 `json:"maxReplicasPerCluster,omitempty"`
}

// UsesClusterAffinity reports the explicit opt-in, without inferring policy
// from selector presence, mode, or member state.
func (p *PlacementSpec) UsesClusterAffinity() bool {
	return p != nil && p.Policy == PlacementPolicyClusterAffinity
}
