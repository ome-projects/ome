package v1beta1

import corev1 "k8s.io/api/core/v1"

// Cluster affinity bounds are API limits on selector expression size.
const (
	MaxClusterAffinityTerms        = 64
	MaxClusterSelectorRequirements = 64
	MaxClusterSelectorValues       = 64
)

// ClusterAffinityTerm selects WorkloadClusters by ANDing its requirements.
// Multiple terms form a union; weights assign replica shares to each match.
// +kubebuilder:validation:XValidation:rule="(has(self.matchExpressions) && size(self.matchExpressions) > 0) || (has(self.matchFields) && size(self.matchFields) > 0)",message="a cluster affinity term must contain a requirement"
type ClusterAffinityTerm struct {
	// Weights sets each component's replica ratio on the clusters this term
	// matches, and is permitted only for static Split without a split block.
	// If any term gives a component a ratio, the ratios of matching terms add
	// and a term that gives none contributes {min: 1, max: 1}. With no weights
	// on any term, every matched cluster weighs one and routers are divided.
	// +optional
	Weights *ClusterAffinityWeights `json:"weights,omitempty"`

	// MatchExpressions selects the WorkloadCluster's actual labels.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	MatchExpressions []ClusterSelectorRequirement `json:"matchExpressions,omitempty"`

	// MatchFields selects the real metadata.name using In or NotIn. Multiple
	// names are allowed; a label named metadata.name does not affect this match.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	MatchFields []ClusterSelectorRequirement `json:"matchFields,omitempty"`
}

// ClusterSelectorRequirement tests one label or supported object field.
type ClusterSelectorRequirement struct {
	// Key is a label key, or metadata.name for a field requirement.
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
	// Operator follows Kubernetes node-selector semantics for labels.
	// +kubebuilder:validation:Enum=In;NotIn;Exists;DoesNotExist;Gt;Lt
	Operator corev1.NodeSelectorOperator `json:"operator"`
	// Values is nonempty for In/NotIn, empty for Exists/DoesNotExist, and a
	// single integer for Gt/Lt.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	Values []string `json:"values,omitempty"`
}

// ClusterAffinityWeights holds one term's replica ratio per component. The
// engine takes engine, then default. The decoder takes decoder, then default,
// then the engine's ratio. The router takes router, then default, then the
// engine's ratio. Routers are divided across clusters unless the terms name
// only the engine or decoder; then every cluster runs the full router policy.
// +kubebuilder:validation:MinProperties=1
type ClusterAffinityWeights struct {
	// Default is the ratio of every component this term does not name.
	// Setting it on any term divides router.minReplicas across the clusters,
	// as router does.
	// +optional
	Default *ReplicaRatio `json:"default,omitempty"`

	// Engine is the engine's ratio.
	// +optional
	Engine *ReplicaRatio `json:"engine,omitempty"`

	// Decoder is the decoder's ratio.
	// +optional
	Decoder *ReplicaRatio `json:"decoder,omitempty"`

	// Router is the router's ratio. Setting it or default on any term divides
	// router.minReplicas across the clusters, as setting no weights does;
	// ratios that name only the engine or decoder keep the full router policy
	// on every cluster.
	// +optional
	Router *ReplicaRatio `json:"router,omitempty"`
}

// ReplicaRatio gives a cluster's share of a component's minimum and maximum
// replicas. Both values are ratios, not replica counts: min divides the
// source's minReplicas and max divides its maxReplicas across the clusters.
// An omitted min counts as 1 and an omitted max counts as 1, independently,
// so an empty ratio weighs the same as a matching term that sets no ratio.
type ReplicaRatio struct {
	// Min is the cluster's ratio share of minReplicas. When omitted, it is 1.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Min *int32 `json:"min,omitempty"`

	// Max is the cluster's ratio share of maxReplicas. When omitted, it is 1.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Max *int32 `json:"max,omitempty"`
}

// MinOrOne returns Min when set and 1 otherwise.
func (r *ReplicaRatio) MinOrOne() int32 {
	if r.Min != nil {
		return *r.Min
	}
	return 1
}

// MaxOrOne returns Max when set and 1 otherwise.
func (r *ReplicaRatio) MaxOrOne() int32 {
	if r.Max != nil {
		return *r.Max
	}
	return 1
}
