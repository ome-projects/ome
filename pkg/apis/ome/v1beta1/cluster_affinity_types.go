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
	// Weights sets each component's replica weight on the clusters this term
	// matches, and is permitted only for static Split. If any term gives a
	// component a weight, the weights of matching terms add and a term that
	// gives none contributes one. Weights are ratios, so they have no upper bound.
	// With no weights on any term, every matched cluster weighs one and routers
	// are divided, as a default weight of one on every term would.
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

// ClusterAffinityWeights holds one term's weight per component. The engine
// takes engine, then default. The decoder takes decoder, then default, then
// the engine's weight. The router takes router, then default, then the
// engine's weight. Routers are divided across clusters unless the terms set
// weights that name only the engine or decoder; then every cluster runs the
// full router policy. The integer keys and the *Replicas ratio keys are two
// forms of the same weights; one placement uses only one form.
// +kubebuilder:validation:MinProperties=1
type ClusterAffinityWeights struct {
	// Default is the weight of every component this term does not name.
	// Setting it on any term divides router.minReplicas across the clusters,
	// as router does.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Default *int32 `json:"default,omitempty"`

	// Engine is the engine's weight.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Engine *int32 `json:"engine,omitempty"`

	// Decoder is the decoder's weight.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Decoder *int32 `json:"decoder,omitempty"`

	// Router is the router's weight. Setting it or default on any term divides
	// router.minReplicas across the clusters, as setting no weights does;
	// weights that name only the engine or decoder keep the full router policy
	// on every cluster.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Router *int32 `json:"router,omitempty"`

	// DefaultReplicas is the ratio form of default. It applies to every
	// component this term does not name.
	// +optional
	DefaultReplicas *ReplicaRatio `json:"defaultReplicas,omitempty"`

	// EngineReplicas is the ratio form of engine.
	// +optional
	EngineReplicas *ReplicaRatio `json:"engineReplicas,omitempty"`

	// DecoderReplicas is the ratio form of decoder.
	// +optional
	DecoderReplicas *ReplicaRatio `json:"decoderReplicas,omitempty"`

	// RouterReplicas is the ratio form of router.
	// +optional
	RouterReplicas *ReplicaRatio `json:"routerReplicas,omitempty"`
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
