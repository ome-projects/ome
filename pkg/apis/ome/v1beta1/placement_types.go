package v1beta1

import "k8s.io/apimachinery/pkg/api/resource"

// PlacementMode is the cardinality of a multi-cluster placement: how many
// workload clusters end up serving the InferenceService.
// +kubebuilder:validation:Enum=Single;All;Split
type PlacementMode string

const (
	// PlacementModeSingle places the InferenceService on exactly one workload
	// cluster (the winner of the fan-out race) and sweeps the rest. This is
	// the default.
	PlacementModeSingle PlacementMode = "Single"

	// PlacementModeAll places the InferenceService on every candidate cluster
	// that admits it; none are swept and each autoscales locally against its own
	// Kueue quota. The endpoint fans out across all serving homes. For
	// redundancy / serve-everywhere.
	PlacementModeAll PlacementMode = "All"

	// PlacementModeSplit distributes the InferenceService's desired replicas
	// fractionally across candidate clusters: each admits as many replicas as
	// fit, every cluster that admits >=1 is kept, and the endpoint is weighted by
	// each home's ready replicas. For scaling past a single cluster's capacity.
	PlacementModeSplit PlacementMode = "Split"
)

// PlacementSpec declares how the control plane selects the workload clusters an
// InferenceService is placed onto, and how many of them serve it. It subsumes the
// legacy ome.io/accelerator-requirements and ome.io/cluster-selector annotations
// in a typed, schema-validated form; when this field is nil the control plane
// still honors those annotations for backward compatibility.
type PlacementSpec struct {
	// Mode is the placement cardinality: Single (one cluster), All (every
	// candidate), or Split (replicas distributed across clusters). Defaults to
	// Single. All and Split are rejected by admission on a control plane that
	// does not implement them.
	// +kubebuilder:default=Single
	// +optional
	Mode PlacementMode `json:"mode,omitempty"`

	// Requirements is the intrinsic capability selector a candidate workload
	// cluster MUST satisfy, expressed as a Kubernetes label-selector string
	// matched against WorkloadCluster labels (e.g. "accelerator in (gb300,
	// tpu7x)"). It is the structured equivalent of the
	// ome.io/accelerator-requirements annotation. Empty means no intrinsic
	// requirement.
	// +optional
	Requirements string `json:"requirements,omitempty"`

	// ClusterSelector is an optional operator-imposed routing overlay
	// (label-selector string) AND-ed onto Requirements to further narrow
	// candidates (e.g. "provider=cloud-a"). Structured equivalent of the
	// ome.io/cluster-selector annotation.
	// +optional
	ClusterSelector string `json:"clusterSelector,omitempty"`

	// Split tunes Split mode (distributing replicas across clusters). Only
	// consulted when Mode is Split; ignored otherwise. Nil means Split defaults:
	// distribute the engine's minReplicas, packed onto the fewest clusters.
	// +optional
	Split *SplitSpec `json:"split,omitempty"`

	// CapacityFactors overrides the per-replica relative serving capacity of
	// named workload clusters, keyed by WorkloadCluster name. It weights traffic
	// for heterogeneous hardware where one cluster's replica serves more (or less)
	// than another's: a home's routed share scales with its admitted replicas
	// times this factor. A quantity of "2" means each replica on that cluster
	// carries twice the share of a factor-1 replica; "500m" means half. A cluster
	// absent from the map (or the whole field unset) uses the identity factor 1.
	// This is a routing weight only — it does not influence placement or how many
	// replicas a cluster admits.
	// +optional
	CapacityFactors map[string]resource.Quantity `json:"capacityFactors,omitempty"`
}

// SplitSpec tunes how Split mode distributes replicas across candidate
// clusters. All fields are optional and degrade to the documented defaults.
type SplitSpec struct {
	// Replicas is the fleet-wide desired replica count to distribute across homes.
	// Unset falls back to the engine component's minReplicas (the guaranteed
	// floor) — the count OME actually guarantees running and thus the one worth
	// spreading. maxReplicas is deliberately NOT used (it is an autoscaling
	// ceiling that stays a per-home local concern).
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Spread selects the apportionment policy. False (default) is Packed: fan out
	// in preference order and fill the fewest clusters the fleet's quota forces
	// (better locality, fewer endpoint backends). True is Balanced: apportion
	// ~evenly (ceil(N/candidates)) so replicas spread across more clusters
	// (blast-radius resilience over locality).
	// +optional
	Spread bool `json:"spread,omitempty"`

	// MaxReplicasPerCluster caps how many replicas one cluster may hold. It bounds
	// the over-request the fractional fan-out makes, and — combined with Spread —
	// is the lever that forces the fill to move on before a cluster is full
	// (deliberate spread without reading capacity). Zero means no cap.
	// +optional
	MaxReplicasPerCluster int32 `json:"maxReplicasPerCluster,omitempty"`

	// MinReplicasPerCluster is the anti-sliver floor: a home that admits fewer
	// than this is dropped and its replicas returned to the deficit, so the
	// placement does not keep a home serving a tiny, uneconomical fraction. Zero
	// keeps any home that admitted >=1.
	// +optional
	MinReplicasPerCluster int32 `json:"minReplicasPerCluster,omitempty"`
}
