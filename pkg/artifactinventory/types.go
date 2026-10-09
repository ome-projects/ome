// Package artifactinventory maps a node's Ready BaseModels to artifact paths
// and estimated sizes. It reads Kubernetes records, never the node filesystem.
package artifactinventory

import "k8s.io/apimachinery/pkg/types"

// Inventory is rebuilt on each call. It is not an atomic or complete census of
// live references: models outside the ConfigMap's Ready entries are excluded.
type Inventory struct {
	NodeName  string     `json:"nodeName"`
	Artifacts []Artifact `json:"artifacts"`
}

// Artifact represents one directory on the requested node, counted once even
// when multiple BaseModels or namespaces share it.
type Artifact struct {
	Path                   string `json:"path"`
	EstimatedArtifactBytes *int64 `json:"estimatedArtifactBytes"`
	// EvictionEligible means mapping and size checks passed. Serving protection
	// and final eviction authorization belong to the caller.
	EvictionEligible bool       `json:"evictionEligible"`
	Models           []ModelRef `json:"models"`
	Issues           []Issue    `json:"issues"`
}

// ModelRef retains CR identity and the model's entry path, which can differ
// from Artifact.Path when the entry points to a shared parent.
type ModelRef struct {
	Namespace       string    `json:"namespace"`
	Name            string    `json:"name"`
	UID             types.UID `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
	TenancyID       string    `json:"tenancyId"`
	Path            string    `json:"path"`

	// NodesReady snapshots CR status for estimating potential node impact.
	// CM entries determine readiness on the requested node, not this list.
	NodesReady []string `json:"nodesReady"`
}

// Issue explains why the entire artifact is excluded from automatic selection.
type Issue struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	ModelKey string `json:"modelKey,omitempty"`
}
