// Package modelparser parses Hugging Face model directories (config.json /
// model_index.json + safetensors headers) and produces metadata that callers
// can use to populate BaseModel / ClusterBaseModel CRD specs. The parser is
// shared between the per-node model agent and (in the future) the controller
// path that consumes models via cluster_cache.
package modelparser

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ConfigParsingAnnotation is the annotation key to skip config parsing
const ConfigParsingAnnotation = "ome.oracle.com/skip-config-parsing"

// ModelMetadata contains the extracted metadata about a model
type ModelMetadata struct {
	ModelType                 string
	ModelArchitecture         string
	ModelFramework            *v1beta1.ModelFrameworkSpec
	ModelFormat               v1beta1.ModelFormat
	ModelParameterSize        string
	MaxTokens                 int32
	ModelCapabilities         []string
	ApiCapabilities           []string
	ModelConfiguration        []byte
	DecodedModelConfiguration map[string]interface{} `json:"DecodedModelConfiguration,omitempty"`
	Quantization              v1beta1.ModelQuantization
	DiffusionPipeline         *v1beta1.DiffusionPipelineSpec
	Artifact                  Artifact `json:"Artifact,omitempty"`
}

// Artifact records the information of model artifact, including version (Sha) and storage paths
type Artifact struct {
	Sha string `json:"sha"` // sha string fetched from HuggingFace
	// Origin captures optional provenance for artifact reuse across storage
	// backends, for example OCI objects imported from a Hugging Face revision.
	Origin *ArtifactOrigin `json:"origin,omitempty"`
	// parent model name -> parent model artifact storage path
	// parent name convention is
	// For ClusterBaseModel: clusterbasemodel.{model_name}
	// For BaseModel: {namespace}.basemodel.{model_name}
	ParentPath    map[string]string `json:"parentPath"`
	ChildrenPaths []string          `json:"childrenPaths"` // an array of children paths
}

// ArtifactOrigin records the source identity used to prove two local artifacts
// are equivalent even when they were downloaded through different storage
// backends.
type ArtifactOrigin struct {
	Type        string `json:"type,omitempty"`
	HFModelID   string `json:"hfModelId,omitempty"`
	HFCommitSHA string `json:"hfCommitSha,omitempty"`
}
