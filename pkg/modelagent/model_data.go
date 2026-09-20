// Package modelagent implements the model agent components for managing models in OME.
package modelagent

import (
	"encoding/json"

	"sigs.k8s.io/ome/pkg/modelparser"
)

// ModelStatus represents the status of a model on a node
type ModelStatus string

// Model status constants
const (
	// ModelStatusReady indicates the model is ready for use
	ModelStatusReady ModelStatus = "Ready"
	// ModelStatusUpdating indicates the model is currently being downloaded or updated
	ModelStatusUpdating ModelStatus = "Updating"
	// ModelStatusFailed indicates the model failed to download or initialize
	ModelStatusFailed ModelStatus = "Failed"
	// ModelStatusDeleted indicates the model was deleted
	ModelStatusDeleted ModelStatus = "Deleted"
)

// ConfigParsingAnnotation is the annotation key to skip config parsing
const ConfigParsingAnnotation = "ome.oracle.com/skip-config-parsing"

// Hugging Face origin annotations are written by the control plane for OCI
// imports that were originally resolved from Hugging Face. The model agent uses
// them only as provenance metadata for local artifact reuse; the model source
// remains OCI Object Storage.
const (
	ArtifactOriginTypeHuggingFace = "huggingface"

	HuggingFaceModelIDAnnotationKey = "hf-model-id"
	HuggingFaceSHAAnnotationKey     = "hf-model-sha"
)

// ModelMetadata is the metadata produced by the shared modelparser bridge.
// It is aliased so existing modelagent code (cache, configmap reconciler,
// gopher) keeps using the unqualified name while the parser implementation
// lives in pkg/modelparser.
type ModelMetadata = modelparser.ModelMetadata

// ModelConfig represents the configuration of a model
// This is a structured version of the model metadata that is stored in the ConfigMap
type ModelConfig struct {
	// Core model identification
	ModelType         string `json:"modelType,omitempty"`         // e.g., "mistral", "llama", "phi"
	ModelArchitecture string `json:"modelArchitecture,omitempty"` // e.g., "MistralModel", "LlamaModel"

	// Framework and format information
	ModelFramework map[string]string `json:"modelFramework,omitempty"` // e.g., {"name":"transformers","version":"4.34.0"}
	ModelFormat    map[string]string `json:"modelFormat,omitempty"`    // e.g., {"name":"safetensors","version":"1.0.0"}

	// Model capabilities and size
	ModelParameterSize string   `json:"modelParameterSize,omitempty"` // Human-readable size, e.g., "7.11B"
	MaxTokens          int32    `json:"maxTokens,omitempty"`          // Maximum context length, e.g., 32768
	ModelCapabilities  []string `json:"modelCapabilities,omitempty"`  // e.g., ["TEXT_GENERATION", "TEXT_EMBEDDINGS"]
	ApiCapabilities    []string `json:"apiCapabilities,omitempty"`    // e.g., ["OPENAI_V1_CHAT_COMPLETIONS"]

	// Advanced information
	DecodedModelConfiguration map[string]interface{} `json:"decodedModelConfiguration,omitempty"` // Detailed configuration
	Quantization              string                 `json:"quantization,omitempty"`              // Quantization type if applicable
	// Artifact downloading info
	Artifact Artifact `json:"artifact,omitempty"` // artifact's commit sha and paths
}

// Artifact records a model artifact's version (Sha) and storage paths.
// Aliased to the shared modelparser type.
type Artifact = modelparser.Artifact

// ArtifactOrigin records the source identity used to prove two local artifacts
// are equivalent even when they were downloaded through different storage
// backends.
type ArtifactOrigin = modelparser.ArtifactOrigin

// ArtifactIdentity is the normalized identity used while searching the node
// ConfigMap for a reusable artifact.
type ArtifactIdentity struct {
	OriginType  string
	HFModelID   string
	HFCommitSHA string
}

// DownloadProgress tracks the progress of a model download
type DownloadProgress struct {
	Phase            string  `json:"phase"`            // Scanning, Downloading, Finalizing
	TotalBytes       uint64  `json:"totalBytes"`       // Total bytes to download
	CompletedBytes   uint64  `json:"completedBytes"`   // Bytes downloaded so far
	TotalFiles       uint32  `json:"totalFiles"`       // Total number of files
	CompletedFiles   uint32  `json:"completedFiles"`   // Files downloaded so far
	SpeedBytesPerSec float64 `json:"speedBytesPerSec"` // Current download speed
	LastUpdated      string  `json:"lastUpdated"`      // RFC3339 timestamp of last update
}

// Percentage returns the download progress as a percentage (0-100)
func (p *DownloadProgress) Percentage() float64 {
	if p == nil || p.TotalBytes == 0 {
		return 0
	}
	return float64(p.CompletedBytes) / float64(p.TotalBytes) * 100
}

// ModelEntry represents an entry in the node model ConfigMap
// This is the top-level structure stored for each model in the ConfigMap
type ModelEntry struct {
	Name     string            `json:"name"`               // Name of the model
	Status   ModelStatus       `json:"status"`             // Current status of the model on this node
	Config   *ModelConfig      `json:"config,omitempty"`   // Model configuration, may be nil if just tracking status
	Progress *DownloadProgress `json:"progress,omitempty"` // Download progress, nil when not downloading
}

// ConvertMetadataToModelConfig converts internal ModelMetadata to a client-facing ModelConfig
// This transforms the internal representation to the structured format stored in ConfigMaps
func ConvertMetadataToModelConfig(metadata ModelMetadata) *ModelConfig {
	// Convert ModelFramework to map
	var modelFramework map[string]string
	if metadata.ModelFramework != nil {
		modelFramework = make(map[string]string)
		modelFramework["name"] = metadata.ModelFramework.Name
		if metadata.ModelFramework.Version != nil {
			modelFramework["version"] = *metadata.ModelFramework.Version
		}
	}

	// Convert ModelFormat to map
	var modelFormat map[string]string
	if metadata.ModelFormat.Name != "" {
		modelFormat = make(map[string]string)
		modelFormat["name"] = metadata.ModelFormat.Name
		if metadata.ModelFormat.Version != nil {
			modelFormat["version"] = *metadata.ModelFormat.Version
		}
	}

	// Convert Quantization to string
	var quantization string
	if metadata.Quantization != "" {
		quantization = string(metadata.Quantization)
	}

	// If DecodedModelConfiguration is nil but we have ModelConfiguration,
	// try to decode it
	decodedConfig := metadata.DecodedModelConfiguration
	if decodedConfig == nil && len(metadata.ModelConfiguration) > 0 {
		var configMap map[string]interface{}
		if err := json.Unmarshal(metadata.ModelConfiguration, &configMap); err == nil {
			decodedConfig = configMap
		}
	}

	// convert artifact
	var artifact Artifact
	if metadata.Artifact.Sha != "" || metadata.Artifact.Origin != nil || metadata.Artifact.ParentPath != nil || metadata.Artifact.ChildrenPaths != nil {
		currentArtifact := metadata.Artifact
		var origin *ArtifactOrigin
		if currentArtifact.Origin != nil {
			origin = &ArtifactOrigin{
				Type:        currentArtifact.Origin.Type,
				HFModelID:   currentArtifact.Origin.HFModelID,
				HFCommitSHA: currentArtifact.Origin.HFCommitSHA,
			}
		}
		// Deep copy ParentPath to avoid aliasing
		var parent map[string]string
		if currentArtifact.ParentPath != nil {
			parent = make(map[string]string, len(currentArtifact.ParentPath))
			for k, v := range currentArtifact.ParentPath {
				parent[k] = v
			}
		}
		// Preserve nil vs empty slice semantics for ChildrenPaths
		var children []string
		if currentArtifact.ChildrenPaths != nil {
			children = make([]string, len(currentArtifact.ChildrenPaths))
			copy(children, currentArtifact.ChildrenPaths)
		}
		artifact = Artifact{
			Sha:           currentArtifact.Sha,
			Origin:        origin,
			ParentPath:    parent,
			ChildrenPaths: children,
		}
	}

	return &ModelConfig{
		ModelType:                 metadata.ModelType,
		ModelArchitecture:         metadata.ModelArchitecture,
		ModelFramework:            modelFramework,
		ModelFormat:               modelFormat,
		ModelParameterSize:        metadata.ModelParameterSize,
		MaxTokens:                 metadata.MaxTokens,
		ModelCapabilities:         metadata.ModelCapabilities,
		ApiCapabilities:           metadata.ApiCapabilities,
		DecodedModelConfiguration: decodedConfig,
		Quantization:              quantization,
		Artifact:                  artifact,
	}
}
