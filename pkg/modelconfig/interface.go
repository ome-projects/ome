// Package modelconfig parses Hugging Face model configurations
// (config.json and model_index.json) into a uniform interface for
// downstream metadata extraction (parameter count, context length,
// vision capability, quantization, etc.).
//
// All consumers should depend on the interfaces below; the concrete
// types live in config.go (transformer configs) and diffusion.go
// (diffusion pipelines).
package modelconfig

// HuggingFaceModel is the contract every parsed config implements.
type HuggingFaceModel interface {
	// GetParameterCount returns the total number of parameters in the model.
	GetParameterCount() int64
	// GetTransformerVersion returns the transformers library version used for this model.
	GetTransformerVersion() string
	// GetQuantizationType returns the quantization method used (if any).
	GetQuantizationType() string
	// GetArchitecture returns the model architecture (e.g., "LlamaForCausalLM").
	GetArchitecture() string
	// GetModelType returns the model type (e.g., "llama", "deepseek_v3").
	GetModelType() string
	// GetContextLength returns the maximum context length supported by the model.
	GetContextLength() int
	// GetModelSizeBytes returns the estimated size of the model in bytes.
	GetModelSizeBytes() int64
	// GetTorchDtype returns the torch data type used by the model.
	GetTorchDtype() string
	// HasVision returns true if this is a multimodal vision model.
	HasVision() bool
	// IsEmbedding returns true if this model is intended for generating embeddings.
	IsEmbedding() bool
	// GetCapabilities returns the capabilities (inference tasks) the
	// model serves. Always returns at least one element; an opaque
	// model returns []Capability{CapabilityUnknown}.
	GetCapabilities() []Capability
	// GetHFQuantConfig returns quantization metadata in the unified
	// HFQuantConfig shape, bridging the two real-world schemas
	// (separate hf_quant_config.json vs embedded quantization_config).
	// Returns nil for unquantized models.
	GetHFQuantConfig() *HFQuantConfig
}

// HuggingFaceDiffusionModel is the additional contract for diffusion
// pipelines parsed from model_index.json.
type HuggingFaceDiffusionModel interface {
	HuggingFaceModel
	GetDiffusionModel() *DiffusionPipelineSpec
}

// ModelConfigInput provides config JSON from either a local path or
// a non-filesystem source. Path is used only as a logical label and
// to disambiguate model_index.json from config.json.
type ModelConfigInput struct {
	Path string
	Data []byte
	// SentenceTransformersConfig holds the config_sentence_transformers.json
	// found beside the config, if the model ships one.
	SentenceTransformersConfig []byte
}
