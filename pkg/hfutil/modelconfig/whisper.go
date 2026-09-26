package modelconfig

import (
	"encoding/json"
	"fmt"
	"os"
)

// WhisperConfig defines the configuration for Whisper speech recognition models
// (e.g., openai/whisper-large-v3, openai/whisper-large-v3-turbo).
//
// Whisper is an encoder-decoder Transformer where the encoder consumes log-Mel
// spectrogram features and the decoder produces text tokens. As a result the
// config carries separate dimensions for the encoder and decoder stacks rather
// than the single num_hidden_layers / hidden_size pair used by causal LMs.
type WhisperConfig struct {
	BaseModelConfig

	// Shared model dimensions
	DModel    int `json:"d_model"`
	VocabSize int `json:"vocab_size"`

	// Encoder dimensions
	EncoderLayers         int `json:"encoder_layers"`
	EncoderAttentionHeads int `json:"encoder_attention_heads"`
	EncoderFfnDim         int `json:"encoder_ffn_dim"`

	// Decoder dimensions
	DecoderLayers         int `json:"decoder_layers"`
	DecoderAttentionHeads int `json:"decoder_attention_heads"`
	DecoderFfnDim         int `json:"decoder_ffn_dim"`

	// Audio / position limits
	NumMelBins         int `json:"num_mel_bins"`
	MaxSourcePositions int `json:"max_source_positions"`
	MaxTargetPositions int `json:"max_target_positions"`

	// Special tokens
	BosTokenId          int `json:"bos_token_id"`
	EosTokenId          int `json:"eos_token_id"`
	PadTokenId          int `json:"pad_token_id"`
	DecoderStartTokenId int `json:"decoder_start_token_id"`
	ClassifierProjSize  int `json:"classifier_proj_size"`

	// Activation / regularization
	ActivationFunction string  `json:"activation_function"`
	ActivationDropout  float64 `json:"activation_dropout"`
	AttentionDropout   float64 `json:"attention_dropout"`
	Dropout            float64 `json:"dropout"`
	EncoderLayerdrop   float64 `json:"encoder_layerdrop"`
	DecoderLayerdrop   float64 `json:"decoder_layerdrop"`
	InitStd            float64 `json:"init_std"`

	// Misc options
	IsEncoderDecoder    bool `json:"is_encoder_decoder"`
	ScaleEmbedding      bool `json:"scale_embedding"`
	UseCache            bool `json:"use_cache"`
	UseWeightedLayerSum bool `json:"use_weighted_layer_sum"`
	NumHiddenLayers     int  `json:"num_hidden_layers"`
}

// LoadWhisperConfig loads a Whisper model configuration from a JSON file.
func LoadWhisperConfig(configPath string) (*WhisperConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Whisper config file '%s': %w", configPath, err)
	}

	var config WhisperConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse Whisper config JSON from '%s': %w", configPath, err)
	}

	config.ConfigPath = configPath

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Whisper configuration in '%s': %w", configPath, err)
	}

	return &config, nil
}

// Validate checks if the Whisper configuration is internally consistent.
func (c *WhisperConfig) Validate() error {
	if c.DModel <= 0 {
		return fmt.Errorf("d_model must be positive, got %d", c.DModel)
	}
	if c.EncoderLayers <= 0 {
		return fmt.Errorf("encoder_layers must be positive, got %d", c.EncoderLayers)
	}
	if c.DecoderLayers <= 0 {
		return fmt.Errorf("decoder_layers must be positive, got %d", c.DecoderLayers)
	}
	if c.EncoderAttentionHeads <= 0 {
		return fmt.Errorf("encoder_attention_heads must be positive, got %d", c.EncoderAttentionHeads)
	}
	if c.DecoderAttentionHeads <= 0 {
		return fmt.Errorf("decoder_attention_heads must be positive, got %d", c.DecoderAttentionHeads)
	}
	if c.VocabSize <= 0 {
		return fmt.Errorf("vocab_size must be positive, got %d", c.VocabSize)
	}
	if c.MaxTargetPositions <= 0 {
		return fmt.Errorf("max_target_positions must be positive, got %d", c.MaxTargetPositions)
	}
	if c.MaxSourcePositions <= 0 {
		return fmt.Errorf("max_source_positions must be positive, got %d", c.MaxSourcePositions)
	}
	return nil
}

// Implementation of the HuggingFaceModel interface

// GetParameterCount returns the total number of parameters in the model.
// It first tries to read the precise count from accompanying safetensors
// files, and falls back to a hard-coded value for the well-known Whisper
// checkpoints.
func (c *WhisperConfig) GetParameterCount() int64 {
	count, err := FindAndParseSafetensors(c.ConfigPath)
	if err == nil {
		return count
	}

	fmt.Printf("Warning: failed to get parameter count from safetensors: %v\n", err)

	// Hard-coded counts for known OpenAI Whisper checkpoints. Whisper sizes
	// are determined by (encoder_layers, decoder_layers, d_model).
	switch {
	case c.EncoderLayers == 32 && c.DecoderLayers == 4 && c.DModel == 1280:
		return 809_000_000 // whisper-large-v3-turbo (~809M)
	case c.EncoderLayers == 32 && c.DecoderLayers == 32 && c.DModel == 1280:
		return 1_550_000_000 // whisper-large / large-v2 / large-v3 (~1.55B)
	case c.EncoderLayers == 24 && c.DecoderLayers == 24 && c.DModel == 1024:
		return 769_000_000 // whisper-medium (~769M)
	case c.EncoderLayers == 12 && c.DecoderLayers == 12 && c.DModel == 768:
		return 244_000_000 // whisper-small (~244M)
	case c.EncoderLayers == 6 && c.DecoderLayers == 6 && c.DModel == 512:
		return 74_000_000 // whisper-base (~74M)
	case c.EncoderLayers == 4 && c.DecoderLayers == 4 && c.DModel == 384:
		return 39_000_000 // whisper-tiny (~39M)
	}

	return 0
}

// GetTransformerVersion returns the transformers library version.
func (c *WhisperConfig) GetTransformerVersion() string {
	return c.TransformerVersion
}

// GetQuantizationType returns the quantization method used (if any).
func (c *WhisperConfig) GetQuantizationType() string {
	return ""
}

// GetArchitecture returns the model architecture.
func (c *WhisperConfig) GetArchitecture() string {
	if len(c.Architectures) > 0 {
		return c.Architectures[0]
	}
	return "WhisperForConditionalGeneration"
}

// GetModelType returns the model type.
func (c *WhisperConfig) GetModelType() string {
	return c.ModelType
}

// GetContextLength returns the maximum context length.
//
// For Whisper this is the decoder token budget (max_target_positions, 448
// for every published OpenAI checkpoint), which is what callers use when
// sizing requests against the OpenAI-compatible API.
func (c *WhisperConfig) GetContextLength() int {
	return c.MaxTargetPositions
}

// GetModelSizeBytes returns the estimated size of the model in bytes.
func (c *WhisperConfig) GetModelSizeBytes() int64 {
	return EstimateModelSizeBytes(c.GetParameterCount(), c.GetTorchDtype())
}

// GetTorchDtype returns the torch data type used by the model.
func (c *WhisperConfig) GetTorchDtype() string {
	return c.TorchDtype
}

// HasVision returns false. Whisper is an audio model, not a vision model.
func (c *WhisperConfig) HasVision() bool {
	return false
}

// IsEmbedding returns false since Whisper is a generative ASR model.
func (c *WhisperConfig) IsEmbedding() bool {
	return false
}

// Register the Whisper model handler.
func init() {
	RegisterModelLoader("whisper", func(configPath string) (HuggingFaceModel, error) {
		return LoadWhisperConfig(configPath)
	})
}
