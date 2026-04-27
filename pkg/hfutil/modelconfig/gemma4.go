package modelconfig

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

// Gemma4Config defines the configuration for Google Gemma 4 multimodal models.
type Gemma4Config struct {
	BaseModelConfig

	// Gemma 4 configs (transformers 5.x) use top-level "dtype" instead of "torch_dtype".
	Dtype string `json:"dtype"`

	// Special tokens for multimodal support
	AudioTokenId  int         `json:"audio_token_id"`
	BoaTokenId    int         `json:"boa_token_id"`
	BoiTokenId    int         `json:"boi_token_id"`
	EoaTokenId    int         `json:"eoa_token_id"`
	EoaTokenIndex int         `json:"eoa_token_index"`
	EoiTokenId    int         `json:"eoi_token_id"`
	EosTokenId    interface{} `json:"eos_token_id"`
	ImageTokenId  int         `json:"image_token_id"`
	VideoTokenId  int         `json:"video_token_id"`

	InitializerRange         float64 `json:"initializer_range"`
	TieWordEmbeddings        bool    `json:"tie_word_embeddings"`
	VisionSoftTokensPerImage int     `json:"vision_soft_tokens_per_image"`

	TextConfig   Gemma4TextConfig   `json:"text_config"`
	VisionConfig Gemma4VisionConfig `json:"vision_config"`

	// AudioConfig is nil on text/vision-only variants (26B, 31B) and populated on E2B/E4B.
	AudioConfig *Gemma4AudioConfig `json:"audio_config"`

	QuantizationConfig *struct {
		QuantMethod string `json:"quant_method"`
	} `json:"quantization_config,omitempty"`
}

// Gemma4TextConfig defines the text/language model configuration for Gemma 4.
type Gemma4TextConfig struct {
	ModelType string `json:"model_type"`
	Dtype     string `json:"dtype"`

	HiddenSize            int `json:"hidden_size"`
	IntermediateSize      int `json:"intermediate_size"`
	NumHiddenLayers       int `json:"num_hidden_layers"`
	HeadDim               int `json:"head_dim"`
	NumAttentionHeads     int `json:"num_attention_heads"`
	NumKeyValueHeads      int `json:"num_key_value_heads"`
	MaxPositionEmbeddings int `json:"max_position_embeddings"`
	VocabSize             int `json:"vocab_size"`

	SlidingWindow           int `json:"sliding_window"`
	NumKvSharedLayers       int `json:"num_kv_shared_layers"`
	HiddenSizePerLayerInput int `json:"hidden_size_per_layer_input"`

	// MoE fields: populated on MoE variants (26B-A4B), null on dense variants.
	EnableMoeBlock      bool `json:"enable_moe_block"`
	NumExperts          *int `json:"num_experts"`
	TopKExperts         *int `json:"top_k_experts"`
	MoeIntermediateSize *int `json:"moe_intermediate_size"`
}

// Gemma4VisionConfig defines the vision tower configuration for Gemma 4.
type Gemma4VisionConfig struct {
	ModelType string `json:"model_type"`
	Dtype     string `json:"dtype"`

	HiddenSize        int `json:"hidden_size"`
	IntermediateSize  int `json:"intermediate_size"`
	NumHiddenLayers   int `json:"num_hidden_layers"`
	NumAttentionHeads int `json:"num_attention_heads"`
	PatchSize         int `json:"patch_size"`
}

// Gemma4AudioConfig defines the audio encoder configuration for Gemma 4.
type Gemma4AudioConfig struct {
	ModelType string `json:"model_type"`
	Dtype     string `json:"dtype"`

	HiddenSize        int `json:"hidden_size"`
	NumHiddenLayers   int `json:"num_hidden_layers"`
	NumAttentionHeads int `json:"num_attention_heads"`
}

// LoadGemma4Config loads a Gemma 4 model configuration from a JSON file.
func LoadGemma4Config(configPath string) (*Gemma4Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Gemma4 config file '%s': %w", configPath, err)
	}

	// Gemma 4 configs may contain Python-style Infinity/NaN values that stdlib json rejects.
	data = SanitizeJSONBytes(data)

	var config Gemma4Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse Gemma4 config JSON from '%s': %w", configPath, err)
	}

	// Promote top-level dtype to TorchDtype so size estimation stays correct.
	if config.TorchDtype == "" && config.Dtype != "" {
		config.TorchDtype = config.Dtype
	}

	config.ConfigPath = configPath
	return &config, nil
}

// GetParameterCount returns the total number of parameters in the model.
func (c *Gemma4Config) GetParameterCount() int64 {
	count, err := FindAndParseSafetensors(c.ConfigPath)
	if err == nil {
		return count
	}
	log.Printf("Warning: failed to get parameter count from safetensors: %v", err)

	tc := c.TextConfig

	switch {
	case tc.HiddenSize == 1536 && tc.NumHiddenLayers == 35:
		return 5_400_000_000 // Gemma 4 E2B (total params including PLE)
	case tc.HiddenSize == 2560 && tc.NumHiddenLayers == 42:
		return 8_000_000_000 // Gemma 4 E4B (total params including PLE)
	case tc.HiddenSize == 2816 && tc.NumHiddenLayers == 30 && tc.EnableMoeBlock:
		return 26_000_000_000 // Gemma 4 26B-A4B (MoE)
	case tc.HiddenSize == 5376 && tc.NumHiddenLayers == 60:
		return 31_000_000_000 // Gemma 4 31B
	}

	if tc.EnableMoeBlock && tc.NumExperts != nil && tc.TopKExperts != nil {
		return estimateMoEParamCount(tc.HiddenSize, tc.NumHiddenLayers, tc.IntermediateSize, *tc.NumExperts, *tc.TopKExperts)
	}

	textParams := estimateTextParams(tc.HiddenSize, tc.NumHiddenLayers, tc.IntermediateSize)
	visionParams := estimateVisionParams(c.VisionConfig.HiddenSize, c.VisionConfig.NumHiddenLayers, c.VisionConfig.IntermediateSize)
	return textParams + visionParams
}

// GetContextLength returns the maximum context length supported by the model.
func (c *Gemma4Config) GetContextLength() int {
	return c.TextConfig.MaxPositionEmbeddings
}

// GetModelSizeBytes returns the estimated size of the model in bytes.
func (c *Gemma4Config) GetModelSizeBytes() int64 {
	return EstimateModelSizeBytes(c.GetParameterCount(), c.TorchDtype)
}

// GetQuantizationType returns the quantization method used (if any).
func (c *Gemma4Config) GetQuantizationType() string {
	if c.QuantizationConfig == nil {
		return ""
	}
	if strings.Contains(strings.ToLower(c.QuantizationConfig.QuantMethod), "fp8") {
		return "fp8"
	}
	// Fail safe: any other non-nil quantization_config is treated as int4 so the matcher rejects it from unquantized runtimes.
	return "int4"
}

// HasVision returns true since all Gemma 4 variants ship a vision tower.
func (c *Gemma4Config) HasVision() bool {
	return true
}

// IsEmbedding returns false since this is not an embedding model.
func (c *Gemma4Config) IsEmbedding() bool {
	return false
}

func init() {
	RegisterModelLoader("gemma4", func(configPath string) (HuggingFaceModel, error) {
		return LoadGemma4Config(configPath)
	})
}
