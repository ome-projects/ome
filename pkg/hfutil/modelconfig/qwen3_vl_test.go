package modelconfig

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestQwen3VLConfig(t *testing.T) {
	configPath := filepath.Join("testdata", "qwen3_vl_235b.json")

	// Load the config
	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load Qwen3VL config: %v", err)
	}

	// Check that it's the correct model type
	if config.GetModelType() != "qwen3_vl_moe" {
		t.Errorf("Expected model type 'qwen3_vl_moe' but got '%s'", config.GetModelType())
	}

	// Check that it's parsed as a Qwen3Config
	qwen3VLConfig, ok := config.(*Qwen3VLConfig)
	textConfig := qwen3VLConfig.TextConfig
	if !ok {
		t.Fatalf("Expected config to be of type *Qwen3VLConfig, but got %T", config)
	}

	// Check key fields
	if textConfig.HiddenSize != 4096 {
		t.Errorf("Expected hidden size to be 4096, but got %d", textConfig.HiddenSize)
	}

	if textConfig.NumHiddenLayers != 94 {
		t.Errorf("Expected hidden layers to be 94, but got %d", textConfig.NumHiddenLayers)
	}

	if textConfig.NumAttentionHeads != 64 {
		t.Errorf("Expected attention heads to be 64, but got %d", textConfig.NumAttentionHeads)
	}

	if textConfig.NumKeyValueHeads != 4 {
		t.Errorf("Expected key-value heads to be 4, but got %d", textConfig.NumKeyValueHeads)
	}

	// Check context length (should use seq_length)
	contextLength := config.GetContextLength()
	expectedLength := 262144
	if contextLength != expectedLength {
		t.Errorf("Expected context length to be %d, but got %d", expectedLength, contextLength)
	}

	// Check parameter count (should be approximately 235B)
	paramCount := config.GetParameterCount()
	expectedCount := int64(235_000_000_000) // 235B parameters
	// Allow 5% tolerance for parameter estimation
	tolerance := expectedCount / 20 // 5% tolerance
	if paramCount < expectedCount-tolerance || paramCount > expectedCount+tolerance {
		t.Errorf("Expected parameter count to be around %d (±%d), but got %d", expectedCount, tolerance, paramCount)
	}

	// Check RoPE theta value (specific to Qwen3)
	if textConfig.RopeTheta != 5000000.0 {
		t.Errorf("Expected RoPE theta to be 5000000.0, but got %f", textConfig.RopeTheta)
	}

	// Test vision capability
	if !config.HasVision() {
		t.Errorf("Expected HasVision() to return true, got %v", config.HasVision())
	}

	// Check model size bytes (should be non-zero)
	modelSize := config.GetModelSizeBytes()
	if modelSize <= 0 {
		t.Errorf("Expected model size bytes to be positive, but got %d", modelSize)
	}
}

func TestQwen3VLConfig30B(t *testing.T) {
	configPath := filepath.Join("testdata", "qwen3_vl_30b_a3b_instruct.json")

	// Load the config
	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load Qwen3VL config: %v", err)
	}

	// Check that it's the correct model type
	if config.GetModelType() != "qwen3_vl_moe" {
		t.Errorf("Expected model type 'qwen3_vl_moe' but got '%s'", config.GetModelType())
	}

	// Check that it's parsed as a Qwen3Config
	qwen3VLConfig, ok := config.(*Qwen3VLConfig)
	textConfig := qwen3VLConfig.TextConfig
	if !ok {
		t.Fatalf("Expected config to be of type *Qwen3VLConfig, but got %T", config)
	}

	// Check key fields for 30B model
	if textConfig.HiddenSize != 2048 {
		t.Errorf("Expected hidden size to be 2048, but got %d", textConfig.HiddenSize)
	}

	if textConfig.NumHiddenLayers != 48 {
		t.Errorf("Expected hidden layers to be 48, but got %d", textConfig.NumHiddenLayers)
	}

	if textConfig.NumAttentionHeads != 32 {
		t.Errorf("Expected attention heads to be 32, but got %d", textConfig.NumAttentionHeads)
	}

	if textConfig.NumKeyValueHeads != 4 {
		t.Errorf("Expected key-value heads to be 4, but got %d", textConfig.NumKeyValueHeads)
	}

	// Check MoE configuration
	if textConfig.NumExperts != 128 {
		t.Errorf("Expected num experts to be 128, but got %d", textConfig.NumExperts)
	}

	if textConfig.MoeIntermediateSize != 768 {
		t.Errorf("Expected MoE intermediate size to be 768, but got %d", textConfig.MoeIntermediateSize)
	}

	if textConfig.NumExpertsPerTok != 8 {
		t.Errorf("Expected num experts per token to be 8, but got %d", textConfig.NumExpertsPerTok)
	}

	// Check context length
	contextLength := config.GetContextLength()
	expectedLength := 262144
	if contextLength != expectedLength {
		t.Errorf("Expected context length to be %d, but got %d", expectedLength, contextLength)
	}

	// Check RoPE theta value (specific to Qwen3)
	if textConfig.RopeTheta != 5000000.0 {
		t.Errorf("Expected RoPE theta to be 5000000.0, but got %f", textConfig.RopeTheta)
	}

	// Test vision capability
	if !config.HasVision() {
		t.Errorf("Expected HasVision() to return true, got %v", config.HasVision())
	}

	// Check model size bytes (should be non-zero)
	modelSize := config.GetModelSizeBytes()
	if modelSize <= 0 {
		t.Errorf("Expected model size bytes to be positive, but got %d", modelSize)
	}

	// Check parameter count
	paramCount := config.GetParameterCount()
	// Expected around 30B parameters with 10% tolerance
	expectedCount := int64(30_000_000_000) // 30B parameters
	tolerance := expectedCount / 20        // 5% tolerance
	if paramCount < expectedCount-tolerance || paramCount > expectedCount+tolerance {
		t.Errorf("Expected parameter count to be around %d (±%d), but got %d", expectedCount, tolerance, paramCount)
	}

	// Check vision config
	visionConfig := qwen3VLConfig.VisionConfig
	if visionConfig.HiddenSize != 1152 {
		t.Errorf("Expected vision hidden size to be 1152, but got %d", visionConfig.HiddenSize)
	}

	if visionConfig.Depth != 27 {
		t.Errorf("Expected vision depth to be 27, but got %d", visionConfig.Depth)
	}
}

func TestQwen3VLConfigNonMoE(t *testing.T) {
	configPath := filepath.Join("testdata", "qwen3_vl_2b_instruct.json")

	// Load the config
	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load Qwen3VL config: %v", err)
	}

	// Check that it's the correct model type
	if config.GetModelType() != "qwen3_vl" {
		t.Errorf("Expected model type 'qwen3_vl' but got '%s'", config.GetModelType())
	}

	// Check that it's parsed as a Qwen3Config
	qwen3VLConfig, ok := config.(*Qwen3VLConfig)
	textConfig := qwen3VLConfig.TextConfig
	if !ok {
		t.Fatalf("Expected config to be of type *Qwen3VLConfig, but got %T", config)
	}

	// Check key fields for non-MoE model
	if textConfig.HiddenSize != 2048 {
		t.Errorf("Expected hidden size to be 2048, but got %d", textConfig.HiddenSize)
	}

	if textConfig.NumHiddenLayers != 28 {
		t.Errorf("Expected hidden layers to be 28, but got %d", textConfig.NumHiddenLayers)
	}

	if textConfig.NumAttentionHeads != 16 {
		t.Errorf("Expected attention heads to be 16, but got %d", textConfig.NumAttentionHeads)
	}

	if textConfig.NumKeyValueHeads != 8 {
		t.Errorf("Expected key-value heads to be 8, but got %d", textConfig.NumKeyValueHeads)
	}

	// Check that it's not a MoE model
	if textConfig.NumExperts != 0 {
		t.Errorf("Expected num experts to be 0 for non-MoE model, but got %d", textConfig.NumExperts)
	}

	// Check context length
	contextLength := config.GetContextLength()
	expectedLength := 262144
	if contextLength != expectedLength {
		t.Errorf("Expected context length to be %d, but got %d", expectedLength, contextLength)
	}

	// Check RoPE theta value (specific to Qwen3)
	if textConfig.RopeTheta != 5000000.0 {
		t.Errorf("Expected RoPE theta to be 5000000.0, but got %f", textConfig.RopeTheta)
	}

	// Test vision capability
	if !config.HasVision() {
		t.Errorf("Expected HasVision() to return true, got %v", config.HasVision())
	}

	// Check model size bytes (should be non-zero)
	modelSize := config.GetModelSizeBytes()
	if modelSize <= 0 {
		t.Errorf("Expected model size bytes to be positive, but got %d", modelSize)
	}

	// Check parameter count
	paramCount := config.GetParameterCount()
	// For 2B model, expect around 2B parameters with 20% tolerance
	expectedCount := int64(2_000_000_000) // 2B parameters
	tolerance := expectedCount / 20       // 5% tolerance
	if paramCount < expectedCount-tolerance || paramCount > expectedCount+tolerance {
		t.Errorf("Expected parameter count to be around %d (±%d), but got %d", expectedCount, tolerance, paramCount)
	}
}

func TestQwen3VLQuantizationConfig(t *testing.T) {
	jsonData := []byte(`{
		"architectures": ["Qwen3VLForConditionalGeneration"],
		"model_type": "qwen3_vl",
		"image_token_id": 151655,
		"video_token_id": 151656,
		"vision_start_token_id": 151652,
		"vision_end_token_id": 151653,
		"tie_word_embeddings": false,
		"text_config": {
			"hidden_size": 2048,
			"intermediate_size": 11008,
			"num_hidden_layers": 28,
			"num_attention_heads": 16,
			"num_key_value_heads": 8,
			"max_position_embeddings": 262144,
			"vocab_size": 151936,
			"rope_theta": 5000000.0
		},
		"vision_config": {
			"hidden_size": 1152,
			"depth": 27,
			"num_heads": 16,
			"intermediate_size": 4304
		},
		"quantization_config": {
			"activation_scheme": "dynamic",
			"fmt": "e4m3",
			"quant_method": "fp8",
			"weight_block_size": [128, 128]
		}
	}`)

	config := &Qwen3VLConfig{}
	if err := json.Unmarshal(jsonData, config); err != nil {
		t.Fatalf("Failed to unmarshal Qwen3VL FP8 config: %v", err)
	}

	if config.QuantizationConfig == nil {
		t.Fatal("Expected QuantizationConfig to be non-nil")
	}

	if config.GetQuantizationType() != "fp8" {
		t.Errorf("Expected quantization type 'fp8', but got '%s'", config.GetQuantizationType())
	}

	if config.QuantizationConfig.Format != "e4m3" {
		t.Errorf("Expected format 'e4m3', but got '%s'", config.QuantizationConfig.Format)
	}
}

func TestQwen35ConfigDense(t *testing.T) {
	configPath := filepath.Join("testdata", "qwen3_5_27b.json")

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load Qwen3.5 dense config: %v", err)
	}

	if config.GetModelType() != "qwen3_5" {
		t.Errorf("Expected model type 'qwen3_5' but got '%s'", config.GetModelType())
	}

	qwenConfig, ok := config.(*Qwen3VLConfig)
	if !ok {
		t.Fatalf("Expected config to be of type *Qwen3VLConfig, but got %T", config)
	}

	if !config.HasVision() {
		t.Error("Expected HasVision to return true for Qwen3.5 dense model")
	}

	if qwenConfig.TextConfig.HiddenSize != 5120 {
		t.Errorf("Expected hidden size 5120, got %d", qwenConfig.TextConfig.HiddenSize)
	}

	if context := config.GetContextLength(); context != 262144 {
		t.Errorf("Expected context length 262144, got %d", context)
	}

	if params := config.GetParameterCount(); params <= 0 {
		t.Errorf("Expected positive parameter count, got %d", params)
	}

	if qwenConfig.GetTorchDtype() != "bfloat16" {
		t.Errorf("Expected torch dtype 'bfloat16', got '%s'", qwenConfig.GetTorchDtype())
	}

	paramCount := config.GetParameterCount()
	expectedModelSize := EstimateModelSizeBytes(paramCount, "bfloat16")
	if modelSize := config.GetModelSizeBytes(); modelSize != expectedModelSize {
		t.Errorf("Expected model size bytes %d, got %d", expectedModelSize, modelSize)
	}
}

func TestQwen35ConfigMoE(t *testing.T) {
	configPath := filepath.Join("testdata", "qwen3_5_35b_a3b.json")

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load Qwen3.5 MoE config: %v", err)
	}

	if config.GetModelType() != "qwen3_5_moe" {
		t.Errorf("Expected model type 'qwen3_5_moe' but got '%s'", config.GetModelType())
	}

	qwenConfig, ok := config.(*Qwen3VLConfig)
	if !ok {
		t.Fatalf("Expected config to be of type *Qwen3VLConfig, but got %T", config)
	}

	if !config.HasVision() {
		t.Error("Expected HasVision to return true for Qwen3.5 MoE model")
	}

	if qwenConfig.TextConfig.NumExperts != 256 {
		t.Errorf("Expected num experts 256, got %d", qwenConfig.TextConfig.NumExperts)
	}

	if qwenConfig.TextConfig.MoeIntermediateSize != 512 {
		t.Errorf("Expected moe_intermediate_size 512, got %d", qwenConfig.TextConfig.MoeIntermediateSize)
	}

	if context := config.GetContextLength(); context != 262144 {
		t.Errorf("Expected context length 262144, got %d", context)
	}

	if params := config.GetParameterCount(); params <= 0 {
		t.Errorf("Expected positive parameter count, got %d", params)
	}

	if qwenConfig.GetTorchDtype() != "bfloat16" {
		t.Errorf("Expected torch dtype 'bfloat16', got '%s'", qwenConfig.GetTorchDtype())
	}

	paramCount := config.GetParameterCount()
	expectedModelSize := EstimateModelSizeBytes(paramCount, "bfloat16")
	if modelSize := config.GetModelSizeBytes(); modelSize != expectedModelSize {
		t.Errorf("Expected model size bytes %d, got %d", expectedModelSize, modelSize)
	}
}
