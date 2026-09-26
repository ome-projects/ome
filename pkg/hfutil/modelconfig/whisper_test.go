package modelconfig

import (
	"path/filepath"
	"testing"
)

func TestWhisperConfig(t *testing.T) {
	configPath := filepath.Join("testdata", "whisper_large_v3_turbo.json")

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load Whisper config: %v", err)
	}

	if config.GetModelType() != "whisper" {
		t.Errorf("Incorrect model type, expected 'whisper', got '%s'", config.GetModelType())
	}

	if config.GetArchitecture() != "WhisperForConditionalGeneration" {
		t.Errorf("Incorrect architecture, expected 'WhisperForConditionalGeneration', got '%s'", config.GetArchitecture())
	}

	whisperConfig, ok := config.(*WhisperConfig)
	if !ok {
		t.Fatalf("Failed to convert to WhisperConfig")
	}

	if whisperConfig.DModel != 1280 {
		t.Errorf("Incorrect d_model, expected 1280, got %d", whisperConfig.DModel)
	}

	if whisperConfig.EncoderLayers != 32 {
		t.Errorf("Incorrect encoder_layers, expected 32, got %d", whisperConfig.EncoderLayers)
	}

	if whisperConfig.DecoderLayers != 4 {
		t.Errorf("Incorrect decoder_layers, expected 4, got %d", whisperConfig.DecoderLayers)
	}

	if whisperConfig.NumMelBins != 128 {
		t.Errorf("Incorrect num_mel_bins, expected 128, got %d", whisperConfig.NumMelBins)
	}

	if whisperConfig.MaxSourcePositions != 1500 {
		t.Errorf("Incorrect max_source_positions, expected 1500, got %d", whisperConfig.MaxSourcePositions)
	}

	if whisperConfig.MaxTargetPositions != 448 {
		t.Errorf("Incorrect max_target_positions, expected 448, got %d", whisperConfig.MaxTargetPositions)
	}

	if whisperConfig.VocabSize != 51866 {
		t.Errorf("Incorrect vocab_size, expected 51866, got %d", whisperConfig.VocabSize)
	}

	if !whisperConfig.IsEncoderDecoder {
		t.Errorf("Expected is_encoder_decoder to be true")
	}

	// Context length should be the decoder token budget.
	if config.GetContextLength() != 448 {
		t.Errorf("Incorrect context length, expected 448, got %d", config.GetContextLength())
	}

	if config.GetTorchDtype() != "float16" {
		t.Errorf("Incorrect torch_dtype, expected 'float16', got '%s'", config.GetTorchDtype())
	}

	// whisper-large-v3-turbo has ~809M parameters.
	paramCount := config.GetParameterCount()
	expectedCount := int64(809_000_000)
	if paramCount != expectedCount {
		t.Errorf("Incorrect parameter count, expected %s, got %s",
			FormatParamCount(expectedCount), FormatParamCount(paramCount))
	}

	// float16 → 2 bytes per parameter.
	modelSize := config.GetModelSizeBytes()
	expectedSize := int64(809_000_000 * 2)
	if modelSize != expectedSize {
		t.Errorf("Incorrect model size, expected %s, got %s",
			FormatSize(expectedSize), FormatSize(modelSize))
	}

	if config.HasVision() {
		t.Errorf("Whisper should not report HasVision() == true")
	}
}

func TestLoadModelWithWhisper(t *testing.T) {
	configPath := filepath.Join("testdata", "whisper_large_v3_turbo.json")

	model, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load Whisper model through generic loader: %v", err)
	}

	if model.GetModelType() != "whisper" {
		t.Errorf("Expected model type 'whisper', got '%s'", model.GetModelType())
	}

	if model.GetContextLength() != 448 {
		t.Errorf("Expected context length 448, got %d", model.GetContextLength())
	}

	paramCount := model.GetParameterCount()
	expectedCount := int64(809_000_000)
	if paramCount != expectedCount {
		t.Errorf("Expected parameter count %s, got %s",
			FormatParamCount(expectedCount), FormatParamCount(paramCount))
	}

	t.Logf("Whisper model parameter count via generic loader: %s", FormatParamCount(paramCount))
}
