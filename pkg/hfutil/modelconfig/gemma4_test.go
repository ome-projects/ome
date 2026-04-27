package modelconfig

import (
	"path/filepath"
	"testing"
)

func TestLoadGemma4Config(t *testing.T) {
	tests := []struct {
		name       string
		configFile string
	}{
		{"E2B", "gemma4_e2b.json"},
		{"E4B", "gemma4_e4b.json"},
		{"26B-A4B", "gemma4_26b_a4b.json"},
		{"31B", "gemma4_31b.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join("testdata", tt.configFile)
			config, err := LoadGemma4Config(configPath)
			if err != nil {
				t.Fatalf("Failed to load Gemma4 config: %v", err)
			}

			if config.GetModelType() != "gemma4" {
				t.Errorf("Expected model type 'gemma4', got '%s'", config.GetModelType())
			}
			if config.GetArchitecture() != "Gemma4ForConditionalGeneration" {
				t.Errorf("Expected architecture 'Gemma4ForConditionalGeneration', got '%s'", config.GetArchitecture())
			}
			if !config.HasVision() {
				t.Error("Expected HasVision to be true for all Gemma 4 variants")
			}
			if config.IsEmbedding() {
				t.Error("Expected IsEmbedding to be false")
			}
		})
	}
}

func TestGemma4ConfigContextLength(t *testing.T) {
	tests := []struct {
		name           string
		configFile     string
		expectedLength int
	}{
		{"E2B", "gemma4_e2b.json", 131072},
		{"E4B", "gemma4_e4b.json", 131072},
		{"26B-A4B", "gemma4_26b_a4b.json", 262144},
		{"31B", "gemma4_31b.json", 262144},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := LoadGemma4Config(filepath.Join("testdata", tt.configFile))
			if err != nil {
				t.Fatalf("Failed to load config: %v", err)
			}
			if got := config.GetContextLength(); got != tt.expectedLength {
				t.Errorf("Expected context length %d, got %d", tt.expectedLength, got)
			}
		})
	}
}

func TestGemma4ConfigAudioEncoderPresence(t *testing.T) {
	tests := []struct {
		name        string
		file        string
		wantPresent bool
	}{
		{"E2B ships audio encoder", "gemma4_e2b.json", true},
		{"E4B ships audio encoder", "gemma4_e4b.json", true},
		{"26B-A4B has no audio", "gemma4_26b_a4b.json", false},
		{"31B has no audio", "gemma4_31b.json", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := LoadGemma4Config(filepath.Join("testdata", tt.file))
			if err != nil {
				t.Fatalf("Failed to load config: %v", err)
			}
			gotPresent := config.AudioConfig != nil
			if gotPresent != tt.wantPresent {
				t.Errorf("Expected AudioConfig presence=%v, got %v", tt.wantPresent, gotPresent)
			}
		})
	}
}

func TestGemma4ConfigMoEFields(t *testing.T) {
	config, err := LoadGemma4Config(filepath.Join("testdata", "gemma4_26b_a4b.json"))
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if !config.TextConfig.EnableMoeBlock {
		t.Error("Expected EnableMoeBlock=true for 26B-A4B")
	}
	if config.TextConfig.NumExperts == nil || *config.TextConfig.NumExperts != 128 {
		t.Errorf("Expected NumExperts=128, got %v", config.TextConfig.NumExperts)
	}
	if config.TextConfig.TopKExperts == nil || *config.TextConfig.TopKExperts != 8 {
		t.Errorf("Expected TopKExperts=8, got %v", config.TextConfig.TopKExperts)
	}
	if config.TextConfig.MoeIntermediateSize == nil || *config.TextConfig.MoeIntermediateSize != 704 {
		t.Errorf("Expected MoeIntermediateSize=704, got %v", config.TextConfig.MoeIntermediateSize)
	}
}

func TestGemma4ConfigDenseHasNoMoE(t *testing.T) {
	for _, file := range []string{"gemma4_e2b.json", "gemma4_e4b.json", "gemma4_31b.json"} {
		t.Run(file, func(t *testing.T) {
			config, err := LoadGemma4Config(filepath.Join("testdata", file))
			if err != nil {
				t.Fatalf("Failed to load config: %v", err)
			}
			if config.TextConfig.EnableMoeBlock {
				t.Error("Expected EnableMoeBlock=false on dense variant")
			}
			if config.TextConfig.NumExperts != nil {
				t.Errorf("Expected NumExperts=nil on dense variant, got %v", *config.TextConfig.NumExperts)
			}
		})
	}
}

func TestGemma4ConfigGetTorchDtype(t *testing.T) {
	config, err := LoadGemma4Config(filepath.Join("testdata", "gemma4_e2b.json"))
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	if got := config.GetTorchDtype(); got != "bfloat16" {
		t.Errorf("Expected torch dtype 'bfloat16' (promoted from top-level dtype), got '%s'", got)
	}
}

func TestGemma4ConfigGetParameterCount(t *testing.T) {
	tests := []struct {
		name      string
		file      string
		minParams int64
		maxParams int64
	}{
		{"E2B", "gemma4_e2b.json", 2_000_000_000, 10_000_000_000},
		{"E4B", "gemma4_e4b.json", 4_000_000_000, 12_000_000_000},
		{"26B-A4B", "gemma4_26b_a4b.json", 20_000_000_000, 32_000_000_000},
		{"31B", "gemma4_31b.json", 25_000_000_000, 40_000_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := LoadGemma4Config(filepath.Join("testdata", tt.file))
			if err != nil {
				t.Fatalf("Failed to load config: %v", err)
			}
			got := config.GetParameterCount()
			if got < tt.minParams || got > tt.maxParams {
				t.Errorf("Expected param count in [%d, %d], got %d", tt.minParams, tt.maxParams, got)
			}
		})
	}
}

func TestGemma4ConfigRegisteredLoader(t *testing.T) {
	// LoadModelConfig dispatches on model_type; verify "gemma4" resolves to Gemma4Config.
	config, err := LoadModelConfig(filepath.Join("testdata", "gemma4_e2b.json"))
	if err != nil {
		t.Fatalf("LoadModelConfig failed: %v", err)
	}
	if _, ok := config.(*Gemma4Config); !ok {
		t.Errorf("Expected *Gemma4Config from LoadModelConfig, got %T", config)
	}
	if !config.HasVision() {
		t.Error("Expected HasVision=true via interface dispatch for E2B")
	}
}

func TestGemma4ConfigInterface(t *testing.T) {
	var _ HuggingFaceModel = (*Gemma4Config)(nil)
}
