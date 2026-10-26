package modelconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseModelConfigFromBytes(t *testing.T) {
	configPath := filepath.Join("testdata", "tiny-random-PhiModel", "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config fixture: %v", err)
	}

	model, err := ParseModelConfig(ModelConfigInput{
		Path: "config.json",
		Data: data,
	})
	if err != nil {
		t.Fatalf("ParseModelConfig returned error: %v", err)
	}

	if model.GetModelType() != "phi" {
		t.Fatalf("expected model type phi, got %q", model.GetModelType())
	}
	if model.GetArchitecture() != "PhiModel" {
		t.Fatalf("expected architecture PhiModel, got %q", model.GetArchitecture())
	}
}

func TestUnsupportedModelType(t *testing.T) {
	configPath := filepath.Join("testdata", "clip_vision_model.json")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skipf("Skipping test: file %s not found", configPath)
		return
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Expected no error when loading unsupported model type with generic fallback, but got: %v", err)
	}
	if config == nil {
		t.Fatalf("Expected a config but got nil")
	}

	modelType := config.GetModelType()
	if modelType != "clip_vision_model" {
		t.Errorf("Expected model type 'clip_vision_model' but got '%s'", modelType)
	}

	t.Logf("Generic config for unsupported model type: modelType=%s, params=%d, context=%d",
		config.GetModelType(), config.GetParameterCount(), config.GetContextLength())
}

func TestGenericConfigFallback(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// Config with common fields but a model_type the package has no
	// per-model knowledge of — exercises the generic resolver end-to-end.
	configJSON := `{
		"model_type": "falcon",
		"architectures": ["FalconForCausalLM"],
		"hidden_size": 4544,
		"num_hidden_layers": 32,
		"num_attention_heads": 71,
		"intermediate_size": 18176,
		"max_position_embeddings": 2048,
		"vocab_size": 65024,
		"torch_dtype": "bfloat16",
		"transformers_version": "4.30.0"
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load generic config: %v", err)
	}

	if config.GetModelType() != "falcon" {
		t.Errorf("Expected model type 'falcon', got '%s'", config.GetModelType())
	}
	if config.GetArchitecture() != "FalconForCausalLM" {
		t.Errorf("Expected architecture 'FalconForCausalLM', got '%s'", config.GetArchitecture())
	}
	if config.GetContextLength() != 2048 {
		t.Errorf("Expected context length 2048, got %d", config.GetContextLength())
	}
	if config.GetTorchDtype() != "bfloat16" {
		t.Errorf("Expected torch_dtype 'bfloat16', got '%s'", config.GetTorchDtype())
	}
	if config.GetTransformerVersion() != "4.30.0" {
		t.Errorf("Expected transformers_version '4.30.0', got '%s'", config.GetTransformerVersion())
	}

	paramCount := config.GetParameterCount()
	if paramCount <= 0 {
		t.Errorf("Expected positive parameter count from estimation, got %d", paramCount)
	}
	t.Logf("Estimated parameter count for Falcon: %s (%d)", FormatParamCount(paramCount), paramCount)

	modelSize := config.GetModelSizeBytes()
	if modelSize <= 0 {
		t.Errorf("Expected positive model size, got %d", modelSize)
	}
	t.Logf("Estimated model size: %s", FormatSize(modelSize))
}

func TestGenericMultimodalFallback(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// Nemotron-style config: top-level model_type with nested llm_config.
	configJSON := `{
		"architectures": ["Reasoning_V3"],
		"model_type": "Reasoning_V3",
		"torch_dtype": "bfloat16",
		"max_sequence_length": 131072,
		"llm_config": {
			"architectures": ["CausalLM"],
			"model_type": "multi_h",
			"transformers_version": "4.55.4",
			"torch_dtype": "bfloat16",
			"hidden_size": 2688,
			"num_hidden_layers": 52,
			"num_attention_heads": 32,
			"intermediate_size": 1856,
			"max_position_embeddings": 262144,
			"vocab_size": 131072,
			"n_routed_experts": 128,
			"n_shared_experts": 1,
			"moe_intermediate_size": 1856
		},
		"vision_config": {
			"architectures": ["RADIOModel"],
			"max_resolution": 2048
		},
		"sound_config": {
			"model_type": "parakeet",
			"hidden_size": 1024
		}
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetModelType() != "Reasoning_V3" {
		t.Errorf("Expected model type 'Reasoning_V3', got '%s'", config.GetModelType())
	}
	if config.GetTransformerVersion() != "4.55.4" {
		t.Errorf("Expected transformers_version '4.55.4', got '%s'", config.GetTransformerVersion())
	}
	// Nested probe of llm_config fills MaxPositionEmbeddings (262144),
	// which beats the top-level max_sequence_length (131072) per the
	// cascade order in GetContextLength.
	if config.GetContextLength() != 262144 {
		t.Errorf("Expected context length 262144, got %d", config.GetContextLength())
	}
	if !config.HasVision() {
		t.Error("Expected HasVision() to return true")
	}
	if config.IsEmbedding() {
		t.Error("Expected IsEmbedding() to return false")
	}
	// Parameter count should use MoE estimation (~68.8B), not dense (~4.86B).
	paramCount := config.GetParameterCount()
	if paramCount < 60_000_000_000 || paramCount > 80_000_000_000 {
		t.Errorf("Expected MoE parameter count in range [60B, 80B], got %d (%s)",
			paramCount, FormatParamCount(paramCount))
	}
	t.Logf("Estimated parameter count: %s (%d)", FormatParamCount(paramCount), paramCount)
}

func TestGenericMultimodalFallback_TextConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	configJSON := `{
		"architectures": ["SomeVisionModel"],
		"model_type": "some_vision_model",
		"text_config": {
			"hidden_size": 4096,
			"num_hidden_layers": 32,
			"intermediate_size": 11008,
			"max_position_embeddings": 4096,
			"vocab_size": 32000,
			"transformers_version": "4.40.0",
			"torch_dtype": "float16"
		},
		"vision_config": {
			"hidden_size": 1024,
			"num_hidden_layers": 24
		}
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetTransformerVersion() != "4.40.0" {
		t.Errorf("Expected transformers_version '4.40.0', got '%s'", config.GetTransformerVersion())
	}
	if config.GetContextLength() != 4096 {
		t.Errorf("Expected context length 4096, got %d", config.GetContextLength())
	}
	if config.GetTorchDtype() != "float16" {
		t.Errorf("Expected torch_dtype 'float16', got '%s'", config.GetTorchDtype())
	}
	if !config.HasVision() {
		t.Error("Expected HasVision() to return true")
	}
	if config.GetParameterCount() <= 0 {
		t.Error("Expected positive parameter count")
	}
}

// TestGenericMultimodalFallback_TextConfigModelMaxLength covers a
// multimodal model whose only context-length signal is a nested
// text_config.model_max_length (no top-level context field and no
// nested max_position_embeddings). probeNestedConfig must merge it up
// so GetContextLength's model_max_length cascade slot resolves.
func TestGenericMultimodalFallback_TextConfigModelMaxLength(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	configJSON := `{
		"architectures": ["InklingMMModel"],
		"model_type": "inkling_mm_model",
		"text_config": {
			"model_max_length": 1048576,
			"hidden_size": 6144,
			"num_hidden_layers": 66,
			"num_attention_heads": 64,
			"vocab_size": 201024,
			"torch_dtype": "bfloat16"
		}
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetContextLength() != 1048576 {
		t.Errorf("Expected context length 1048576 from nested text_config.model_max_length, got %d", config.GetContextLength())
	}
}

func TestGenericMultimodalFallback_LanguageConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	configJSON := `{
		"architectures": ["SomeVLModel"],
		"model_type": "some_vl_model",
		"language_config": {
			"hidden_size": 2048,
			"num_hidden_layers": 24,
			"intermediate_size": 5504,
			"max_position_embeddings": 4096,
			"vocab_size": 100000,
			"transformers_version": "4.38.0"
		},
		"vision_config": {
			"hidden_size": 1152
		}
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetTransformerVersion() != "4.38.0" {
		t.Errorf("Expected transformers_version '4.38.0', got '%s'", config.GetTransformerVersion())
	}
	if config.GetContextLength() != 4096 {
		t.Errorf("Expected context length 4096, got %d", config.GetContextLength())
	}
	if !config.HasVision() {
		t.Error("Expected HasVision() to return true")
	}
}

func TestGenericMoEFallback(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// Flat MoE config (Mixtral style) with the num_local_experts variant.
	configJSON := `{
		"architectures": ["SomeMoEModel"],
		"model_type": "some_moe",
		"hidden_size": 4096,
		"num_hidden_layers": 32,
		"intermediate_size": 14336,
		"max_position_embeddings": 32768,
		"vocab_size": 32000,
		"torch_dtype": "bfloat16",
		"transformers_version": "4.36.0",
		"num_local_experts": 8,
		"moe_intermediate_size": 14336
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Dense estimate would be ~12 * 4096^2 * 32 + embeddings ≈ 6.6B;
	// MoE estimate with 8 experts should be much higher.
	paramCount := config.GetParameterCount()
	if paramCount < 30_000_000_000 {
		t.Errorf("Expected MoE parameter count > 30B, got %d (%s)",
			paramCount, FormatParamCount(paramCount))
	}
	t.Logf("Estimated MoE parameter count: %s (%d)", FormatParamCount(paramCount), paramCount)
}

func TestGenericContextLengthMaxSequenceLength(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// max_sequence_length present but no max_position_embeddings.
	configJSON := `{
		"architectures": ["SomeModel"],
		"model_type": "some_model",
		"max_sequence_length": 131072,
		"hidden_size": 2048,
		"num_hidden_layers": 24,
		"vocab_size": 32000,
		"torch_dtype": "bfloat16",
		"transformers_version": "4.40.0"
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetContextLength() != 131072 {
		t.Errorf("Expected context length 131072 from max_sequence_length, got %d", config.GetContextLength())
	}
	if config.HasVision() {
		t.Error("Expected HasVision() to return false for non-multimodal config")
	}
}

// TestGenericContextLengthCascade pins down the order in which
// GenericModelConfig.GetContextLength resolves a context length:
// max_position_embeddings, then max_sequence_length, then seq_length
// (ChatGLM family), then model_max_length (Baichuan family). First
// non-zero wins. Nested LLM-config fallback is covered by
// probeNestedConfig and tested elsewhere.
func TestGenericContextLengthCascade(t *testing.T) {
	cases := []struct {
		name string
		json string
		want int
	}{
		{
			name: "max_position_embeddings wins when present",
			json: `{"model_type":"unknown","max_position_embeddings":4096,"max_sequence_length":2048,"seq_length":1024,"model_max_length":512}`,
			want: 4096,
		},
		{
			name: "falls back to max_sequence_length",
			json: `{"model_type":"unknown","max_sequence_length":2048,"seq_length":1024}`,
			want: 2048,
		},
		{
			name: "falls back to seq_length (ChatGLM)",
			json: `{"model_type":"unknown","seq_length":8192}`,
			want: 8192,
		},
		{
			name: "falls back to model_max_length (Baichuan)",
			json: `{"model_type":"unknown","model_max_length":4096}`,
			want: 4096,
		},
		{
			name: "all absent => zero",
			json: `{"model_type":"unknown","hidden_size":4096}`,
			want: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, err := ParseModelConfig(ModelConfigInput{
				Path: "config.json",
				Data: []byte(tc.json),
			})
			if err != nil {
				t.Fatalf("ParseModelConfig: %v", err)
			}
			if got := model.GetContextLength(); got != tc.want {
				t.Errorf("GetContextLength() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestGenericNestedDtypeRescue covers nested text_config.dtype being
// promoted to top-level TorchDtype when no top-level torch_dtype is
// present (qwen3_5 family stores dtype under text_config.dtype).
func TestGenericNestedDtypeRescue(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{
			name: "top-level torch_dtype wins",
			json: `{"model_type":"unknown","torch_dtype":"bfloat16","text_config":{"dtype":"float16"}}`,
			want: "bfloat16",
		},
		{
			name: "rescues nested text_config.dtype",
			json: `{"model_type":"unknown","text_config":{"dtype":"float16"}}`,
			want: "float16",
		},
		{
			name: "rescues nested text_config.torch_dtype too (existing)",
			json: `{"model_type":"unknown","text_config":{"torch_dtype":"float16"}}`,
			want: "float16",
		},
		{
			name: "neither present => empty",
			json: `{"model_type":"unknown","hidden_size":4096}`,
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, err := ParseModelConfig(ModelConfigInput{
				Path: "config.json",
				Data: []byte(tc.json),
			})
			if err != nil {
				t.Fatalf("ParseModelConfig: %v", err)
			}
			if got := model.GetTorchDtype(); got != tc.want {
				t.Errorf("GetTorchDtype() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestGenericVisionShapeDetection covers the shape signals the
// generic resolver looks for to flag a model as multimodal:
//
//   - vision_config block (most VL models)
//   - mm_vision_tower (LLaVA v1.5 style — pre-vision_config convention)
//   - image_token_id / image_token_index (Qwen2-VL, Qwen3-VL, Gemma3,
//     Llama4, MLlama, LLaVA-1.5-HF — usually paired with vision_config
//     but one of these alone is enough)
//
// Pure JSON-shape; no model_type allowlist.
func TestGenericVisionShapeDetection(t *testing.T) {
	cases := []struct {
		name string
		json string
		want bool
	}{
		// Unknown model_types so the generic resolver — not a per-model
		// parser — is what runs. These tests pin down GenericModelConfig's
		// HasVision behavior.
		{
			name: "vision_config present",
			json: `{"model_type":"unknown_vl","vision_config":{"hidden_size":1024}}`,
			want: true,
		},
		{
			name: "mm_vision_tower present (LLaVA v1.5 style)",
			json: `{"model_type":"unknown_mm","mm_vision_tower":"openai/clip-vit"}`,
			want: true,
		},
		{
			name: "image_token_id present (Qwen2-VL style)",
			json: `{"model_type":"unknown_qwen","image_token_id":151655}`,
			want: true,
		},
		{
			name: "image_token_index present (Gemma3 style)",
			json: `{"model_type":"unknown_gemma","image_token_index":262144}`,
			want: true,
		},
		{
			name: "img_processor present (Phi-3 Vision style)",
			json: `{"model_type":"unknown_phi","img_processor":{"name":"clip_vision_model"}}`,
			want: true,
		},
		{
			name: "no vision signals",
			json: `{"model_type":"unknown_text","hidden_size":4096}`,
			want: false,
		},
		{
			name: "unrelated mm_ field is not enough",
			json: `{"model_type":"unknown_text","mm_use_im_start_end":false}`,
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, err := ParseModelConfig(ModelConfigInput{
				Path: "config.json",
				Data: []byte(tc.json),
			})
			if err != nil {
				t.Fatalf("ParseModelConfig: %v", err)
			}
			if got := model.HasVision(); got != tc.want {
				t.Errorf("HasVision() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestQwen3EmbeddingClassifiesAsEmbedding covers decoder-based embedding
// models. Qwen3-Embedding's config.json is that of a Qwen3 causal LM;
// only the config_sentence_transformers.json beside it marks the
// checkpoint as an embedding model.
func TestQwen3EmbeddingClassifiesAsEmbedding(t *testing.T) {
	model, err := LoadModelConfig(filepath.Join("testdata", "Qwen3-Embedding-8B", "config.json"))
	if err != nil {
		t.Fatalf("LoadModelConfig: %v", err)
	}
	if got := model.GetArchitecture(); got != "Qwen3ForCausalLM" {
		t.Fatalf("architecture = %q, want Qwen3ForCausalLM", got)
	}
	if !model.IsEmbedding() {
		t.Error("IsEmbedding() = false, want true")
	}
	if got, want := model.GetCapabilities(), []Capability{CapabilityEmbedding}; !equalCaps(got, want) {
		t.Errorf("GetCapabilities() = %v, want %v", got, want)
	}
}

func TestGenericSentenceTransformersDetection(t *testing.T) {
	config := []byte(`{"model_type":"qwen3","architectures":["Qwen3ForCausalLM"],"max_position_embeddings":40960}`)
	cases := []struct {
		name    string
		sidecar string
		want    bool
	}{
		{
			name: "no sentence-transformers config",
			want: false,
		},
		{
			name:    "similarity function named",
			sidecar: `{"similarity_fn_name":"cosine"}`,
			want:    true,
		},
		{
			// sentence-transformers before 3.0 records no similarity
			// function; the architecture decides.
			name:    "no similarity function",
			sidecar: `{"__version__":{"sentence_transformers":"2.7.0"}}`,
			want:    false,
		},
		{
			name:    "null similarity function",
			sidecar: `{"similarity_fn_name":null}`,
			want:    false,
		},
		{
			name:    "malformed config is ignored",
			sidecar: `{`,
			want:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, err := ParseModelConfig(ModelConfigInput{
				Path:                       "config.json",
				Data:                       config,
				SentenceTransformersConfig: []byte(tc.sidecar),
			})
			if err != nil {
				t.Fatalf("ParseModelConfig: %v", err)
			}
			if got := model.IsEmbedding(); got != tc.want {
				t.Errorf("IsEmbedding() = %v, want %v", got, tc.want)
			}
		})
	}
}
