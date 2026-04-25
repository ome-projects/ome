package modelagent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/client/clientset/versioned"
	"github.com/sgl-project/ome/pkg/hfutil/modelconfig"
)

// mockHuggingFaceModel implements the modelconfig.HuggingFaceModel interface for testing
type mockHuggingFaceModel struct {
	modelType          string
	architecture       string
	parameterCount     int64
	contextLength      int
	transformerVersion string
	quantizationType   string
	torchDtype         string
	modelSizeBytes     int64
	hasVision          bool
	isEmbedding        bool
}

type mockDiffusionModel struct {
	mockHuggingFaceModel
	diffusionModel *modelconfig.DiffusionPipelineSpec
}

func (m *mockDiffusionModel) GetDiffusionModel() *modelconfig.DiffusionPipelineSpec {
	return m.diffusionModel
}

// Implement all methods of the HuggingFaceModel interface
func (m *mockHuggingFaceModel) GetModelType() string          { return m.modelType }
func (m *mockHuggingFaceModel) GetArchitecture() string       { return m.architecture }
func (m *mockHuggingFaceModel) GetParameterCount() int64      { return m.parameterCount }
func (m *mockHuggingFaceModel) GetContextLength() int         { return m.contextLength }
func (m *mockHuggingFaceModel) GetTransformerVersion() string { return m.transformerVersion }
func (m *mockHuggingFaceModel) GetQuantizationType() string   { return m.quantizationType }
func (m *mockHuggingFaceModel) GetTorchDtype() string         { return m.torchDtype }
func (m *mockHuggingFaceModel) GetModelSizeBytes() int64      { return m.modelSizeBytes }
func (m *mockHuggingFaceModel) HasVision() bool               { return m.hasVision }
func (m *mockHuggingFaceModel) IsEmbedding() bool             { return m.isEmbedding }

// Define a helper function to create a mock model with default values
func createDefaultMockModel() *mockHuggingFaceModel {
	return &mockHuggingFaceModel{
		modelType:          "llama",
		architecture:       "LlamaForCausalLM",
		parameterCount:     7000000000, // 7B
		contextLength:      4096,
		transformerVersion: "4.33.2",
		quantizationType:   "",
		torchDtype:         "float16",
		modelSizeBytes:     14000000000, // 14GB
		hasVision:          false,
		isEmbedding:        false,
	}
}

// TestExtractModelMetadataFromHF tests the extractModelMetadataFromHF function
func TestExtractModelMetadataFromHF(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer func() { _ = logger.Sync() }()

	// Create a ModelConfigParser with the logger
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Define test cases
	testCases := []struct {
		name                 string
		mockModel            *mockHuggingFaceModel
		expectedMetadata     func(metadata ModelMetadata) bool
		expectedCapability   string
		expectedQuantization v1beta1.ModelQuantization
	}{
		{
			name:      "Standard LLM Model",
			mockModel: createDefaultMockModel(),
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "llama" &&
					metadata.ModelArchitecture == "LlamaForCausalLM" &&
					metadata.ModelParameterSize == "7B" &&
					metadata.MaxTokens == 4096 &&
					metadata.ModelFramework.Name == "transformers" &&
					*metadata.ModelFramework.Version == "4.33.2" &&
					metadata.ModelFormat.Name == "safetensors" &&
					*metadata.ModelFormat.Version == "1.0.0"
			},
			expectedCapability:   string(v1beta1.ModelCapabilityTextToText),
			expectedQuantization: "", // No quantization
		},
		{
			name: "INT4 Quantized Model",
			mockModel: &mockHuggingFaceModel{
				modelType:          "mixtral",
				architecture:       "MixtralForCausalLM",
				parameterCount:     8000000000, // 8B
				contextLength:      32768,
				transformerVersion: "4.35.0",
				quantizationType:   "gptq_int4",
				torchDtype:         "int4",
				modelSizeBytes:     4000000000, // 4GB (reduced due to quantization)
				hasVision:          false,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "mixtral" &&
					metadata.ModelArchitecture == "MixtralForCausalLM" &&
					metadata.ModelParameterSize == "8B" &&
					metadata.MaxTokens == 32768
			},
			expectedCapability:   string(v1beta1.ModelCapabilityTextToText),
			expectedQuantization: v1beta1.ModelQuantizationINT4,
		},
		{
			name: "FP8 Quantized Model",
			mockModel: &mockHuggingFaceModel{
				modelType:          "phi",
				architecture:       "PhiForCausalLM",
				parameterCount:     2800000000, // 2.8B
				contextLength:      2048,
				transformerVersion: "4.34.1",
				quantizationType:   "fp8-e4m3",
				torchDtype:         "float8",
				modelSizeBytes:     6000000000,
				hasVision:          false,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "phi" &&
					metadata.ModelArchitecture == "PhiForCausalLM" &&
					metadata.ModelParameterSize == "2.8B"
			},
			expectedCapability:   string(v1beta1.ModelCapabilityTextToText),
			expectedQuantization: v1beta1.ModelQuantizationFP8,
		},
		{
			name: "Vision Model",
			mockModel: &mockHuggingFaceModel{
				modelType:          "clip",
				architecture:       "CLIPModel",
				parameterCount:     400000000, // 400M
				contextLength:      77,        // CLIP typically uses smaller context
				transformerVersion: "4.32.0",
				quantizationType:   "",
				torchDtype:         "float16",
				modelSizeBytes:     1500000000,
				hasVision:          true,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "clip" &&
					metadata.ModelArchitecture == "CLIPModel" &&
					metadata.ModelParameterSize == "400M"
			},
			expectedCapability:   string(v1beta1.ModelCapabilityImageTextToText),
			expectedQuantization: "",
		},
		{
			name: "Missing Transformer Version",
			mockModel: &mockHuggingFaceModel{
				modelType:          "bert",
				architecture:       "BertModel",
				parameterCount:     110000000, // 110M
				contextLength:      512,
				transformerVersion: "", // Missing transformer version
				quantizationType:   "",
				torchDtype:         "float32",
				modelSizeBytes:     440000000,
				hasVision:          false,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "bert" &&
					metadata.ModelArchitecture == "BertModel" &&
					metadata.ModelFramework.Name == "transformers" &&
					metadata.ModelFramework.Version == nil // Should be nil when missing
			},
			expectedCapability:   string(v1beta1.ModelCapabilityEmbedding),
			expectedQuantization: "",
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function under test
			metadata := parser.extractModelMetadataFromHF(tc.mockModel)

			// Verify the metadata using the custom validation function
			if !tc.expectedMetadata(metadata) {
				t.Errorf("Metadata does not match expected values for test case %s", tc.name)
			}

			// Check specific expected capabilities
			hasCapability := false
			for _, cap := range metadata.ModelCapabilities {
				if cap == tc.expectedCapability {
					hasCapability = true
					break
				}
			}
			assert.True(t, hasCapability, "Expected capability %s not found", tc.expectedCapability)

			// Check quantization
			assert.Equal(t, tc.expectedQuantization, metadata.Quantization,
				"Quantization mismatch for test case %s", tc.name)

			// Check that ModelConfiguration JSON is valid and contains the expected fields
			if len(metadata.ModelConfiguration) > 0 {
				var configData map[string]interface{}
				err := json.Unmarshal(metadata.ModelConfiguration, &configData)
				assert.NoError(t, err, "Failed to unmarshal ModelConfiguration JSON")

				assert.Equal(t, tc.mockModel.GetModelType(), configData["model_type"],
					"model_type mismatch in ModelConfiguration")
				assert.Equal(t, tc.mockModel.GetArchitecture(), configData["architecture"],
					"architecture mismatch in ModelConfiguration")
				assert.Equal(t, tc.mockModel.GetContextLength(), int(configData["context_length"].(float64)),
					"context_length mismatch in ModelConfiguration")
				assert.Equal(t, tc.mockModel.HasVision(), configData["has_vision"],
					"has_vision mismatch in ModelConfiguration")
			}
		})
	}
}

// TestDetermineModelCapabilitiesFromHF tests the determineModelCapabilitiesFromHF function
func TestDetermineModelCapabilitiesFromHF(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer func() { _ = logger.Sync() }()

	// Create a ModelConfigParser with the logger
	parser := &ModelConfigParser{
		logger: sugar,
	}

	testCases := []struct {
		name                 string
		mockModel            modelconfig.HuggingFaceModel
		expectedCapabilities []string
	}{
		{
			name: "Text Generation Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "llama",
				architecture: "LlamaForCausalLM",
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityTextToText)},
		},
		{
			name: "Vision Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "clip",
				architecture: "CLIPModel",
				hasVision:    true,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityImageTextToText)},
		},
		{
			name: "Text Embedding Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "bert",
				architecture: "BertModel",
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityEmbedding)},
		},
		{
			name: "Sentence Transformer Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "sentence-transformer",
				architecture: "SentenceTransformerModel",
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityEmbedding)},
		},
		{
			name: "Special Case Mistral Embedding Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "mistral",
				architecture: "MistralModel", // Not MistralForCausalLM
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityEmbedding)},
		},
		{
			name: "Vision-capable LLM",
			mockModel: &mockHuggingFaceModel{
				modelType:    "gemma",
				architecture: "GemmaForCausalLM",
				hasVision:    true,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityImageTextToText)},
		},
		{
			name: "Diffusion Model",
			mockModel: &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "StableDiffusionPipeline",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{ClassName: "StableDiffusionPipeline"},
			},
			expectedCapabilities: []string{
				string(v1beta1.ModelCapabilityTextToImage),
			},
		},
		{
			name: "Diffusion QwenImagePipeline",
			mockModel: &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "QwenImagePipeline",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{ClassName: "QwenImagePipeline"},
			},
			expectedCapabilities: []string{
				string(v1beta1.ModelCapabilityTextToImage),
			},
		},
		{
			name: "Diffusion QwenImageEditPlus",
			mockModel: &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "QwenImageEditPlus",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{ClassName: "QwenImageEditPlus"},
			},
			expectedCapabilities: []string{
				string(v1beta1.ModelCapabilityImageTextToImage),
			},
		},
		{
			name: "Transformer Qwen3 Omni",
			mockModel: &mockHuggingFaceModel{
				modelType:    "qwen",
				architecture: "Qwen3OmniMoeForConditionalGeneration",
				hasVision:    false,
			},
			expectedCapabilities: []string{
				string(v1beta1.ModelCapabilityTextToAudio),
				string(v1beta1.ModelCapabilityImageTextToAudio),
				string(v1beta1.ModelCapabilityVideoTextToAudio),
				string(v1beta1.ModelCapabilityAudioToText),
				string(v1beta1.ModelCapabilityAudioToAudio),
			},
		},
		{
			name: "NemotronH_Nano Omni Model - falls through to vision due to case mismatch",
			mockModel: &mockHuggingFaceModel{
				modelType:    "NemotronH_Nano_Omni_Reasoning_V3",
				architecture: "NemotronH_Nano_Omni_Reasoning_V3",
				hasVision:    true,
			},
			expectedCapabilities: []string{
				string(v1beta1.ModelCapabilityImageTextToText),
				string(v1beta1.ModelCapabilityTextToText),
				string(v1beta1.ModelCapabilityAudioToText),
			},
		},
		{
			name: "Whisper ASR Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "whisper",
				architecture: "WhisperForConditionalGeneration",
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityAudioToText)},
		},
		{
			name: "Wav2Vec2 ASR Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "wav2vec2",
				architecture: "Wav2Vec2ForCTC",
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityAudioToText)},
		},
		{
			name: "HuBERT ASR Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "hubert",
				architecture: "HubertForCTC",
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityAudioToText)},
		},
		{
			name: "Generic ForSpeechToText Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "seamless_m4t",
				architecture: "SeamlessM4TForSpeechToText",
				hasVision:    false,
			},
			expectedCapabilities: []string{string(v1beta1.ModelCapabilityAudioToText)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capabilities := parser.determineModelCapabilitiesFromHF(tc.mockModel)

			// Check that the result has the expected length
			assert.Equal(t, len(tc.expectedCapabilities), len(capabilities),
				"Number of capabilities doesn't match expected for test case %s", tc.name)

			// All expected capabilities should be present
			for _, expectedCap := range tc.expectedCapabilities {
				found := false
				for _, actualCap := range capabilities {
					if actualCap == expectedCap {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected capability %s not found in %v for test case %s",
					expectedCap, capabilities, tc.name)
			}
		})
	}
}

// Helper function to find test for the direct internal functions
// TestNewModelConfigParser tests the constructor
func TestNewModelConfigParser(t *testing.T) {
	// Create dependencies
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Test with nil client
	parser := NewModelConfigParser(nil, sugar)
	assert.NotNil(t, parser)
	assert.Nil(t, parser.omeClient)
	assert.Equal(t, sugar, parser.logger)

	// Test with a client
	client := &versioned.Clientset{}
	parser = NewModelConfigParser(client, sugar)
	assert.NotNil(t, parser)
	assert.Equal(t, client, parser.omeClient)
}

// TestFindConfigFile tests the findConfigFile method
func TestFindConfigFile(t *testing.T) {
	// Create a temp directory structure for testing
	tempDir, err := os.MkdirTemp("", "config-test")
	assert.NoError(t, err)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create parser
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Case 1: Config in root directory
	configPath := filepath.Join(tempDir, DefaultConfigFileName)
	_, err = os.Create(configPath)
	assert.NoError(t, err)

	resultPath, err := parser.findConfigFile(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, configPath, resultPath)

	// Case 2: Config in model subdirectory
	_ = os.Remove(configPath) // Remove from root
	modelDir := filepath.Join(tempDir, "model")
	err = os.Mkdir(modelDir, 0755)
	assert.NoError(t, err)
	configPath = filepath.Join(modelDir, DefaultConfigFileName)
	_, err = os.Create(configPath)
	assert.NoError(t, err)

	resultPath, err = parser.findConfigFile(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, configPath, resultPath)

	// Case 3: No config file
	_ = os.RemoveAll(tempDir)
	err = os.Mkdir(tempDir, 0755)
	assert.NoError(t, err)

	_, err = parser.findConfigFile(tempDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config.json not found in")
}

// TestShouldSkipConfigParsing tests the shouldSkipConfigParsing method
func TestShouldSkipConfigParsing(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create parser
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Case 1: BaseModel with skip annotation
	baseModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-base-model",
			Annotations: map[string]string{
				ConfigParsingAnnotation: "true",
			},
		},
	}

	result := parser.shouldSkipConfigParsing(baseModel, nil)
	assert.True(t, result)

	// Case 2: BaseModel without skip annotation
	baseModel.Annotations = map[string]string{}
	result = parser.shouldSkipConfigParsing(baseModel, nil)
	assert.False(t, result)

	// Case 3: ClusterBaseModel with skip annotation
	clusterBaseModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster-base-model",
			Annotations: map[string]string{
				ConfigParsingAnnotation: "true",
			},
		},
	}

	result = parser.shouldSkipConfigParsing(nil, clusterBaseModel)
	assert.True(t, result)

	// Case 4: ClusterBaseModel without skip annotation
	clusterBaseModel.Annotations = map[string]string{}
	result = parser.shouldSkipConfigParsing(nil, clusterBaseModel)
	assert.False(t, result)

	// Case 5: Case insensitive "TRUE" value
	baseModel.Annotations = map[string]string{
		ConfigParsingAnnotation: "TRUE",
	}
	result = parser.shouldSkipConfigParsing(baseModel, nil)
	assert.True(t, result)
}

// TestUpdateModelSpec tests the updateModelSpec method which only updates nil fields
func TestUpdateModelSpec(t *testing.T) {
	// Create a simple logger
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	// Create basic metadata with just string fields
	metadata := ModelMetadata{
		ModelType:          "llama",
		ModelArchitecture:  "LlamaForCausalLM",
		ModelParameterSize: "7B",
		ModelCapabilities:  []string{string(v1beta1.ModelCapabilityTextToText)},
	}

	// Basic test: Empty spec gets fields set
	emptySpec := &v1beta1.BaseModelSpec{}
	parser.updateModelSpec(emptySpec, metadata)

	// Verify only string pointer fields that are guaranteed to be set
	assert.NotNil(t, emptySpec.ModelType)
	assert.Equal(t, "llama", *emptySpec.ModelType)

	assert.NotNil(t, emptySpec.ModelArchitecture)
	assert.Equal(t, "LlamaForCausalLM", *emptySpec.ModelArchitecture)

	assert.NotNil(t, emptySpec.ModelParameterSize)
	assert.Equal(t, "7B", *emptySpec.ModelParameterSize)

	// Verify slice
	assert.Equal(t, []string{string(v1beta1.ModelCapabilityTextToText)}, emptySpec.ModelCapabilities)

	// Test that existing values aren't overwritten
	existingType := "something-else"
	existingArch := "different-arch"
	existingSpec := &v1beta1.BaseModelSpec{
		ModelType:         &existingType,
		ModelArchitecture: &existingArch,
		ModelCapabilities: []string{"EXISTING_CAP"},
	}

	parser.updateModelSpec(existingSpec, metadata)

	// Existing values shouldn't be overwritten
	assert.Equal(t, "something-else", *existingSpec.ModelType)
	assert.Equal(t, "different-arch", *existingSpec.ModelArchitecture)
	assert.Equal(t, []string{"EXISTING_CAP"}, existingSpec.ModelCapabilities)

	// But nil values should be populated
	assert.NotNil(t, existingSpec.ModelParameterSize)
	assert.Equal(t, "7B", *existingSpec.ModelParameterSize)
}

func TestParseDiffusionPipelineSpec(t *testing.T) {
	data := []byte(`{
  "_class_name": "StableDiffusionPipeline",
  "_diffusers_version": "0.24.0",
  "scheduler": ["diffusers", "EulerDiscreteScheduler"],
  "text_encoder": ["transformers", "CLIPTextModel"],
  "tokenizer": ["transformers", "CLIPTokenizer"],
  "unet": ["diffusers", "UNet2DConditionModel"],
  "vae": ["diffusers", "AutoencoderKL"],
  "safety_checker": ["diffusers", "StableDiffusionSafetyChecker"]
}`)

	parsed, err := modelconfig.ParseDiffusionPipelineSpec(data)
	assert.NoError(t, err)
	pipeline := convertDiffusionPipelineSpec(parsed)
	if assert.NotNil(t, pipeline) {
		if assert.NotNil(t, pipeline.ClassName) {
			assert.Equal(t, "StableDiffusionPipeline", *pipeline.ClassName)
		}
		if assert.NotNil(t, pipeline.Scheduler) {
			assert.Equal(t, "diffusers", pipeline.Scheduler.Library)
			assert.Equal(t, "EulerDiscreteScheduler", pipeline.Scheduler.Type)
		}
		if assert.NotNil(t, pipeline.TextEncoder) {
			assert.Equal(t, "transformers", pipeline.TextEncoder.Library)
			assert.Equal(t, "CLIPTextModel", pipeline.TextEncoder.Type)
		}
		if assert.NotNil(t, pipeline.Tokenizer) {
			assert.Equal(t, "transformers", pipeline.Tokenizer.Library)
			assert.Equal(t, "CLIPTokenizer", pipeline.Tokenizer.Type)
		}
		if assert.NotNil(t, pipeline.Transformer) {
			assert.Equal(t, "diffusers", pipeline.Transformer.Library)
			assert.Equal(t, "UNet2DConditionModel", pipeline.Transformer.Type)
		}
		if assert.NotNil(t, pipeline.VAE) {
			assert.Equal(t, "diffusers", pipeline.VAE.Library)
			assert.Equal(t, "AutoencoderKL", pipeline.VAE.Type)
		}
		if assert.NotNil(t, pipeline.AdditionalComponents) {
			component, ok := pipeline.AdditionalComponents["safety_checker"]
			assert.True(t, ok)
			assert.Equal(t, "diffusers", component.Library)
			assert.Equal(t, "StableDiffusionSafetyChecker", component.Type)
		}
	}
}

// TestParseModelConfig tests part of the ParseModelConfig method logic,
// focusing on the parts that don't rely on the modelconfig.LoadModelConfig function
func TestParseModelConfig(t *testing.T) {
	// Create a temp directory structure for testing
	tempDir, err := os.MkdirTemp("", "model-config-test")
	assert.NoError(t, err)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create parser
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Create config.json
	configDir := filepath.Join(tempDir, "model")
	err = os.MkdirAll(configDir, 0755)
	assert.NoError(t, err)
	configPath := filepath.Join(configDir, DefaultConfigFileName)
	configFile, err := os.Create(configPath)
	assert.NoError(t, err)
	_, _ = configFile.WriteString("{\"model_type\": \"llama\", \"architectures\": [\"LlamaForCausalLM\"]}\n")
	_ = configFile.Close()

	// Case 1: Non-existent directory
	metadata, err := parser.ParseModelConfig("/non-existent", nil, nil)
	assert.Nil(t, err)
	assert.Nil(t, metadata)

	// Case 2: Skip config parsing due to annotation
	baseModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				ConfigParsingAnnotation: "true",
			},
		},
	}
	metadata, err = parser.ParseModelConfig(tempDir, baseModel, nil)
	assert.Nil(t, err)
	assert.Nil(t, metadata)

	// Note: We can't fully test cases 3 and 4 without mocking the private loadModelConfig field,
	// but we can at least verify the shouldSkipConfigParsing functionality
}

func TestParseModelConfig_PrefersModelIndex(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "model-index-test")
	assert.NoError(t, err)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	modelIndex := []byte(`{
  "_class_name": "StableDiffusionPipeline",
  "scheduler": ["diffusers", "EulerDiscreteScheduler"]
}`)
	modelIndexPath := filepath.Join(tempDir, DefaultModelIndexFileName)
	err = os.WriteFile(modelIndexPath, modelIndex, 0644)
	assert.NoError(t, err)

	vaeDir := filepath.Join(tempDir, "vae")
	err = os.MkdirAll(vaeDir, 0755)
	assert.NoError(t, err)
	configPath := filepath.Join(vaeDir, DefaultConfigFileName)
	err = os.WriteFile(configPath, []byte(`{"model_type":"vae","architectures":["AutoencoderKL"]}`), 0644)
	assert.NoError(t, err)

	loadCalled := false
	parser := &ModelConfigParser{
		logger: sugar,
		loadModelConfig: func(configPath string) (modelconfig.HuggingFaceModel, error) {
			loadCalled = true
			return &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "StableDiffusionPipeline",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{
					ClassName: "StableDiffusionPipeline",
					Scheduler: &modelconfig.DiffusionComponentSpec{Library: "diffusers", Type: "EulerDiscreteScheduler"},
				},
			}, nil
		},
	}

	metadata, parseErr := parser.ParseModelConfig(tempDir, nil, nil)
	assert.NoError(t, parseErr)
	if assert.NotNil(t, metadata) {
		assert.NotNil(t, metadata.DiffusionPipeline)
		if metadata.DiffusionPipeline != nil && metadata.DiffusionPipeline.ClassName != nil {
			assert.Equal(t, "StableDiffusionPipeline", *metadata.DiffusionPipeline.ClassName)
		}
		if assert.NotNil(t, metadata.ModelFramework) {
			assert.Equal(t, "diffusers", metadata.ModelFramework.Name)
		}
		assert.Equal(t, "diffusers", metadata.ModelFormat.Name)
	}
	assert.True(t, loadCalled, "loadModelConfig should be called when model_index.json is present")
}

func TestFormatParamCount(t *testing.T) {
	testCases := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1000000, "1M"},
		{1500000, "1.5M"},
		{10000000, "10M"},
		{1000000000, "1B"},
		{1500000000, "1.5B"},
		{7000000000, "7B"},
		{13000000000, "13B"},
		{70000000000, "70B"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := modelconfig.FormatParamCount(tc.input)
			assert.Equal(t, tc.expected, result, "FormatParamCount failed for input %d", tc.input)
		})
	}
}

func TestPopulateArtifactAttribute_SetsFields(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	orig := &ModelMetadata{} // empty input metadata
	sha := "abc123"
	parent := "/models/model1"
	artifact := &Artifact{
		Sha:        sha,
		ParentPath: map[string]string{"parentModel": parent},
	}

	out := parser.populateArtifactAttribute(artifact, orig)
	assert.NotNil(t, out, "returned metadata should not be nil")

	// Verify Artifact fields
	assert.Equal(t, sha, out.Artifact.Sha)
	assert.Equal(t, map[string]string{"parentModel": "/models/model1"}, out.Artifact.ParentPath)
	assert.Nil(t, out.Artifact.ChildrenPaths)
}

func TestPopulateArtifactAttribute_NilInput(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	// Prepare input with pre-filled artifact to ensure immutability of the input value
	original := &ModelMetadata{
		ModelType: "ClusterBaseModel",
	}

	out := parser.populateArtifactAttribute(nil, original)
	assert.Empty(t, out.Artifact.Sha)
	assert.Nil(t, out.Artifact.ParentPath)
	assert.Nil(t, out.Artifact.ChildrenPaths)
	assert.Equal(t, original, out, "no update should happen to original model moetadata")
}

func TestBuildArtifactAttribute_Basic(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	sha := "commit-sha-123"
	parentName := "clusterbasemodel.parentModel"
	parentPath := "/models/parent1"
	childrenPaths := []string{"/models/child1"}

	artifact := parser.buildArtifactAttribute(sha, parentName, parentPath, childrenPaths)
	if assert.NotNil(t, artifact, "artifact should not be nil") {
		assert.Equal(t, sha, artifact.Sha, "sha should match input")
		assert.Equal(t, map[string]string{parentName: parentPath}, artifact.ParentPath, "parentPath should be a single-entry map with provided key/value")
		// ChildrenPaths should be a non-nil empty slice to allow safe appends
		assert.NotNil(t, artifact.ChildrenPaths, "childrenPaths should be non-nil")
		assert.Equal(t, childrenPaths, artifact.ChildrenPaths, "childrenPaths should be same as input")
	}
}
