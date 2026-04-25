package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/client/clientset/versioned"
	"github.com/sgl-project/ome/pkg/hfutil/modelconfig"
)

const (
	// DefaultConfigFileName Default config file name used by Hugging Face models
	DefaultConfigFileName = "config.json"
	// DefaultModelIndexFileName Default model index file name used by diffusers models
	DefaultModelIndexFileName = "model_index.json"
)

// modelConfigLoader is a function type for loading model configurations
// This allows for easy mocking in tests
type modelConfigLoader func(configPath string) (modelconfig.HuggingFaceModel, error)

// ModelConfigParser is responsible for parsing model config files
// and updating the corresponding Model CRD
type ModelConfigParser struct {
	logger          *zap.SugaredLogger
	omeClient       versioned.Interface
	loadModelConfig modelConfigLoader // Function to load model configs
}

// NewModelConfigParser creates a new model config parser
func NewModelConfigParser(omeClient versioned.Interface, logger *zap.SugaredLogger) *ModelConfigParser {
	return &ModelConfigParser{
		logger:          logger,
		omeClient:       omeClient,
		loadModelConfig: modelconfig.LoadModelConfig, // Use the real implementation by default
	}
}

// ParseModelConfig reads model_index.json (if present) or config.json from the model directory and extracts metadata
// without updating any resources. This allows the caller to control when and how updates happen.
func (p *ModelConfigParser) ParseModelConfig(modelDir string, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) (*ModelMetadata, error) {
	p.logger.Infof("Parsing model config at: %s", modelDir)

	// Skip if the directory doesn't exist
	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		p.logger.Warnf("Model directory doesn't exist: %s", modelDir)
		return nil, nil
	}

	// Check if model should skip config parsing
	if shouldSkip := p.shouldSkipConfigParsing(baseModel, clusterBaseModel); shouldSkip {
		p.logger.Infof("Skipping config parsing due to annotation")
		return nil, nil
	}

	var metadata ModelMetadata
	var hasMetadata bool

	// Prefer model_index.json for diffusion models (skip config.json search if present)
	modelIndexPath, err := p.findModelIndexFile(modelDir)
	if err == nil {
		p.logger.Infof("Found model_index.json at: %s", modelIndexPath)
		hfModel, loadErr := p.loadModelConfig(modelIndexPath)
		if loadErr != nil {
			return nil, fmt.Errorf("failed to parse model_index.json with hf_model_config: %w", loadErr)
		}

		metadata = p.extractModelMetadataFromHF(hfModel)
		hasMetadata = true
	} else {
		p.logger.Infof("model_index.json not found: %v", err)

		// Look for the config.json file - it could be at the root level or a subdirectory
		configPath, findErr := p.findConfigFile(modelDir)
		if findErr != nil {
			p.logger.Infof("Config file not found: %v", findErr)
		} else {
			p.logger.Infof("Found config file at: %s", configPath)

			// Parse the config.json file using the hfutil.model_config module
			hfModel, loadErr := p.loadModelConfig(configPath)
			if loadErr != nil {
				return nil, fmt.Errorf("failed to parse config file with hf_model_config: %w", loadErr)
			}

			// Use the HuggingFaceModel interface to extract metadata
			metadata = p.extractModelMetadataFromHF(hfModel)
			hasMetadata = true
			p.logger.Infof("Extracted metadata: %+v", metadata)
		}
	}

	if !hasMetadata {
		return nil, fmt.Errorf("no model_index.json or config.json found in %s", modelDir)
	}

	// Update BaseModel or ClusterBaseModel if provided
	if baseModel != nil {
		if err := p.updateBaseModel(baseModel, metadata); err != nil {
			p.logger.Errorf("Failed to update BaseModel: %v", err)
			// Continue anyway to return metadata
		}
	} else if clusterBaseModel != nil {
		if err := p.updateClusterBaseModel(clusterBaseModel, metadata); err != nil {
			p.logger.Errorf("Failed to update ClusterBaseModel: %v", err)
			// Continue anyway to return metadata
		}
	}

	return &metadata, nil
}

// findConfigFile searches for the config.json file in the model directory
// It checks the root directory and common subdirectories
func (p *ModelConfigParser) findConfigFile(modelDir string) (string, error) {
	// Check the root directory first
	rootConfigPath := filepath.Join(modelDir, DefaultConfigFileName)
	if _, err := os.Stat(rootConfigPath); err == nil {
		return rootConfigPath, nil
	}

	// Common places where config.json might be located
	possiblePaths := []string{
		filepath.Join(modelDir, "safetensors", DefaultConfigFileName),
		filepath.Join(modelDir, "pytorch_model", DefaultConfigFileName),
		filepath.Join(modelDir, "model", DefaultConfigFileName),
	}

	// Check if config.json exists in any of the possible paths
	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}

	// If not found in common locations, do a recursive search (limited to avoid deep searching)
	var configPath string
	err := filepath.Walk(modelDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip error files
		}
		if !info.IsDir() && info.Name() == DefaultConfigFileName {
			configPath = path
			return filepath.SkipDir // Found it, stop searching
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	if configPath == "" {
		return "", fmt.Errorf("config.json not found in %s", modelDir)
	}
	return configPath, nil
}

// findModelIndexFile searches for the model_index.json file in the model directory
// It checks the root directory and common subdirectories
func (p *ModelConfigParser) findModelIndexFile(modelDir string) (string, error) {
	// Check the root directory first
	rootIndexPath := filepath.Join(modelDir, DefaultModelIndexFileName)
	if _, err := os.Stat(rootIndexPath); err == nil {
		return rootIndexPath, nil
	}

	// If not found in rootdir, do a recursive search (limited to avoid deep searching)
	var indexPath string
	err := filepath.Walk(modelDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip error files
		}
		if !info.IsDir() && info.Name() == DefaultModelIndexFileName {
			indexPath = path
			return filepath.SkipDir // Found it, stop searching
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	if indexPath == "" {
		return "", fmt.Errorf("model_index.json not found in %s", modelDir)
	}
	return indexPath, nil
}

// updateModel is a generic function to update either a BaseModel or ClusterBaseModel
func (p *ModelConfigParser) updateModel(model interface{}, metadata ModelMetadata) error {
	// Get model info for logging
	var modelInfo string
	var namespace, name string

	// Update model spec fields from the extracted metadata
	switch m := model.(type) {
	case *v1beta1.BaseModel:
		p.updateModelSpec(&m.Spec, metadata)
		namespace, name = m.Namespace, m.Name
		modelInfo = fmt.Sprintf("BaseModel %s/%s", namespace, name)
	case *v1beta1.ClusterBaseModel:
		p.updateModelSpec(&m.Spec, metadata)
		name = m.Name
		modelInfo = fmt.Sprintf("ClusterBaseModel %s", name)
	default:
		return fmt.Errorf("unsupported model type: %T", model)
	}

	p.logger.Infof("Updating %s with extracted metadata", modelInfo)

	// Update the model in Kubernetes
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest interface{}
		var err error

		// Get the latest version of the model
		switch model.(type) {
		case *v1beta1.BaseModel:
			latest, err = p.omeClient.OmeV1beta1().BaseModels(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		case *v1beta1.ClusterBaseModel:
			latest, err = p.omeClient.OmeV1beta1().ClusterBaseModels().Get(context.TODO(), name, metav1.GetOptions{})
		}

		if err != nil {
			return fmt.Errorf("failed to get latest model: %w", err)
		}

		// Update the spec with our changes (reapply them to the latest version)
		switch m := latest.(type) {
		case *v1beta1.BaseModel:
			p.updateModelSpec(&m.Spec, metadata)
		case *v1beta1.ClusterBaseModel:
			p.updateModelSpec(&m.Spec, metadata)
		}

		// Update the model
		switch m := latest.(type) {
		case *v1beta1.BaseModel:
			_, err = p.omeClient.OmeV1beta1().BaseModels(namespace).Update(context.TODO(), m, metav1.UpdateOptions{})
		case *v1beta1.ClusterBaseModel:
			_, err = p.omeClient.OmeV1beta1().ClusterBaseModels().Update(context.TODO(), m, metav1.UpdateOptions{})
		}

		if err != nil {
			return fmt.Errorf("failed to update model: %w", err)
		}

		p.logger.Debugf("Successfully updated %s", modelInfo)
		return nil
	})
}

// updateBaseModel updates the BaseModel CRD with information from the extracted metadata
func (p *ModelConfigParser) updateBaseModel(model *v1beta1.BaseModel, metadata ModelMetadata) error {
	return p.updateModel(model, metadata)
}

// updateClusterBaseModel updates the ClusterBaseModel CRD with information from the extracted metadata
func (p *ModelConfigParser) updateClusterBaseModel(model *v1beta1.ClusterBaseModel, metadata ModelMetadata) error {
	return p.updateModel(model, metadata)
}

// extractModelMetadataFromHF extracts relevant metadata using the HuggingFaceModel interface
func (p *ModelConfigParser) extractModelMetadataFromHF(hfModel modelconfig.HuggingFaceModel) ModelMetadata {
	p.logger.Infof("Extracting metadata from HuggingFace model: type=%s, architecture=%s",
		hfModel.GetModelType(), hfModel.GetArchitecture())

	var diffusionModel *modelconfig.DiffusionPipelineSpec
	if dm, ok := hfModel.(modelconfig.HuggingFaceDiffusionModel); ok {
		diffusionModel = dm.GetDiffusionModel()
	}
	isDiffusion := diffusionModel != nil
	modelType := hfModel.GetModelType()

	metadata := ModelMetadata{
		ModelType:          modelType,
		ModelArchitecture:  hfModel.GetArchitecture(),
		ModelParameterSize: modelconfig.FormatParamCount(hfModel.GetParameterCount()),
		MaxTokens:          int32(hfModel.GetContextLength()),
		ModelCapabilities:  p.determineModelCapabilitiesFromHF(hfModel),
	}

	if isDiffusion {
		metadata.ModelFormat = v1beta1.ModelFormat{
			Name:    "diffusers",
			Version: &diffusionModel.DiffusersVersion,
		}
		metadata.ModelFramework = &v1beta1.ModelFrameworkSpec{
			Name:    "diffusers",
			Version: &diffusionModel.DiffusersVersion,
		}
		metadata.DiffusionPipeline = convertDiffusionPipelineSpec(diffusionModel)
	} else {
		// Set the model format (most models use SafeTensors)
		version := "1.0.0"
		metadata.ModelFormat = v1beta1.ModelFormat{
			Name:    "safetensors",
			Version: &version,
		}

		metadata.ModelFramework = &v1beta1.ModelFrameworkSpec{
			Name: "transformers",
		}
	}

	// Set transformer version if available
	if !isDiffusion {
		transformerVersion := hfModel.GetTransformerVersion()
		if transformerVersion != "" {
			metadata.ModelFramework.Version = &transformerVersion
			p.logger.Infof("Setting transformer version: %s", transformerVersion)
		}
	}

	// Extract quantization information if available
	quantType := hfModel.GetQuantizationType()
	if quantType != "" {
		p.logger.Infof("Detected quantization type: %s", quantType)
		switch {
		case strings.Contains(strings.ToLower(quantType), "int4"):
			metadata.Quantization = v1beta1.ModelQuantizationINT4
			p.logger.Infof("Setting quantization to INT4")
		case strings.Contains(strings.ToLower(quantType), "fp8"):
			metadata.Quantization = v1beta1.ModelQuantizationFP8
			p.logger.Infof("Setting quantization to FP8")
		}
	}

	// Get the model size in bytes
	modelSizeBytes := hfModel.GetModelSizeBytes()
	if modelSizeBytes > 0 {
		p.logger.Infof("Model size in bytes: %d (%.2f GB)",
			modelSizeBytes, float64(modelSizeBytes)/1000000000.0)
	}

	// Get the raw JSON configuration for status
	configJSON, err := json.Marshal(struct {
		ModelType          string `json:"model_type"`
		Architecture       string `json:"architecture"`
		ContextLength      int    `json:"context_length"`
		ParameterCount     string `json:"parameter_count"`
		HasVision          bool   `json:"has_vision"`
		IsEmbedding        bool   `json:"is_embedding"`
		TransformerVersion string `json:"transformers_version"`
		TorchDtype         string `json:"torch_dtype"`
		ModelSizeBytes     int64  `json:"model_size_bytes"`
	}{
		ModelType:          hfModel.GetModelType(),
		Architecture:       hfModel.GetArchitecture(),
		ContextLength:      hfModel.GetContextLength(),
		ParameterCount:     modelconfig.FormatParamCount(hfModel.GetParameterCount()),
		HasVision:          hfModel.HasVision(),
		IsEmbedding:        hfModel.IsEmbedding(),
		TransformerVersion: hfModel.GetTransformerVersion(),
		TorchDtype:         hfModel.GetTorchDtype(),
		ModelSizeBytes:     modelSizeBytes,
	})
	if err == nil {
		metadata.ModelConfiguration = configJSON
	} else {
		p.logger.Warnf("Failed to marshal model configuration: %v", err)
	}

	p.logger.Infof("Extracted metadata: type=%s, architecture=%s, size=%s, maxTokens=%d, capabilities=%v",
		metadata.ModelType, metadata.ModelArchitecture, metadata.ModelParameterSize,
		metadata.MaxTokens, metadata.ModelCapabilities)

	return metadata
}

func (p *ModelConfigParser) updateModelSpec(spec *v1beta1.BaseModelSpec, metadata ModelMetadata) {
	p.logger.Info("Updating model spec with extracted metadata")

	// Use a helper function for updating fields
	updateField := func(current interface{}, new interface{}, fieldName string) bool {
		isUpdated := false
		switch c := current.(type) {
		case **string:
			if *c == nil && new != nil {
				val := new.(string)
				*c = &val
				isUpdated = true
			}
		case *[]string:
			if len(*c) == 0 && len(new.([]string)) > 0 {
				*c = new.([]string)
				isUpdated = true
			}
		case *v1beta1.ModelFormat:
			if c.Name == "" {
				*c = new.(v1beta1.ModelFormat)
				isUpdated = true
			}
		case **v1beta1.ModelFrameworkSpec:
			if *c == nil && new != nil {
				*c = new.(*v1beta1.ModelFrameworkSpec)
				isUpdated = true
			}
		case **v1beta1.DiffusionPipelineSpec:
			if *c == nil && new != nil {
				*c = new.(*v1beta1.DiffusionPipelineSpec)
				isUpdated = true
			}
		case **v1beta1.ModelQuantization:
			if *c == nil && new.(v1beta1.ModelQuantization) != "" {
				val := new.(v1beta1.ModelQuantization)
				*c = &val
				isUpdated = true
			}
		case *runtime.RawExtension:
			if new != nil && len(new.([]byte)) > 0 {
				c.Raw = new.([]byte)
				isUpdated = true
			}
		}

		if isUpdated {
			p.logger.Debugf("Setting %s: %v", fieldName, new)
		} else if current != nil {
			p.logger.Debugf("%s already set, not updating", fieldName)
		}

		return isUpdated
	}

	// Update each field using the helper function
	updateField(&spec.ModelType, metadata.ModelType, "ModelType")
	updateField(&spec.ModelArchitecture, metadata.ModelArchitecture, "ModelArchitecture")
	updateField(&spec.ModelFramework, metadata.ModelFramework, "ModelFramework")
	updateField(&spec.ModelFormat, metadata.ModelFormat, "ModelFormat")
	updateField(&spec.ModelParameterSize, metadata.ModelParameterSize, "ModelParameterSize")
	updateField(&spec.MaxTokens, metadata.MaxTokens, "MaxTokens")
	updateField(&spec.ModelCapabilities, metadata.ModelCapabilities, "ModelCapabilities")
	updateField(&spec.ApiCapabilities, metadata.ApiCapabilities, "ApiCapabilities")
	updateField(&spec.Quantization, metadata.Quantization, "Quantization")
	updateField(&spec.ModelConfiguration, metadata.ModelConfiguration, "ModelConfiguration")
	updateField(&spec.DiffusionPipeline, metadata.DiffusionPipeline, "DiffusionPipeline")

	p.logger.Info("Model spec update complete")
}

func convertDiffusionPipelineSpec(pipeline *modelconfig.DiffusionPipelineSpec) *v1beta1.DiffusionPipelineSpec {
	if pipeline == nil {
		return nil
	}

	spec := &v1beta1.DiffusionPipelineSpec{}
	if pipeline.ClassName != "" {
		className := pipeline.ClassName
		spec.ClassName = &className
	}

	spec.Scheduler = convertDiffusionComponent(pipeline.Scheduler)
	spec.TextEncoder = convertDiffusionComponent(pipeline.TextEncoder)
	spec.Tokenizer = convertDiffusionComponent(pipeline.Tokenizer)
	spec.Transformer = convertDiffusionComponent(pipeline.Transformer)
	spec.VAE = convertDiffusionComponent(pipeline.VAE)

	if len(pipeline.AdditionalComponents) > 0 {
		additional := make(map[string]v1beta1.DiffusionComponentSpec, len(pipeline.AdditionalComponents))
		for key, value := range pipeline.AdditionalComponents {
			additional[key] = v1beta1.DiffusionComponentSpec{Library: value.Library, Type: value.Type}
		}
		spec.AdditionalComponents = additional
	}

	return spec
}

func convertDiffusionComponent(component *modelconfig.DiffusionComponentSpec) *v1beta1.DiffusionComponentSpec {
	if component == nil {
		return nil
	}
	return &v1beta1.DiffusionComponentSpec{Library: component.Library, Type: component.Type}
}

// determineModelCapabilitiesFromHF determines the model capabilities based on the HuggingFaceModel
// shouldSkipConfigParsing checks if config parsing should be skipped for this model
func (p *ModelConfigParser) shouldSkipConfigParsing(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) bool {
	// Check base model annotations
	if baseModel != nil {
		if value, exists := baseModel.Annotations[ConfigParsingAnnotation]; exists {
			if strings.ToLower(value) == "true" {
				p.logger.Infof("Skipping config parsing for BaseModel %s/%s due to annotation", baseModel.Namespace, baseModel.Name)
				return true
			}
		}
	}

	// Check cluster base model annotations
	if clusterBaseModel != nil {
		if value, exists := clusterBaseModel.Annotations[ConfigParsingAnnotation]; exists {
			if strings.ToLower(value) == "true" {
				p.logger.Infof("Skipping config parsing for ClusterBaseModel %s due to annotation", clusterBaseModel.Name)
				return true
			}
		}
	}

	return false
}

func (p *ModelConfigParser) determineModelCapabilitiesFromHF(hfModel modelconfig.HuggingFaceModel) []string {
	var capabilities []string
	architecture := hfModel.GetArchitecture()
	modelType := hfModel.GetModelType()

	normalizedArchitecture := strings.ToLower(architecture)
	normalizedModelType := strings.ToLower(modelType)

	// Tested against 90+ models.
	if dm, ok := hfModel.(modelconfig.HuggingFaceDiffusionModel); ok {
		pipeline := dm.GetDiffusionModel()
		if pipeline == nil {
			return capabilities
		}
		if strings.Contains(normalizedArchitecture, "imageedit") ||
			strings.Contains(normalizedArchitecture, "pix2pix") ||
			strings.Contains(normalizedArchitecture, "img2img") ||
			strings.Contains(normalizedArchitecture, "inpaint") {
			return append(capabilities, string(v1beta1.ModelCapabilityImageTextToImage))
		}
		if strings.Contains(normalizedArchitecture, "image") ||
			strings.Contains(normalizedArchitecture, "pix") ||
			strings.Contains(normalizedArchitecture, "stablediffusion") {
			return append(capabilities, string(v1beta1.ModelCapabilityTextToImage))
		}
		if strings.Contains(normalizedArchitecture, "texttovideo") ||
			strings.Contains(normalizedArchitecture, "t2v") {
			return append(capabilities, string(v1beta1.ModelCapabilityTextToVideo))
		}
		if strings.Contains(normalizedArchitecture, "video") {
			return append(capabilities, string(v1beta1.ModelCapabilityImageTextToVideo))
		}
	}

	// For NemotronH_Nano capability
	if strings.Contains(normalizedModelType, "nemotronh_nano") {
		return append(capabilities, string(v1beta1.ModelCapabilityImageTextToText), string(v1beta1.ModelCapabilityTextToText),
			string(v1beta1.ModelCapabilityAudioToText))
	}

	// For vision, only support image text capability right now
	if hfModel.HasVision() {
		return append(capabilities, string(v1beta1.ModelCapabilityImageTextToText))
	}

	// Check for omni-model capability
	if strings.Contains(normalizedArchitecture, "omni") {
		return append(capabilities,
			string(v1beta1.ModelCapabilityTextToAudio), string(v1beta1.ModelCapabilityImageTextToAudio),
			string(v1beta1.ModelCapabilityVideoTextToAudio), string(v1beta1.ModelCapabilityAudioToText),
			string(v1beta1.ModelCapabilityAudioToAudio))
	}

	// Check for audio-to-text capability (ASR/transcription models e.g. Whisper, Wav2Vec2, HuBERT)
	if strings.Contains(normalizedArchitecture, "whisper") ||
		strings.Contains(normalizedArchitecture, "wav2vec2") ||
		strings.Contains(normalizedArchitecture, "hubert") ||
		strings.Contains(normalizedArchitecture, "fortc") ||
		strings.Contains(normalizedArchitecture, "forspeechtotext") {
		return append(capabilities, string(v1beta1.ModelCapabilityAudioToText))
	}

	// Check for text embedding capability
	if hfModel.IsEmbedding() ||
		strings.Contains(normalizedArchitecture, "embedding") ||
		strings.Contains(normalizedArchitecture, "sentence") ||
		strings.Contains(normalizedModelType, "bert") ||
		// Special case for known embedding models
		(strings.Contains(normalizedModelType, "mistral") &&
			strings.Contains(normalizedArchitecture, "mistralmodel")) {
		return append(capabilities, string(v1beta1.ModelCapabilityEmbedding))
	}

	// Default to text-to-text capability
	return append(capabilities, string(v1beta1.ModelCapabilityTextToText))
}

// populateArtifactAttribute returns a pointer to an updated copy of currentModelMetadata
// where the Artifact field is set to the provided artifact pointer
//
// Parameters:
//   - artifact: pointer to built artifact based on reusability
//   - currentModelMetadata: pointer of current model metadata
//
// Returns:
//   - *ModelMetadata: pointer to the updated metadata with Artifact populated
func (p *ModelConfigParser) populateArtifactAttribute(artifact *Artifact, currentModelMetadata *ModelMetadata) *ModelMetadata {
	if artifact != nil {
		currentModelMetadata.Artifact = *artifact
		p.logger.Infof("current artifact is :%s", currentModelMetadata.Artifact)
	}
	return currentModelMetadata
}

/*
buildArtifactAttribute constructs an Artifact instance from the provided SHA and parent information.

Fields set:
  - Sha: set to the provided commit SHA identifying the artifact version
  - ParentPath: a single-entry map {matchedParentName: parentPath} pointing to the parent artifact location
  - ChildrenPaths: initialized to an empty (non-nil) slice so it can be safely appended to later

Returns a pointer to the created Artifact.
*/
func (p *ModelConfigParser) buildArtifactAttribute(sha string, matchedParentName string, parentPath string, childrenPaths []string) *Artifact {
	artifact := Artifact{
		Sha:           sha,
		ParentPath:    map[string]string{matchedParentName: parentPath},
		ChildrenPaths: childrenPaths,
	}
	return &artifact
}
