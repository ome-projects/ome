package modelparser

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/modelconfig"
)

const (
	// DefaultConfigFileName is the HuggingFace transformer model config filename.
	DefaultConfigFileName = "config.json"
	// DefaultModelIndexFileName is the diffusers pipeline manifest filename.
	DefaultModelIndexFileName = "model_index.json"

	// Model format / framework names emitted into BaseModel.Spec.
	formatNameSafetensors = "safetensors"
	formatNameDiffusers   = "diffusers"
	frameworkTransformers = "transformers"
	// safetensorsFormatVersion is the wire version we tag every
	// safetensors-backed BaseModel with. Bumped only when we change
	// how we encode metadata.ModelFormat.Version, not when upstream
	// safetensors changes — those bumps are transparent.
	safetensorsFormatVersion = "1.0.0"
)

// commonConfigSubdirs lists subdirectory names HuggingFace bundles
// commonly nest config.json (or hf_quant_config.json) under when the
// repository root holds metadata rather than the model itself.
var commonConfigSubdirs = []string{"safetensors", "pytorch_model", "model"}

// ModelConfigFileInput provides one file to a model config parse operation.
// Path must be relative to the logical model root and use slash separators.
type ModelConfigFileInput struct {
	Path string
	Data []byte
}

// modelConfigLoader loads model configurations from a path. Swappable
// for tests.
type modelConfigLoader func(configPath string) (modelconfig.HuggingFaceModel, error)

// ModelConfigParser parses model config files and applies the extracted
// metadata to a BaseModel or ClusterBaseModel spec in memory. It holds no
// API client: the caller that owns the object persists it.
type ModelConfigParser struct {
	logger          *zap.SugaredLogger
	loadModelConfig modelConfigLoader
}

// NewModelConfigParser creates a parser backed by modelconfig.LoadModelConfig.
func NewModelConfigParser(logger *zap.SugaredLogger) *ModelConfigParser {
	return &ModelConfigParser{
		logger:          logger,
		loadModelConfig: modelconfig.LoadModelConfig,
	}
}

// ParseModelConfig reads model_index.json (if present) or config.json
// from modelDir and extracts metadata. A BaseModel or ClusterBaseModel
// passed in (both may be nil) supplies the skip-parsing annotation and
// receives the metadata in its spec, in memory; callers persist it
// themselves, or apply the returned metadata with ApplyModelMetadata.
func (p *ModelConfigParser) ParseModelConfig(modelDir string, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) (*ModelMetadata, error) {
	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		p.logger.Warnw("model directory does not exist", "dir", modelDir)
		return nil, nil
	}
	if p.shouldSkipConfigParsing(baseModel, clusterBaseModel) {
		return nil, nil
	}

	standaloneQuantConfig := p.loadHFQuantConfig(modelDir)
	hfModel, modelConfigPath, err := p.loadModelConfigFromDir(modelDir)
	if err != nil {
		return nil, err
	}
	if hfModel == nil {
		return nil, fmt.Errorf("no model_index.json or config.json found in %s", modelDir)
	}
	quantConfig := resolveQuantConfig(standaloneQuantConfig, hfModel)
	parameterCount := p.quantAwareParameterCount(modelConfigPath, quantConfig)
	return p.finishParse(hfModel, quantConfig, parameterCount, baseModel, clusterBaseModel), nil
}

// ParseModelConfigFromFiles parses model metadata from an explicit set
// of in-memory files (path/data tuples), bypassing the local
// filesystem. Used by the basemodel reconciler's sharded backend, which
// streams config + safetensors headers from object storage.
func (p *ModelConfigParser) ParseModelConfigFromFiles(ctx context.Context, files []ModelConfigFileInput, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) (*ModelMetadata, error) {
	if p.shouldSkipConfigParsing(baseModel, clusterBaseModel) {
		return nil, nil
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("model config file input set is empty")
	}

	configInput, safetensorsFiles, hfQuantConfigBytes, err := splitModelConfigFileInputs(ctx, files)
	if err != nil {
		return nil, err
	}
	if configInput == nil {
		return nil, fmt.Errorf("no model_index.json or config.json found in file input set")
	}
	if path.Base(configInput.Path) == DefaultConfigFileName {
		// Transformer weights belong to the directory of config.json.
		// Diffusers model_index.json can span component directories.
		configDir := path.Dir(configInput.Path)
		localFiles := make([]modelconfig.SafetensorsFileInput, 0, len(safetensorsFiles))
		for _, file := range safetensorsFiles {
			if path.Dir(file.Path) == configDir {
				localFiles = append(localFiles, file)
			}
		}
		safetensorsFiles = localFiles
	}

	var standaloneQuantConfig *modelconfig.HFQuantConfig
	if len(hfQuantConfigBytes) > 0 {
		parsed, perr := modelconfig.ParseHFQuantConfig(hfQuantConfigBytes)
		if perr != nil {
			p.logger.Warnw("parse hf_quant_config.json failed", "error", perr)
		} else {
			standaloneQuantConfig = parsed
		}
	}

	hfModel, err := modelconfig.ParseModelConfig(*configInput)
	if err != nil {
		return nil, fmt.Errorf("parse model config %s: %w", configInput.Path, err)
	}
	quantConfig := resolveQuantConfig(standaloneQuantConfig, hfModel)

	var parameterCount *int64
	if len(safetensorsFiles) > 0 {
		count, perr := modelconfig.FindAndParseSafetensorsFilesWithQuantConfig(safetensorsFiles, quantConfig)
		if perr != nil {
			return nil, fmt.Errorf("parse safetensors metadata from file inputs: %w", perr)
		}
		parameterCount = &count
	}
	return p.finishParse(hfModel, quantConfig, parameterCount, baseModel, clusterBaseModel), nil
}

// resolveQuantConfig picks the one quantization config that both the
// parameter count and spec.Quantization read, so the two never disagree: the
// standalone hf_quant_config.json (ModelOpt) when present, else the
// config's embedded quantization_config block (mxfp4, fbgemm_fp8, fp8,
// gptq, awq), else nil.
func resolveQuantConfig(standalone *modelconfig.HFQuantConfig, hfModel modelconfig.HuggingFaceModel) *modelconfig.HFQuantConfig {
	if standalone != nil {
		return standalone
	}
	return hfModel.GetHFQuantConfig()
}

// quantAwareParameterCount counts the safetensors beside configPath with
// the quantization config applied; the config's own count under-counts
// packed-quant models and over-counts excluded modules. Nil when there is
// no quantization config or the files cannot be counted.
func (p *ModelConfigParser) quantAwareParameterCount(configPath string, quantConfig *modelconfig.HFQuantConfig) *int64 {
	if quantConfig == nil || configPath == "" {
		return nil
	}
	count, err := modelconfig.FindAndParseSafetensorsWithQuantConfig(configPath, quantConfig)
	if err != nil || count <= 0 {
		return nil
	}
	p.logger.Debugw("using quant-aware safetensors count",
		"count", count, "algo", quantConfig.Quantization.QuantAlgo)
	return &count
}

// finishParse turns a parsed config into the model's metadata, writes the
// quantization read from quantConfig and fills the unset spec fields of
// the model the caller passed. A non-nil parameterCount replaces the
// config's own count.
func (p *ModelConfigParser) finishParse(hfModel modelconfig.HuggingFaceModel, quantConfig *modelconfig.HFQuantConfig, parameterCount *int64, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) *ModelMetadata {
	metadata := p.extractModelMetadata(hfModel, parameterCount)
	applyQuantizationFromConfig(&metadata, quantConfig, p.logger)
	p.updateParsedModelMetadata(baseModel, clusterBaseModel, metadata)
	return &metadata
}

// applyQuantizationFromConfig is the only writer of metadata.Quantization:
// it maps the config's quant_algo to the OME enum. No-op for unquantized
// models or unrecognized algorithms.
func applyQuantizationFromConfig(metadata *ModelMetadata, hfQuantConfig *modelconfig.HFQuantConfig, logger *zap.SugaredLogger) {
	if hfQuantConfig == nil {
		return
	}
	algo := hfQuantConfig.Quantization.QuantAlgo
	if mapped := QuantAlgoToOMEEnum(algo); mapped != "" {
		metadata.Quantization = mapped
		return
	}
	if algo != "" {
		logger.Warnw("unrecognized quant_algo; leaving spec.Quantization unset", "quant_algo", algo)
	}
}

// loadModelConfigFromDir prefers model_index.json (diffusion) and
// falls back to config.json (HF text). Returns (nil, "", nil) if
// neither exists. The returned path anchors the safetensors counting
// helpers' directory lookup.
func (p *ModelConfigParser) loadModelConfigFromDir(modelDir string) (modelconfig.HuggingFaceModel, string, error) {
	if modelIndexPath, err := findFile(modelDir, DefaultModelIndexFileName, nil); err == nil {
		hfModel, loadErr := p.loadModelConfig(modelIndexPath)
		if loadErr != nil {
			return nil, "", fmt.Errorf("parse model_index.json at %s: %w", modelIndexPath, loadErr)
		}
		return hfModel, modelIndexPath, nil
	}
	configPath, findErr := findFile(modelDir, DefaultConfigFileName, commonConfigSubdirs)
	if findErr != nil {
		return nil, "", nil
	}
	hfModel, loadErr := p.loadModelConfig(configPath)
	if loadErr != nil {
		return nil, "", fmt.Errorf("parse config.json at %s: %w", configPath, loadErr)
	}
	return hfModel, configPath, nil
}

// updateParsedModelMetadata fills the unset spec fields of the model the
// caller passed, in memory. Persisting the object is the caller's job.
func (p *ModelConfigParser) updateParsedModelMetadata(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel, metadata ModelMetadata) {
	switch {
	case baseModel != nil:
		p.updateModelSpec(&baseModel.Spec, metadata)
	case clusterBaseModel != nil:
		p.updateModelSpec(&clusterBaseModel.Spec, metadata)
	}
}

// splitModelConfigFileInputs normalizes paths and sorts the file set
// (so iteration is deterministic across producers), then routes each
// file to the appropriate bucket: model_index.json → diffusion,
// config.json → transformer, hf_quant_config.json → standalone quant
// config, safetensors / .index.json → counting helper. When several
// folders hold a model_index.json, config.json or hf_quant_config.json,
// the one nearest the model root wins (see locationRank), so a
// sentence-transformers module's 1_Pooling/config.json does not shadow
// the model's own. A config_sentence_transformers.json beside the chosen
// config.json rides along with it. Files outside the recognized basename
// set are dropped silently.
func splitModelConfigFileInputs(ctx context.Context, files []ModelConfigFileInput) (*modelconfig.ModelConfigInput, []modelconfig.SafetensorsFileInput, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	normalized := make([]ModelConfigFileInput, 0, len(files))
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		relativePath, err := cleanInputFilePath(file.Path)
		if err != nil {
			return nil, nil, nil, err
		}
		normalized = append(normalized, ModelConfigFileInput{Path: relativePath, Data: file.Data})
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].Path < normalized[j].Path
	})

	var (
		modelIndexInput    *modelconfig.ModelConfigInput
		configInput        *modelconfig.ModelConfigInput
		hfQuantConfigPath  string
		hfQuantConfigBytes []byte
		safetensorsFiles   = make([]modelconfig.SafetensorsFileInput, 0)
	)
	for _, file := range normalized {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		if isSafetensorsFileInput(file.Path) {
			safetensorsFiles = append(safetensorsFiles, modelconfig.SafetensorsFileInput{Path: file.Path, Data: file.Data})
			continue
		}
		switch path.Base(file.Path) {
		case DefaultModelIndexFileName:
			if modelIndexInput == nil || locationRank(file.Path) < locationRank(modelIndexInput.Path) {
				input := modelconfig.ModelConfigInput{Path: file.Path, Data: file.Data}
				modelIndexInput = &input
			}
		case DefaultConfigFileName:
			if configInput == nil || locationRank(file.Path) < locationRank(configInput.Path) {
				input := modelconfig.ModelConfigInput{Path: file.Path, Data: file.Data}
				configInput = &input
			}
		case modelconfig.HFQuantConfigFileName:
			if hfQuantConfigPath == "" || locationRank(file.Path) < locationRank(hfQuantConfigPath) {
				hfQuantConfigPath, hfQuantConfigBytes = file.Path, file.Data
			}
		}
	}

	// model_index.json (diffusion) beats config.json — we prefer the
	// pipeline manifest when both exist.
	if modelIndexInput != nil {
		return modelIndexInput, safetensorsFiles, hfQuantConfigBytes, nil
	}
	if configInput != nil {
		stConfigPath := path.Join(path.Dir(configInput.Path), modelconfig.SentenceTransformersConfigFileName)
		for _, file := range normalized {
			if file.Path == stConfigPath {
				configInput.SentenceTransformersConfig = file.Data
				break
			}
		}
	}
	return configInput, safetensorsFiles, hfQuantConfigBytes, nil
}

// locationRank orders input paths that share a base name the way the
// directory path searches: the model root first, then commonConfigSubdirs
// in order (findInCommonLocations), then any other folder, shallowest
// first. Lower ranks win; on a tie the path that sorts first is kept.
func locationRank(filePath string) int {
	dir := path.Dir(filePath)
	if dir == "." {
		return 0
	}
	for i, sub := range commonConfigSubdirs {
		if dir == sub {
			return i + 1
		}
	}
	return len(commonConfigSubdirs) + strings.Count(filePath, "/")
}

func cleanInputFilePath(filePath string) (string, error) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return "", fmt.Errorf("model parser input file path is required")
	}
	filePath = strings.TrimLeft(filePath, "/")
	filePath = path.Clean(filePath)
	if filePath == "." || filePath == "" {
		return "", fmt.Errorf("model parser input file path is invalid: %q", filePath)
	}
	if filePath == ".." || strings.HasPrefix(filePath, "../") || filepath.IsAbs(filePath) {
		return "", fmt.Errorf("model parser input file path must stay under model root: %q", filePath)
	}
	return filePath, nil
}

func isSafetensorsFileInput(filePath string) bool {
	base := path.Base(filePath)
	return base == modelconfig.SafetensorsIndexFileName || strings.HasSuffix(base, modelconfig.SafetensorsExt)
}

// findFile returns the path of name under modelDir: the root first, then
// each of subdirs, then a recursive walk that ends at its first match in
// lexical order.
func findFile(modelDir, name string, subdirs []string) (string, error) {
	if found := findInRootOrSubdirs(modelDir, name, subdirs); found != "" {
		return found, nil
	}

	var found string
	err := filepath.Walk(modelDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && info.Name() == name {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("%s not found in %s", name, modelDir)
	}
	return found, nil
}

// findInRootOrSubdirs returns the first match of name directly under
// modelDir or under one of subdirs, in that order, or "" if none exists.
func findInRootOrSubdirs(modelDir, name string, subdirs []string) string {
	if path := filepath.Join(modelDir, name); fileExists(path) {
		return path
	}
	for _, sub := range subdirs {
		if path := filepath.Join(modelDir, sub, name); fileExists(path) {
			return path
		}
	}
	return ""
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// loadHFQuantConfig returns nil for both "absent" and "parse failed"
// so callers can use the optional-file pattern without branching.
func (p *ModelConfigParser) loadHFQuantConfig(modelDir string) *modelconfig.HFQuantConfig {
	path := findInRootOrSubdirs(modelDir, modelconfig.HFQuantConfigFileName, commonConfigSubdirs)
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		p.logger.Warnw("read hf_quant_config.json failed", "path", path, "error", err)
		return nil
	}
	cfg, err := modelconfig.ParseHFQuantConfig(data)
	if err != nil {
		p.logger.Warnw("parse hf_quant_config.json failed", "path", path, "error", err)
		return nil
	}
	return cfg
}

// extractModelMetadata reads the metadata of a parsed config. A non-nil
// parameterCountOverride replaces the config's own count and sizes the
// model from it. Quantization is left to applyQuantizationFromConfig.
func (p *ModelConfigParser) extractModelMetadata(hfModel modelconfig.HuggingFaceModel, parameterCountOverride *int64) ModelMetadata {
	var diffusionModel *modelconfig.DiffusionPipelineSpec
	if dm, ok := hfModel.(modelconfig.HuggingFaceDiffusionModel); ok {
		diffusionModel = dm.GetDiffusionModel()
	}

	parameterCount := hfModel.GetParameterCount()
	if parameterCountOverride != nil {
		parameterCount = *parameterCountOverride
	}

	metadata := ModelMetadata{
		ModelType:          hfModel.GetModelType(),
		ModelArchitecture:  hfModel.GetArchitecture(),
		ModelParameterSize: modelconfig.FormatParamCount(parameterCount),
		MaxTokens:          int32(hfModel.GetContextLength()),
		ModelCapabilities:  capabilitiesAsStrings(hfModel, p.logger),
	}
	p.populateFormatAndFramework(&metadata, hfModel, diffusionModel)

	modelSizeBytes := hfModel.GetModelSizeBytes()
	if parameterCountOverride != nil {
		modelSizeBytes = modelconfig.EstimateModelSizeBytes(parameterCount, hfModel.GetTorchDtype())
	}

	configJSON, err := marshalModelConfiguration(hfModel, parameterCount, modelSizeBytes)
	if err != nil {
		p.logger.Warnw("marshal model configuration failed", "error", err)
	} else {
		metadata.ModelConfiguration = configJSON
	}

	p.logger.Infow("extracted model metadata",
		"model_type", metadata.ModelType,
		"architecture", metadata.ModelArchitecture,
		"size", metadata.ModelParameterSize,
		"max_tokens", metadata.MaxTokens,
		"capabilities", metadata.ModelCapabilities)
	return metadata
}

// populateFormatAndFramework sets ModelFormat, ModelFramework, and (for
// diffusion models) DiffusionPipeline. Diffusion uses the diffusers
// library version end-to-end; transformer models default to
// safetensors + transformers, with the version overridden if the
// config reports one.
func (p *ModelConfigParser) populateFormatAndFramework(metadata *ModelMetadata, hfModel modelconfig.HuggingFaceModel, diffusionModel *modelconfig.DiffusionPipelineSpec) {
	if diffusionModel != nil {
		version := diffusionModel.DiffusersVersion
		metadata.ModelFormat = v1beta1.ModelFormat{Name: formatNameDiffusers, Version: &version}
		metadata.ModelFramework = &v1beta1.ModelFrameworkSpec{Name: formatNameDiffusers, Version: &version}
		metadata.DiffusionPipeline = convertDiffusionPipelineSpec(diffusionModel)
		return
	}

	version := safetensorsFormatVersion
	metadata.ModelFormat = v1beta1.ModelFormat{Name: formatNameSafetensors, Version: &version}
	metadata.ModelFramework = &v1beta1.ModelFrameworkSpec{Name: frameworkTransformers}
	if transformerVersion := hfModel.GetTransformerVersion(); transformerVersion != "" {
		metadata.ModelFramework.Version = &transformerVersion
	}
}

// modelConfigurationSnapshot is the JSON shape we serialize into
// BaseModel.Spec.ModelConfiguration. Keep field names stable — they're
// read by downstream tooling and consumed verbatim by the agent's
// status path.
type modelConfigurationSnapshot struct {
	ModelType          string `json:"model_type"`
	Architecture       string `json:"architecture"`
	ContextLength      int    `json:"context_length"`
	ParameterCount     string `json:"parameter_count"`
	HasVision          bool   `json:"has_vision"`
	IsEmbedding        bool   `json:"is_embedding"`
	TransformerVersion string `json:"transformers_version"`
	TorchDtype         string `json:"torch_dtype"`
	ModelSizeBytes     int64  `json:"model_size_bytes"`
}

func marshalModelConfiguration(hfModel modelconfig.HuggingFaceModel, parameterCount, modelSizeBytes int64) ([]byte, error) {
	return json.Marshal(modelConfigurationSnapshot{
		ModelType:          hfModel.GetModelType(),
		Architecture:       hfModel.GetArchitecture(),
		ContextLength:      hfModel.GetContextLength(),
		ParameterCount:     modelconfig.FormatParamCount(parameterCount),
		HasVision:          hfModel.HasVision(),
		IsEmbedding:        hfModel.IsEmbedding(),
		TransformerVersion: hfModel.GetTransformerVersion(),
		TorchDtype:         hfModel.GetTorchDtype(),
		ModelSizeBytes:     modelSizeBytes,
	})
}

// updateModelSpec merges parsed metadata into spec, only writing fields
// the operator left unset. Returns whether any field was changed. The
// "only fill what's nil" rule lets operators pin overrides in the CR
// and have the parser leave them alone on every reconcile.
func (p *ModelConfigParser) updateModelSpec(spec *v1beta1.BaseModelSpec, metadata ModelMetadata) bool {
	updated := false
	for _, set := range []func() bool{
		func() bool { return updateField(&spec.ModelType, metadata.ModelType) },
		func() bool { return updateField(&spec.ModelArchitecture, metadata.ModelArchitecture) },
		func() bool { return updateField(&spec.ModelFramework, metadata.ModelFramework) },
		func() bool { return updateField(&spec.ModelFormat, metadata.ModelFormat) },
		func() bool { return updateField(&spec.ModelParameterSize, metadata.ModelParameterSize) },
		func() bool { return updateField(&spec.MaxTokens, metadata.MaxTokens) },
		func() bool { return updateField(&spec.ModelCapabilities, metadata.ModelCapabilities) },
		func() bool { return updateField(&spec.ApiCapabilities, metadata.ApiCapabilities) },
		func() bool { return updateField(&spec.Quantization, metadata.Quantization) },
		func() bool { return updateField(&spec.ModelConfiguration, metadata.ModelConfiguration) },
		func() bool { return updateField(&spec.DiffusionPipeline, metadata.DiffusionPipeline) },
	} {
		if set() {
			updated = true
		}
	}
	return updated
}

// updateField writes new into current only when current is the zero
// value for its type (nil pointer, empty slice, empty struct) and new
// carries a value; a nil pointer in new is nothing to write. Returns
// whether the write happened. The type switch covers every pointer/slice
// kind that BaseModelSpec uses for parser-derived fields.
func updateField(current interface{}, new interface{}) bool {
	switch c := current.(type) {
	case **string:
		if *c == nil && new != nil {
			val := new.(string)
			*c = &val
			return true
		}
	case *[]string:
		if len(*c) == 0 && len(new.([]string)) > 0 {
			*c = new.([]string)
			return true
		}
	case *v1beta1.ModelFormat:
		if c.Name == "" {
			*c = new.(v1beta1.ModelFormat)
			return true
		}
	case **v1beta1.ModelFrameworkSpec:
		if framework, ok := new.(*v1beta1.ModelFrameworkSpec); ok && framework != nil && *c == nil {
			*c = framework
			return true
		}
	case **int32:
		if *c == nil {
			if val, ok := new.(int32); ok && val > 0 {
				*c = &val
				return true
			}
		}
	case **v1beta1.DiffusionPipelineSpec:
		if pipeline, ok := new.(*v1beta1.DiffusionPipelineSpec); ok && pipeline != nil && *c == nil {
			*c = pipeline
			return true
		}
	case **v1beta1.ModelQuantization:
		if *c == nil && new.(v1beta1.ModelQuantization) != "" {
			val := new.(v1beta1.ModelQuantization)
			*c = &val
			return true
		}
	case *runtime.RawExtension:
		// Fill-only, like every other field here. ModelConfiguration is a
		// preserve-unknown-fields RawExtension: the apiserver re-serializes it
		// with sorted keys, so byte-comparing that against the parser's
		// struct-order output never matches — which marks the field changed on
		// every pass and rewrites the spec with no real difference.
		if len(c.Raw) == 0 && new != nil && len(new.([]byte)) > 0 {
			c.Raw = append(c.Raw[:0], new.([]byte)...)
			return true
		}
	}
	return false
}

// ApplyModelMetadata updates unset BaseModel spec fields from parsed metadata.
func (p *ModelConfigParser) ApplyModelMetadata(spec *v1beta1.BaseModelSpec, metadata ModelMetadata) bool {
	if spec == nil {
		return false
	}
	return p.updateModelSpec(spec, metadata)
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

// shouldSkipConfigParsing honors the per-object skip annotation
// (truthy string). Returns false when neither object carries it.
func (p *ModelConfigParser) shouldSkipConfigParsing(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) bool {
	if baseModel != nil && hasSkipParsingAnnotation(baseModel.Annotations) {
		return true
	}
	if clusterBaseModel != nil && hasSkipParsingAnnotation(clusterBaseModel.Annotations) {
		return true
	}
	return false
}

func hasSkipParsingAnnotation(annotations map[string]string) bool {
	return strings.EqualFold(annotations[ConfigParsingAnnotation], "true")
}

// PopulateArtifactAttribute writes the provided Artifact onto
// currentModelMetadata in place and returns it. A nil artifact is a
// no-op so callers can pass through unconditionally.
func (p *ModelConfigParser) PopulateArtifactAttribute(artifact *Artifact, currentModelMetadata *ModelMetadata) *ModelMetadata {
	if artifact != nil {
		currentModelMetadata.Artifact = *artifact
	}
	return currentModelMetadata
}

// BuildArtifactAttribute constructs an Artifact pointing at a single
// parent path keyed by matchedParentName. ChildrenPaths is passed
// through verbatim (callers append to it later).
func (p *ModelConfigParser) BuildArtifactAttribute(sha string, matchedParentName string, parentPath string, childrenPaths []string) *Artifact {
	return &Artifact{
		Sha:           sha,
		ParentPath:    map[string]string{matchedParentName: parentPath},
		ChildrenPaths: childrenPaths,
	}
}
