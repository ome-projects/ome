package modelconfig

import (
	"fmt"
	"os"
	"path/filepath"
)

// ParseModelConfig parses a HuggingFace model configuration from
// in-memory bytes. Path is used as a label and to disambiguate
// model_index.json (diffusion pipelines) from config.json
// (transformer configs).
func ParseModelConfig(input ModelConfigInput) (HuggingFaceModel, error) {
	if len(input.Data) == 0 {
		return nil, fmt.Errorf("model config data cannot be empty")
	}
	return parseGenericModelConfig(input)
}

// LoadModelConfig reads a HuggingFace model configuration from a
// config.json or model_index.json file and returns a HuggingFaceModel.
// A config_sentence_transformers.json beside the file is read too.
func LoadModelConfig(configPath string) (HuggingFaceModel, error) {
	if configPath == "" {
		return nil, fmt.Errorf("config path cannot be empty")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read model config %s: %w", configPath, err)
	}
	input := ModelConfigInput{Path: configPath, Data: data}
	// Most models ship no sentence-transformers config, so a missing or
	// unreadable one is not an error.
	stConfigPath := filepath.Join(filepath.Dir(configPath), SentenceTransformersConfigFileName)
	if stData, err := os.ReadFile(stConfigPath); err == nil {
		input.SentenceTransformersConfig = stData
	}
	return parseGenericModelConfig(input)
}
