package modelparser

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"sigs.k8s.io/ome/pkg/modelconfig"
)

// writeModelFile creates the parent directory of path and writes data to it.
func writeModelFile(t *testing.T, path string, data string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))
}

// A model directory with no config.json at the root or in the common
// subdirectories, but one copy in each of two other subdirectories: the
// parser reads the first copy in lexical order, and the metadata it
// returns (here the architecture) comes from that copy.
func TestParseModelConfigReadsTheFirstSubdirectoryCopy(t *testing.T) {
	tempDir := t.TempDir()
	writeModelFile(t, filepath.Join(tempDir, "decoder", DefaultConfigFileName),
		`{"model_type":"llama","architectures":["LlamaForCausalLM"]}`)
	writeModelFile(t, filepath.Join(tempDir, "encoder", DefaultConfigFileName),
		`{"model_type":"clip_vision_model","architectures":["CLIPVisionModel"]}`)

	logger, _ := zap.NewDevelopment()
	metadata, err := NewModelConfigParser(logger.Sugar()).ParseModelConfig(tempDir, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, "LlamaForCausalLM", metadata.ModelArchitecture)
}

// The same rule for model_index.json: of two subdirectory copies the
// parser loads the first in lexical order.
func TestParseModelConfigLoadsTheFirstSubdirectoryModelIndex(t *testing.T) {
	tempDir := t.TempDir()
	first := filepath.Join(tempDir, "pipeline-a", DefaultModelIndexFileName)
	writeModelFile(t, first, `{"_class_name":"StableDiffusionPipeline"}`)
	writeModelFile(t, filepath.Join(tempDir, "pipeline-b", DefaultModelIndexFileName),
		`{"_class_name":"StableDiffusionPipeline"}`)

	var loadedPath string
	logger, _ := zap.NewDevelopment()
	parser := &ModelConfigParser{
		logger: logger.Sugar(),
		loadModelConfig: func(configPath string) (modelconfig.HuggingFaceModel, error) {
			loadedPath = configPath
			return &mockHuggingFaceModel{modelType: "diffusers", architecture: "StableDiffusionPipeline"}, nil
		},
	}
	_, err := parser.ParseModelConfig(tempDir, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, first, loadedPath)
}
