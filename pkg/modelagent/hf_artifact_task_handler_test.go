package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func testHfArtifactTaskInput(t *testing.T, modelStoreRoot, childName string) hfArtifactTaskInput {
	t.Helper()
	identity, err := newHfArtifactIdentity("Qwen/Qwen3-8B", testHFCommitSHA)
	require.NoError(t, err)
	childPath := filepath.Join(modelStoreRoot, childName)
	return hfArtifactTaskInput{
		Parent: HfArtifactEntry{
			Key:       hfArtifactConfigMapKey(identity),
			Identity:  identity,
			LocalPath: canonicalHfArtifactPath(childPath, identity),
			Children:  make(map[string]string),
		},
		ChildModelKey:  "default.basemodel." + childName,
		ChildModelUID:  types.UID(childName + "-uid"),
		ChildModelPath: childPath,
		ModelStoreRoot: modelStoreRoot,
	}
}

func seedTestChildModelEntry(t *testing.T, repository *HfArtifactRepository, input hfArtifactTaskInput) {
	t.Helper()
	err := repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(configMap *corev1.ConfigMap) (bool, error) {
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		return writeModelEntry(configMap.Data, input.ChildModelKey, ModelEntry{
			Name:   filepath.Base(input.ChildModelPath),
			Status: ModelStatusUpdating,
		})
	})
	require.NoError(t, err)
}

func runTestHfArtifactDownload(handler *hfArtifactTaskHandler, input hfArtifactTaskInput) error {
	result, err := handler.handleDownload(context.Background(), input, writeTestHfArtifactFiles)
	if err != nil {
		return err
	}
	if result.Outcome != hfArtifactTaskDone {
		return errors.New("shared artifact download did not complete")
	}
	return nil
}

func writeTestHfArtifactFiles(parentPath string) error {
	if err := os.MkdirAll(parentPath, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(parentPath, "config.json"), []byte("{}"), 0o644)
}

func assertParentEntryReady(t *testing.T, repository *HfArtifactRepository, expected HfArtifactEntry, children map[string]string) {
	t.Helper()
	parent, found, err := repository.Get(context.Background(), expected.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
	if len(children) == 0 {
		assert.Empty(t, parent.Children)
	} else {
		assert.Equal(t, children, parent.Children)
	}
	for childKey := range children {
		configMap, getErr := repository.configMaps.getConfigMap(context.Background())
		require.NoError(t, getErr)
		child, childErr := existingModelEntry(configMap.Data, childKey)
		require.NoError(t, childErr)
		assert.Equal(t, parent.Key, child.HfArtifactKey)
	}
}

func assertChildSymlinkTarget(t *testing.T, childPath, parentPath string) {
	t.Helper()
	target, err := os.Readlink(childPath)
	require.NoError(t, err)
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(childPath), target)
	}
	assert.Equal(t, filepath.Clean(parentPath), filepath.Clean(target))
}

func assertChildPathMissing(t *testing.T, childPath string) {
	t.Helper()
	_, err := os.Lstat(childPath)
	assert.True(t, os.IsNotExist(err))
}

func assertChildParentReferenceCleared(t *testing.T, repository *HfArtifactRepository, childModelKey string) {
	t.Helper()
	configMap, err := repository.configMaps.getConfigMap(context.Background())
	require.NoError(t, err)
	child, err := existingModelEntry(configMap.Data, childModelKey)
	require.NoError(t, err)
	assert.Empty(t, child.HfArtifactKey)
}
