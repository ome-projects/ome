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
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestHfArtifactDeleteDoesNotPromoteUnreadyParent(t *testing.T) {
	for _, missingMarker := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed with marker", true: "ready without marker"}[missingMarker], func(t *testing.T) {
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			require.NoError(t, handler.files.CreateChildSymlink(filepath.Join(input.ModelStoreRoot, "unrecorded"), input.Parent.LocalPath))
			if missingMarker {
				require.NoError(t, os.Remove(filepath.Join(input.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)))
			} else {
				require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
					parent, err := requireStoredHfArtifactEntry(cm.Data, input.Parent)
					if err != nil {
						return false, err
					}
					parent.Status = HfArtifactStatusFailed
					return writeHfArtifactEntry(cm.Data, parent)
				}))
			}
			result, err := handler.handleDelete(context.Background(), input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			parent, found, err := repository.Get(context.Background(), input.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, HfArtifactStatusFailed, parent.Status)
		})
	}
}

func TestHfArtifactDeleteRemovesParentAfterLastChild(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	modelStoreRoot := t.TempDir()
	first := testHfArtifactTaskInput(t, modelStoreRoot, "model-1")
	second := testHfArtifactTaskInput(t, modelStoreRoot, "model-2")
	seedTestChildModelEntry(t, repository, first)
	seedTestChildModelEntry(t, repository, second)
	require.NoError(t, runTestHfArtifactDownload(handler, first))
	require.NoError(t, runTestHfArtifactDownload(handler, second))

	firstResult, err := handler.handleDelete(context.Background(), first)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, firstResult.Outcome)
	assertChildPathMissing(t, first.ChildModelPath)
	assertChildParentReferenceCleared(t, repository, first.ChildModelKey)
	assertParentEntryReady(t, repository, first.Parent, map[string]string{
		second.ChildModelKey: second.ChildModelPath,
	})

	secondResult, err := handler.handleDelete(context.Background(), second)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, secondResult.Outcome)
	assertChildPathMissing(t, second.ChildModelPath)
	assertChildParentReferenceCleared(t, repository, second.ChildModelKey)
	_, found, getErr := repository.Get(context.Background(), second.Parent.Identity)
	require.NoError(t, getErr)
	assert.False(t, found)
	_, statErr := os.Stat(second.Parent.LocalPath)
	assert.True(t, os.IsNotExist(statErr))
}

func TestHfArtifactDeleteKeepsParentWithUnrecordedChild(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))

	// The second link exists on disk even though no ConfigMap entry records it.
	unrecordedChildPath := filepath.Join(input.ModelStoreRoot, "model-2")
	require.NoError(t, handler.files.CreateChildSymlink(unrecordedChildPath, input.Parent.LocalPath))

	result, err := handler.handleDelete(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildPathMissing(t, input.ChildModelPath)
	assertChildParentReferenceCleared(t, repository, input.ChildModelKey)
	assertChildSymlinkTarget(t, unrecordedChildPath, input.Parent.LocalPath)
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
	parent, found, err := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
	assert.Empty(t, parent.LockID)
	assert.Empty(t, parent.Children)
}

func TestHfArtifactDeleteRetriesWithoutModelStoreRoot(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	input.ModelStoreRoot = ""

	result, err := handler.handleDelete(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "model store root is required")
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	parent, found, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
	assert.Empty(t, parent.LockID)
	assertParentEntryReady(t, repository, input.Parent, map[string]string{
		input.ChildModelKey: input.ChildModelPath,
	})
	assert.True(t, handler.files.ParentReadyMarkerExists(input.Parent))
}

func TestHfArtifactDeleteRetriesWhenChildPathChanged(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	changedPath := input
	changedPath.ChildModelPath = filepath.Join(filepath.Dir(input.ChildModelPath), "model-1-new-path")

	result, err := handler.handleDelete(context.Background(), changedPath)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	parent, found, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
	assert.Equal(t, map[string]string{
		input.ChildModelKey: input.ChildModelPath,
	}, parent.Children)
}

func TestHfArtifactDeleteRetriesForOneSidedParentReference(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(configMap *corev1.ConfigMap) (bool, error) {
		child, err := existingModelEntry(configMap.Data, input.ChildModelKey)
		if err != nil {
			return false, err
		}
		child.HfArtifactKey = ""
		return writeModelEntry(configMap.Data, input.ChildModelKey, child)
	}))

	result, err := handler.handleDelete(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	parent, found, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
	assert.Equal(t, map[string]string{
		input.ChildModelKey: input.ChildModelPath,
	}, parent.Children)
}

func TestHfArtifactDeleteWaitsForParentOwnerWithoutChildReference(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	parent, found, err := repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	_, err = repository.RemoveModelReference(
		context.Background(),
		parent,
		input.ChildModelKey,
		input.ChildModelUID,
		input.ChildModelPath,
	)
	require.NoError(t, err)

	result, err := handler.handleDelete(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	storedParent, stored, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	require.True(t, stored)
	assert.Equal(t, HfArtifactStatusUpdating, storedParent.Status)
}

func TestHfArtifactDeleteRemovesOnlyUnreferencedSymlink(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	require.NoError(t, writeTestHfArtifactFiles(input.Parent.LocalPath))
	require.NoError(t, handler.files.CreateChildSymlink(input.ChildModelPath, input.Parent.LocalPath))

	result, err := handler.handleDelete(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildPathMissing(t, input.ChildModelPath)
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
}

func TestHfArtifactDeletePreservesChildWhenConfigMapCannotBeRead(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("ConfigMap read unavailable")
	})

	result, err := handler.handleDelete(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "ConfigMap read unavailable")
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
}
