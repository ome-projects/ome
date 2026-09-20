package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestHfArtifactDownloadPreservesParentsNeedingRepairOrAnotherOwner(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     HfArtifactStatus
		marker     string
		child      bool
		wantReason string
	}{
		{name: "ready without marker", status: HfArtifactStatusReady, wantReason: "requires repair"},
		{name: "failed with child", status: HfArtifactStatusFailed, child: true, wantReason: "requires repair"},
		{name: "updating with old marker", status: HfArtifactStatusUpdating, marker: "previous-lock"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
			seedTestChildModelEntry(t, repository, input)
			parent := input.Parent
			parent.Status = tc.status
			if tc.status == HfArtifactStatusUpdating {
				parent.LockID = "current-lock"
			}
			if tc.child {
				parent.Children["default.basemodel.existing"] = filepath.Join(input.ModelStoreRoot, "existing")
			} else {
				parent.Children = nil
			}
			require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(configMap *corev1.ConfigMap) (bool, error) {
				return writeHfArtifactEntry(configMap.Data, parent)
			}))
			require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
			if tc.marker != "" {
				require.NoError(t, os.WriteFile(filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName), []byte(tc.marker), 0o644))
			}

			result, err := handler.handleDownload(context.Background(), input, func(string) error {
				t.Fatal("normal download must not replace this parent")
				return nil
			})

			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
			assert.Equal(t, parent.Key, result.RetryParentKey)
			if tc.wantReason != "" {
				assert.ErrorContains(t, result.RetryReason, tc.wantReason)
			}
			stored, found, err := repository.Get(context.Background(), parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, parent, stored)
			assert.FileExists(t, filepath.Join(parent.LocalPath, "config.json"))
			assertChildPathMissing(t, input.ChildModelPath)
		})
	}
}

func TestHfArtifactDownloadRetriesReadyUpdateWithoutDownloadingAgain(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	blockUpdates := false
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if blockUpdates {
			return true, nil, errors.New("ConfigMap update unavailable")
		}
		return false, nil, nil
	})
	downloads := 0
	download := func(parentPath string) error {
		downloads++
		blockUpdates = true
		return writeTestHfArtifactFiles(parentPath)
	}

	result, err := handler.handleDownload(context.Background(), input, download)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "ConfigMap update unavailable")
	parent, found, err := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusUpdating, parent.Status)
	assert.True(t, handler.files.ParentReadyMarkerMatchesLock(parent))
	assertChildPathMissing(t, input.ChildModelPath)

	blockUpdates = false
	result, err = handler.handleDownload(context.Background(), input, download)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.Equal(t, 1, downloads)
	assertParentEntryReady(t, repository, input.Parent, map[string]string{input.ChildModelKey: input.ChildModelPath})
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
}

func TestHfArtifactDownloadRetriesWhenConfigMapCannotBeRead(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("ConfigMap read unavailable")
	})

	result, err := handler.handleDownload(context.Background(), input, func(string) error {
		t.Fatal("unreadable state must not start a download")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "ConfigMap read unavailable")
	assertChildPathMissing(t, input.ChildModelPath)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestHfArtifactDownloadRetriesMissingChildBeforeLocalChanges(t *testing.T) {
	for _, parentReady := range []bool{false, true} {
		name := "missing parent"
		if parentReady {
			name = "ready parent"
		}
		t.Run(name, func(t *testing.T) {
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			root := t.TempDir()
			input := testHfArtifactTaskInput(t, root, "missing-child")
			if parentReady {
				first := testHfArtifactTaskInput(t, root, "existing-child")
				seedTestChildModelEntry(t, repository, first)
				require.NoError(t, runTestHfArtifactDownload(handler, first))
			}
			before, err := repository.configMaps.getConfigMap(context.Background())
			require.NoError(t, err)
			downloads := 0

			result, err := handler.handleDownload(context.Background(), input, func(parentPath string) error {
				downloads++
				return writeTestHfArtifactFiles(parentPath)
			})

			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
			assert.Equal(t, input.Parent.Key, result.RetryParentKey)
			assert.ErrorContains(t, result.RetryReason, "model entry "+input.ChildModelKey+" does not exist")
			assert.Zero(t, downloads)
			assertChildPathMissing(t, input.ChildModelPath)
			after, err := repository.configMaps.getConfigMap(context.Background())
			require.NoError(t, err)
			assert.Equal(t, before.Data, after.Data)
			if parentReady {
				assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
				assertChildSymlinkTarget(t, filepath.Join(root, "existing-child"), input.Parent.LocalPath)
			} else {
				assert.NoDirExists(t, input.Parent.LocalPath)
			}
		})
	}
}

func TestHfArtifactDownloadCreatesParentForFirstChild(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	var downloads atomic.Int32

	result, err := handler.handleDownload(context.Background(), input, func(parentPath string) error {
		downloads.Add(1)
		return writeTestHfArtifactFiles(parentPath)
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.Equal(t, int32(1), downloads.Load())
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	assertParentEntryReady(t, repository, input.Parent, map[string]string{
		input.ChildModelKey: input.ChildModelPath,
	})
}

func TestHfArtifactDownloadReusesReadyParentForSecondChild(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	modelStoreRoot := t.TempDir()
	first := testHfArtifactTaskInput(t, modelStoreRoot, "model-1")
	second := testHfArtifactTaskInput(t, modelStoreRoot, "model-2")
	seedTestChildModelEntry(t, repository, first)
	seedTestChildModelEntry(t, repository, second)
	var downloads atomic.Int32
	download := func(parentPath string) error {
		downloads.Add(1)
		return writeTestHfArtifactFiles(parentPath)
	}

	_, err := handler.handleDownload(context.Background(), first, download)
	require.NoError(t, err)
	result, err := handler.handleDownload(context.Background(), second, func(string) error {
		t.Fatal("ready parent reuse must not download a second copy")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.Equal(t, int32(1), downloads.Load())
	assertChildSymlinkTarget(t, second.ChildModelPath, first.Parent.LocalPath)
	assertParentEntryReady(t, repository, first.Parent, map[string]string{
		first.ChildModelKey:  first.ChildModelPath,
		second.ChildModelKey: second.ChildModelPath,
	})
}

func TestHfArtifactDownloadRetriesUnreferencedFailedParent(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)

	_, err := handler.handleDownload(context.Background(), input, func(string) error {
		return errors.New("first download failed")
	})
	require.EqualError(t, err, "first download failed")
	parent, found, err := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.Empty(t, parent.LockID)
	assert.Empty(t, parent.Children)
	assertChildPathMissing(t, input.ChildModelPath)

	result, err := handler.handleDownload(context.Background(), input, writeTestHfArtifactFiles)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	assertParentEntryReady(t, repository, input.Parent, map[string]string{
		input.ChildModelKey: input.ChildModelPath,
	})
}

func TestHfArtifactDownloadRetriesWithoutModelStoreRoot(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	input.ModelStoreRoot = ""
	seedTestChildModelEntry(t, repository, input)

	result, err := handler.handleDownload(context.Background(), input, func(string) error {
		t.Fatal("a missing model store root must prevent parent replacement")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "model store root is required")
	_, found, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	assert.False(t, found)
}

func TestHfArtifactDownloadPreservesParentWithUnrecordedChild(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, os.MkdirAll(input.Parent.LocalPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(input.Parent.LocalPath, "config.json"), []byte("{}"), 0o644))
	require.NoError(t, os.Symlink(
		input.Parent.LocalPath,
		filepath.Join(input.ModelStoreRoot, "untracked-model"),
	))

	result, err := handler.handleDownload(context.Background(), input, func(string) error {
		t.Fatal("an existing local child must prevent replacing the shared parent")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "local child symlink")
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
	_, found, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	assert.False(t, found)
}

func TestHfArtifactDownloadUsesDefaultDownloadForOccupiedChildPath(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, os.MkdirAll(input.ChildModelPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "sentinel"), []byte("keep"), 0o644))

	result, err := handler.handleDownload(context.Background(), input, func(string) error {
		t.Fatal("occupied child path must not start shared parent download")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskUseDefaultDownload, result.Outcome)
	_, found, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	assert.False(t, found)
}

func TestHfArtifactDownloadRetriesWhenChildPathChanged(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	changedPath := input
	changedPath.ChildModelPath = filepath.Join(filepath.Dir(input.ChildModelPath), "model-1-new-path")

	result, err := handler.handleDownload(context.Background(), changedPath, func(string) error {
		t.Fatal("a path-mismatched child must not download")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	assertChildPathMissing(t, changedPath.ChildModelPath)
	assertParentEntryReady(t, repository, input.Parent, map[string]string{
		input.ChildModelKey: input.ChildModelPath,
	})
}

func TestHfArtifactDownloadSkipsSymlinkForSupersededChild(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))

	second := testHfArtifactTaskInput(t, input.ModelStoreRoot, "model-2")
	second.ChildModelKey = input.ChildModelKey
	second.ChildModelUID = types.UID("stale-model-uid")
	second.ChildModelPath = filepath.Join(input.ModelStoreRoot, "model-2")
	require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(configMap *corev1.ConfigMap) (bool, error) {
		return writeModelEntry(configMap.Data, second.ChildModelKey, ModelEntry{
			Name:   "model-2",
			Status: ModelStatusUpdating,
		})
	}))
	repository.configMaps.cacheMutex.Lock()
	repository.configMaps.modelCache[second.ChildModelKey] = &CacheEntry{ModelUID: types.UID("current-model-uid")}
	repository.configMaps.cacheMutex.Unlock()
	before, err := repository.configMaps.getConfigMap(context.Background())
	require.NoError(t, err)

	result, err := handler.attachChildToReadyParent(context.Background(), second, second.Parent)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildPathMissing(t, second.ChildModelPath)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	after, err := repository.configMaps.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before.Data, after.Data)
	parent, found, getErr := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, getErr)
	require.True(t, found)
	assert.Equal(t, map[string]string{input.ChildModelKey: input.ChildModelPath}, parent.Children)
}

func TestHfArtifactDownloadSerializesChildrenDuringDownload(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	root := t.TempDir()
	first := testHfArtifactTaskInput(t, root, "model-1")
	second := testHfArtifactTaskInput(t, root, "model-2")
	seedTestChildModelEntry(t, repository, first)
	seedTestChildModelEntry(t, repository, second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var worker sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		worker.Wait()
	})
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	worker.Add(1)
	go func() {
		defer worker.Done()
		result, err := handler.handleDownload(ctx, first, func(path string) error {
			close(started)
			select {
			case <-release:
				return writeTestHfArtifactFiles(path)
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err == nil && result.Outcome != hfArtifactTaskDone {
			err = errors.New("first child did not complete")
		}
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first download did not start")
	}
	noDownload := func(string) error {
		t.Error("second child must not download the shared parent")
		return errors.New("unexpected second download")
	}

	result, err := handler.handleDownload(ctx, second, noDownload)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildPathMissing(t, second.ChildModelPath)

	close(release)
	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("first download did not finish")
	}
	result, err = handler.handleDownload(ctx, second, noDownload)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertParentEntryReady(t, repository, first.Parent, map[string]string{
		first.ChildModelKey: first.ChildModelPath, second.ChildModelKey: second.ChildModelPath,
	})
}

func TestHfArtifactDownloadReusesParentCompletedBeforeLockAcquisition(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	ctx := context.Background()
	parent, acquired, err := repository.TryAcquireLock(ctx, input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
	require.NoError(t, handler.files.WriteParentReadyMarker(parent))
	require.NoError(t, repository.MarkReady(ctx, parent))

	// This task already chose initial download before another task finished the
	// parent. Losing lock acquisition must reuse it without resetting the files.
	result, err := handler.downloadParentAndAttachChild(ctx, input, input.Parent, func(string) error {
		t.Fatal("completed parent must not be downloaded again after losing acquisition")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, parent.LocalPath)
	assertParentEntryReady(t, repository, parent, map[string]string{input.ChildModelKey: input.ChildModelPath})
}

func TestHfArtifactDownloadPreservesSymlinkAfterReferenceWriteFailure(t *testing.T) {
	for _, failure := range []string{"write rejected", "write response lost", "confirmation read failed"} {
		t.Run(failure, func(t *testing.T) {
			repository, client := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			first := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
			second := testHfArtifactTaskInput(t, first.ModelStoreRoot, "model-2")
			seedTestChildModelEntry(t, repository, first)
			seedTestChildModelEntry(t, repository, second)
			require.NoError(t, runTestHfArtifactDownload(handler, first))
			failReads := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if failure == "confirmation read failed" {
					failReads = true
					return false, nil, nil
				}
				if failure == "write response lost" {
					update := action.(ktesting.UpdateAction)
					require.NoError(t, client.Tracker().Update(action.GetResource(), update.GetObject(), action.GetNamespace()))
				}
				return true, nil, errors.New("reference update unavailable")
			})
			client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if failReads {
					return true, nil, errors.New("reference confirmation unavailable")
				}
				return false, nil, nil
			})
			noDownload := func(string) error {
				t.Fatal("reference retries must not download a ready parent")
				return nil
			}

			result, err := handler.handleDownload(context.Background(), second, noDownload)

			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
			assert.Error(t, result.RetryReason)
			assertChildSymlinkTarget(t, second.ChildModelPath, first.Parent.LocalPath)
			client.ReactionChain = client.ReactionChain[2:]
			result, err = handler.handleDownload(context.Background(), second, noDownload)
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskDone, result.Outcome)
			assertParentEntryReady(t, repository, first.Parent, map[string]string{
				first.ChildModelKey: first.ChildModelPath, second.ChildModelKey: second.ChildModelPath,
			})
		})
	}
}

func TestHfArtifactDownloadPreservesExistingSymlinkWhenReferenceCannotBeConfirmed(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("ConfigMap unavailable")
	})

	result, err := handler.attachChildToReadyParent(context.Background(), input, input.Parent)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
}

func TestHfArtifactDownloadRechecksChildrenAfterLockAcquisition(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	parent := input.Parent
	parent.Status = HfArtifactStatusFailed
	parent.Children = map[string]string{"default.basemodel.model-2": filepath.Join(input.ModelStoreRoot, "model-2")}
	require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(configMap *corev1.ConfigMap) (bool, error) {
		return writeHfArtifactEntry(configMap.Data, parent)
	}))
	require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))

	// The task's earlier snapshot had no children. Acquisition returns current
	// state, which must be checked before resetting the parent directory.
	result, err := handler.downloadParentAndAttachChild(context.Background(), input, input.Parent, func(string) error {
		t.Error("a parent with recorded children requires repair")
		return errors.New("unexpected parent download")
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "requires repair")
	stored, found, err := repository.Get(context.Background(), parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusFailed, stored.Status)
	assert.Empty(t, stored.LockID)
	assert.Equal(t, parent.Children, stored.Children)
	assert.FileExists(t, filepath.Join(parent.LocalPath, "config.json"))
}

func TestHfArtifactDownloadRechecksLocalChildrenAfterLockAcquisition(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, writeTestHfArtifactFiles(input.Parent.LocalPath))
	childPath := filepath.Join(input.ModelStoreRoot, "unrecorded-child")
	createdChild := false
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if !createdChild {
			// The initial scan finished, but acquisition has not yet returned.
			require.NoError(t, os.Symlink(input.Parent.LocalPath, childPath))
			createdChild = true
		}
		return false, nil, nil
	})

	result, err := handler.handleDownload(context.Background(), input, func(string) error {
		t.Fatal("a new local child must prevent parent replacement")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "local child symlink")
	parent, found, err := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.Empty(t, parent.LockID)
	assertChildSymlinkTarget(t, childPath, parent.LocalPath)
	assert.FileExists(t, filepath.Join(parent.LocalPath, "config.json"))
}
