package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

func newTestHfArtifactRepair(t *testing.T) (*hfArtifactTaskHandler, hfArtifactTaskInput, hfArtifactTaskInput) {
	t.Helper()
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	root := t.TempDir()
	first := testHfArtifactTaskInput(t, root, "model-1")
	second := testHfArtifactTaskInput(t, root, "model-2")
	for _, input := range []hfArtifactTaskInput{first, second} {
		seedTestChildModelEntry(t, repository, input)
		require.NoError(t, runTestHfArtifactDownload(handler, input))
		setTestRepairChildStatus(t, repository, input.ChildModelKey, ModelStatusReady)
	}
	return handler, first, second
}

func setTestRepairChildStatus(t *testing.T, repository *HfArtifactRepository, key string, status ModelStatus) {
	t.Helper()
	require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		child.Status = status
		return writeModelEntry(cm.Data, key, child)
	}))
}

func testRepairParent(t *testing.T, h *hfArtifactTaskHandler, input hfArtifactTaskInput) HfArtifactEntry {
	t.Helper()
	parent, found, err := h.repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	return parent
}

func assertTestRepairChildStatuses(t *testing.T, h *hfArtifactTaskHandler, statuses map[string]ModelStatus) {
	t.Helper()
	cm, err := h.repository.configMaps.getConfigMap(context.Background())
	require.NoError(t, err)
	for key, want := range statuses {
		child, err := existingModelEntry(cm.Data, key)
		require.NoError(t, err)
		assert.Equal(t, want, child.Status, "child %s", key)
	}
}

func TestHfArtifactRepairValidFilesAreNotReplaced(t *testing.T) {
	for _, newChild := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing requester", true: "new requester"}[newChild], func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			input := first
			if newChild {
				input = testHfArtifactTaskInput(t, first.ModelStoreRoot, "model-3")
				seedTestChildModelEntry(t, h.repository, input)
			}
			sentinel := filepath.Join(first.Parent.LocalPath, "sentinel")
			require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o644))
			before, err := os.Stat(sentinel)
			require.NoError(t, err)
			validated := false
			h.updateChildStatuses = func(context.Context, map[string]ModelStatus) error {
				t.Fatal("valid files must not change sibling statuses")
				return nil
			}

			result, err := h.handleDownloadOverride(context.Background(), input, func(path string) (bool, error) {
				validated = true
				assert.Equal(t, first.Parent.LocalPath, path)
				assert.FileExists(t, sentinel)
				assert.True(t, h.files.ParentReadyMarkerExists(first.Parent))
				assert.Equal(t, HfArtifactStatusUpdating, testRepairParent(t, h, first).Status)
				return true, nil
			}, func(string) error {
				t.Fatal("valid files must not be downloaded")
				return nil
			})

			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskDone, result.Outcome)
			assert.True(t, validated)
			after, err := os.Stat(sentinel)
			require.NoError(t, err)
			assert.True(t, os.SameFile(before, after))
			assert.Equal(t, before.ModTime(), after.ModTime())
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady})
			assert.Equal(t, HfArtifactStatusReady, testRepairParent(t, h, first).Status)
			assertChildSymlinkTarget(t, input.ChildModelPath, first.Parent.LocalPath)
		})
	}
}

func TestHfArtifactRepairCompletionBlocksNextOwner(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	callbackStarted, releaseCallback := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	h.updateChildStatuses = func(_ context.Context, statuses map[string]ModelStatus) error {
		if statuses[first.ChildModelKey] == ModelStatusReady {
			close(callbackStarted)
			<-releaseCallback
		}
		return nil
	}
	go func() {
		_, err := h.handleDownloadOverride(context.Background(), first,
			func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
		done <- err
	}()
	<-callbackStarted
	// A separate handler using the same node repository must observe the same
	// operation lock, including the marker-backed completion path.
	other := newHfArtifactTaskHandler(newHfArtifactRepository(h.repository.configMaps))
	result, err := other.handleDownload(context.Background(), second, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	result, err = other.handleDownloadOverride(context.Background(), second,
		func(string) (bool, error) {
			t.Error("new owner started before prior callback finished")
			return false, nil
		}, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	close(releaseCallback)
	require.NoError(t, <-done)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady})
}

func TestHfArtifactRepairRetryPreservesInterveningChildStatus(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	_, err := h.handleDownloadOverride(context.Background(), first,
		func(string) (bool, error) { return false, nil }, func(string) error { return errors.New("download failed") })
	require.ErrorContains(t, err, "download failed")
	setTestRepairChildStatus(t, h.repository, second.ChildModelKey, ModelStatusUpdating)
	result, err := h.handleDownloadOverride(context.Background(), first,
		func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusUpdating})
}

func TestHfArtifactDeleteDoesNotMutateDifferentParentsOperation(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	unlock, acquired := h.tryParentOperation(first.Parent.Key)
	require.True(t, acquired)
	defer unlock()
	input := first
	input.Parent.Identity, _ = newHfArtifactIdentity("Qwen/different-model", testHFCommitSHA)
	input.Parent.Key = hfArtifactConfigMapKey(input.Parent.Identity)
	input.Parent.LocalPath = canonicalHfArtifactPath(input.ChildModelPath, input.Parent.Identity)
	result, err := h.handleDelete(context.Background(), input)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "parent reference changed")
	parent, found, err := h.repository.GetParentForChild(context.Background(), first.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, first.Parent.Key, parent.Key)
	assert.True(t, h.files.IsChildLinkedToParent(first.ChildModelPath, parent.LocalPath))
}

func TestHfArtifactRepairValidationErrorPreservesChildrenAndMarker(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	markerPath := filepath.Join(first.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)
	marker, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	wantErr := errors.New("cannot validate checksums")
	h.updateChildStatuses = func(context.Context, map[string]ModelStatus) error {
		t.Fatal("validation error must not fail siblings")
		return nil
	}

	_, err = h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) {
		return false, wantErr
	}, func(string) error {
		t.Fatal("validation error must not download")
		return nil
	})

	require.ErrorIs(t, err, wantErr)
	parent := testRepairParent(t, h, first)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.Empty(t, parent.LockID)
	assert.Empty(t, parent.ChildStatusesBeforeRepair)
	after, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	assert.Equal(t, marker, after)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady})
}

func TestHfArtifactRepairFailsBothSiblingsBeforeWritesAndRestoresThem(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	ready := map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady}
	failed := map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed}
	var events []string
	h.updateChildStatuses = func(_ context.Context, statuses map[string]ModelStatus) error {
		parent := testRepairParent(t, h, first)
		assert.Equal(t, HfArtifactStatusUpdating, parent.Status)
		assertTestRepairChildStatuses(t, h, failed)
		if len(events) == 0 {
			assert.Equal(t, failed, statuses)
			assert.True(t, h.files.ParentReadyMarkerExists(parent), "marker remains until writes are permitted")
			events = append(events, "fail labels")
		} else {
			assert.Equal(t, ready, statuses)
			assert.True(t, h.files.ParentReadyMarkerMatchesLock(parent))
			events = append(events, "restore labels")
		}
		return nil
	}

	result, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) {
		return false, nil
	}, func(path string) error {
		assert.Equal(t, []string{"fail labels"}, events)
		assertTestRepairChildStatuses(t, h, failed)
		assert.False(t, h.files.ParentReadyMarkerExists(first.Parent))
		assert.FileExists(t, filepath.Join(path, "config.json"), "handler must not reset data before download")
		assert.Equal(t, ready, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
		events = append(events, "download")
		return writeTestHfArtifactFiles(path)
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.Equal(t, []string{"fail labels", "download", "restore labels"}, events)
	assertTestRepairChildStatuses(t, h, ready)
	parent := testRepairParent(t, h, first)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
	assert.Empty(t, parent.ChildStatusesBeforeRepair)
	assertChildSymlinkTarget(t, first.ChildModelPath, parent.LocalPath)
	assertChildSymlinkTarget(t, second.ChildModelPath, parent.LocalPath)
}

func TestHfArtifactRepairFailureRetainsStatusesForRetry(t *testing.T) {
	for _, validOnRetry := range []bool{false, true} {
		t.Run(map[bool]string{false: "download again", true: "already valid"}[validOnRetry], func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			setTestRepairChildStatus(t, h.repository, second.ChildModelKey, ModelStatusUpdating)
			prior := map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusUpdating}
			_, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) { return false, nil }, func(string) error {
				return errors.New("download interrupted")
			})
			require.ErrorContains(t, err, "download interrupted")
			parent := testRepairParent(t, h, first)
			assert.Equal(t, HfArtifactStatusFailed, parent.Status)
			assert.Equal(t, prior, parent.ChildStatusesBeforeRepair)
			assert.False(t, h.files.ParentReadyMarkerExists(parent))
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed})

			result, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) { return validOnRetry, nil }, func(path string) error {
				assert.False(t, validOnRetry)
				assert.Equal(t, prior, testRepairParent(t, h, first).ChildStatusesBeforeRepair, "retry must not snapshot temporary Failed statuses")
				return writeTestHfArtifactFiles(path)
			})
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskDone, result.Outcome)
			assertTestRepairChildStatuses(t, h, prior)
			assert.Empty(t, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
		})
	}
}

func TestHfArtifactRepairCompetingOwnerWaits(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	parent, acquired, err := h.repository.TryAcquireLockForRepair(context.Background(), testRepairParent(t, h, first))
	require.NoError(t, err)
	require.True(t, acquired)
	assert.False(t, h.files.ParentReadyMarkerMatchesLock(parent))

	result, err := h.handleDownloadOverride(context.Background(), second, func(string) (bool, error) {
		t.Fatal("competing repair must not validate")
		return false, nil
	}, func(string) error {
		t.Fatal("competing repair must not download")
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.Equal(t, parent, testRepairParent(t, h, first))
}

func TestHfArtifactRepairPendingCompletionRestoresWithoutDownloading(t *testing.T) {
	for _, normalDownload := range []bool{false, true} {
		t.Run(map[bool]string{false: "override retry", true: "normal download retry"}[normalDownload], func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			blockReady := true
			h.updateChildStatuses = func(_ context.Context, statuses map[string]ModelStatus) error {
				if statuses[first.ChildModelKey] == ModelStatusReady && blockReady {
					return errors.New("node labels unavailable")
				}
				return nil
			}
			result, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
			assert.ErrorContains(t, result.RetryReason, "node labels unavailable")
			parent := testRepairParent(t, h, first)
			assert.Equal(t, HfArtifactStatusUpdating, parent.Status)
			assert.True(t, h.files.ParentReadyMarkerMatchesLock(parent))
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed})
			blockReady = false
			noDownload := func(string) error {
				t.Fatal("matching marker must finish without downloading")
				return nil
			}
			if normalDownload {
				result, err = h.handleDownload(context.Background(), second, noDownload)
			} else {
				result, err = h.handleDownloadOverride(context.Background(), second, func(string) (bool, error) {
					t.Fatal("matching marker must finish without validation")
					return false, nil
				}, noDownload)
			}
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskDone, result.Outcome)
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady})
			assert.Empty(t, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
		})
	}
}

func TestHfArtifactRepairRejectsStaleOrOccupiedChild(t *testing.T) {
	for _, scenario := range []string{"stale UID", "changed child path", "occupied child path"} {
		t.Run(scenario, func(t *testing.T) {
			h, first, _ := newTestHfArtifactRepair(t)
			input := first
			parent := testRepairParent(t, h, first)
			want := hfArtifactTaskRetry
			switch scenario {
			case "stale UID":
				h.repository.configMaps.modelCache[first.ChildModelKey] = &CacheEntry{ModelUID: types.UID("replacement-uid")}
				want = hfArtifactTaskDone
			case "changed child path":
				input.ChildModelPath += "-changed"
			case "occupied child path":
				require.NoError(t, os.Remove(first.ChildModelPath))
				require.NoError(t, os.Mkdir(first.ChildModelPath, 0o755))
				want = hfArtifactTaskUseDefaultDownload
			}
			result, err := h.handleDownloadOverride(context.Background(), input, func(string) (bool, error) {
				t.Fatal("must reject before validating")
				return false, nil
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, want, result.Outcome)
			assert.Equal(t, parent, testRepairParent(t, h, first))
		})
	}
}

func TestHfArtifactRepairCallbackSkipsReplacedChildOnRetry(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	_, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) { return false, nil }, func(string) error {
		return errors.New("interrupted")
	})
	require.Error(t, err)
	require.NoError(t, h.repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		child, err := existingModelEntry(cm.Data, second.ChildModelKey)
		if err != nil {
			return false, err
		}
		child.HfArtifactKey = "artifact.huggingface.replacement"
		return writeModelEntry(cm.Data, second.ChildModelKey, child)
	}))
	callbacks := 0
	h.updateChildStatuses = func(_ context.Context, statuses map[string]ModelStatus) error {
		callbacks++
		assert.Len(t, statuses, 1)
		assert.Contains(t, statuses, first.ChildModelKey)
		assert.NotContains(t, statuses, second.ChildModelKey)
		return nil
	}

	result, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.Equal(t, 2, callbacks)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusFailed})
}

func TestHfArtifactRepairRechecksRequesterAfterValidation(t *testing.T) {
	for _, scenario := range []string{"superseded UID", "changed path"} {
		t.Run(scenario, func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			result, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) {
				if scenario == "superseded UID" {
					h.repository.configMaps.modelCache[first.ChildModelKey] = &CacheEntry{ModelUID: "new-uid"}
				} else {
					require.NoError(t, h.repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
						parent, err := decodeHfArtifactEntry(first.Parent.Key, cm.Data[first.Parent.Key])
						if err != nil {
							return false, err
						}
						parent.Children[first.ChildModelKey] += "-replacement"
						return writeHfArtifactEntry(cm.Data, parent)
					}))
				}
				return false, nil
			}, func(string) error {
				t.Fatal("superseded requester must not start file mutation")
				return nil
			})
			require.NoError(t, err)
			if scenario == "superseded UID" {
				assert.Equal(t, hfArtifactTaskDone, result.Outcome)
			} else {
				assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
				assert.ErrorContains(t, result.RetryReason, "path does not match")
			}
			assert.Empty(t, testRepairParent(t, h, first).LockID)
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady})
		})
	}
}

func TestHfArtifactRepairFailedStatusCallbackPreventsFileWrites(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	markerPath := filepath.Join(first.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)
	before, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	h.updateChildStatuses = func(context.Context, map[string]ModelStatus) error {
		return errors.New("cannot update node labels")
	}

	_, err = h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) { return false, nil }, func(string) error {
		t.Fatal("failed label update must prevent writes")
		return nil
	})

	require.ErrorContains(t, err, "cannot update node labels")
	after, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	parent := testRepairParent(t, h, first)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.Equal(t, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady}, parent.ChildStatusesBeforeRepair)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed})
}

func TestHfArtifactRepairPendingConfigMapCompletionRetries(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	client := h.repository.configMaps.kubeClient.(*fake.Clientset)
	blockReady := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		parent, err := decodeHfArtifactEntry(first.Parent.Key, cm.Data[first.Parent.Key])
		require.NoError(t, err)
		if blockReady && parent.Status == HfArtifactStatusReady {
			return true, nil, errors.New("cannot publish Ready")
		}
		return false, nil, nil
	})
	callbacks := 0
	h.updateChildStatuses = func(context.Context, map[string]ModelStatus) error {
		callbacks++
		return nil
	}
	result, err := h.handleDownloadOverride(context.Background(), first, func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.ErrorContains(t, result.RetryReason, "cannot publish Ready")
	parent := testRepairParent(t, h, first)
	assert.Equal(t, HfArtifactStatusUpdating, parent.Status)
	assert.True(t, h.files.ParentReadyMarkerMatchesLock(parent))
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed})
	blockReady = false

	result, err = h.handleDownloadOverride(context.Background(), first, nil, nil)

	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.Equal(t, 3, callbacks, "completion retries the Ready label update")
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady})
}

// The fake client discards request contexts; this wrapper checks the actual
// context supplied when cleanup publishes Failed.
type hfArtifactRepairContextClient struct {
	kubernetes.Interface
	checkUpdate func(context.Context, *corev1.ConfigMap)
}

func (c hfArtifactRepairContextClient) CoreV1() typedcorev1.CoreV1Interface {
	return hfArtifactRepairContextCore{c.Interface.CoreV1(), c.checkUpdate}
}

type hfArtifactRepairContextCore struct {
	typedcorev1.CoreV1Interface
	checkUpdate func(context.Context, *corev1.ConfigMap)
}

func (c hfArtifactRepairContextCore) ConfigMaps(namespace string) typedcorev1.ConfigMapInterface {
	return hfArtifactRepairContextConfigMaps{c.CoreV1Interface.ConfigMaps(namespace), c.checkUpdate}
}

type hfArtifactRepairContextConfigMaps struct {
	typedcorev1.ConfigMapInterface
	checkUpdate func(context.Context, *corev1.ConfigMap)
}

func (c hfArtifactRepairContextConfigMaps) Update(ctx context.Context, cm *corev1.ConfigMap, opts metav1.UpdateOptions) (*corev1.ConfigMap, error) {
	c.checkUpdate(ctx, cm)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.ConfigMapInterface.Update(ctx, cm, opts)
}

func TestHfArtifactRepairCancellationStillPublishesFailedWithBoundedContext(t *testing.T) {
	for _, cancelDuring := range []string{"validation", "download"} {
		t.Run(cancelDuring, func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cleanupObserved := false
			h.repository.configMaps.kubeClient = hfArtifactRepairContextClient{
				Interface: h.repository.configMaps.kubeClient,
				checkUpdate: func(cleanupCtx context.Context, cm *corev1.ConfigMap) {
					parent, err := decodeHfArtifactEntry(first.Parent.Key, cm.Data[first.Parent.Key])
					require.NoError(t, err)
					if parent.Status != HfArtifactStatusFailed {
						return
					}
					cleanupObserved = true
					assert.ErrorIs(t, ctx.Err(), context.Canceled)
					assert.NoError(t, cleanupCtx.Err())
					deadline, bounded := cleanupCtx.Deadline()
					assert.True(t, bounded)
					assert.WithinDuration(t, time.Now(), deadline, 10*time.Second)
				},
			}
			_, err := h.handleDownloadOverride(ctx, first, func(string) (bool, error) {
				if cancelDuring == "validation" {
					cancel()
					return false, ctx.Err()
				}
				return false, nil
			}, func(string) error {
				cancel()
				return ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
			assert.True(t, cleanupObserved)
			parent := testRepairParent(t, h, first)
			assert.Equal(t, HfArtifactStatusFailed, parent.Status)
			assert.Empty(t, parent.LockID)
			want := ModelStatusReady
			if cancelDuring == "download" {
				want = ModelStatusFailed
			}
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: want, second.ChildModelKey: want})
		})
	}
}
