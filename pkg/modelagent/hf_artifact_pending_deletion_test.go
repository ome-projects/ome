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
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestHfArtifactDeleteResumeAfterReferenceRemoval(t *testing.T) {
	for _, failure := range []string{"unlink", "parent removal", "delete status", "lost delete response", "pending cleanup"} {
		t.Run(failure, func(t *testing.T) {
			repository, client := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			fail := true
			refsRemoved := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				child, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if !fail {
					return false, nil, nil
				}
				if child.HfArtifactKey == "" && !refsRemoved {
					refsRemoved = true
					switch failure {
					case "unlink":
						require.NoError(t, os.Remove(input.ChildModelPath))
						require.NoError(t, os.Mkdir(input.ChildModelPath, 0o755))
					case "parent removal":
						if os.Geteuid() == 0 {
							t.Skip("root bypasses directory permissions")
						}
						require.NoError(t, os.Chmod(filepath.Dir(input.Parent.LocalPath), 0o500))
						t.Cleanup(func() { _ = os.Chmod(filepath.Dir(input.Parent.LocalPath), 0o755) })
					}
					return false, nil, nil
				}
				_, parentPresent := cm.Data[input.Parent.Key]
				if !parentPresent && (failure == "delete status" || failure == "lost delete response") {
					if failure == "lost delete response" {
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					}
					return true, nil, errors.New("delete response unavailable")
				}
				if !parentPresent && failure == "pending cleanup" && child.HfArtifactPendingDeletion == nil {
					return true, nil, errors.New("pending cleanup unavailable")
				}
				return false, nil, nil
			})
			result, err := handler.handleDelete(context.Background(), input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.True(t, refsRemoved)
			// A new process/reconciler must still discover the shared delete even
			// after both references (and potentially the parent itself) are gone.
			cm, err := repository.configMaps.getConfigMap(context.Background())
			require.NoError(t, err)
			resumed, _ := newTestHfArtifactRepository(t, cm.Data)
			parent, found, err := resumed.GetParentForChild(context.Background(), input.ChildModelKey)
			require.NoError(t, err)
			require.True(t, found, "pending cleanup must not fall back to legacy delete")
			require.Equal(t, input.Parent.Key, parent.Key)
			require.Equal(t, input.ChildModelPath, parent.Children[input.ChildModelKey])
			fail = false
			if failure == "unlink" {
				require.NoError(t, os.Remove(input.ChildModelPath))
				require.NoError(t, os.Symlink(input.Parent.LocalPath, input.ChildModelPath))
			}
			if failure == "parent removal" {
				require.NoError(t, os.Chmod(filepath.Dir(input.Parent.LocalPath), 0o755))
			}
			result, err = newHfArtifactTaskHandler(resumed).handleDelete(context.Background(), input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			_, found, err = resumed.GetParentForChild(context.Background(), input.ChildModelKey)
			require.NoError(t, err)
			assert.False(t, found)
			assertChildPathMissing(t, input.ChildModelPath)
			assert.NoDirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestHfArtifactDeleteLostResponseDoesNotRestoreCachedParent(t *testing.T) {
	for _, reconcileBeforeRetry := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry first", true: "reconcile first"}[reconcileBeforeRetry], func(t *testing.T) {
			h, input, _ := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			c := h.repository.configMaps
			client := c.kubeClient.(*fake.Clientset)
			lostResponse := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				if _, present := cm.Data[input.Parent.Key]; present || lostResponse {
					return false, nil, nil
				}
				lostResponse = true
				require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				return true, nil, errors.New("parent deletion response lost")
			})
			result, err := h.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.True(t, lostResponse)
			require.Contains(t, c.hfArtifactCache, input.Parent.Key)
			assert.NoDirExists(t, input.Parent.LocalPath)
			if reconcileBeforeRetry {
				c.reconcileConfigMaps()
			}

			// Retry in the same process, retaining the cache from before the lost response.
			result, err = h.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome, "%v", result.RetryReason)
			c.reconcileConfigMaps()
			_, found, err := h.repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			assert.False(t, found, "completed deletion must not resurrect the cached parent")
			assert.NotContains(t, c.hfArtifactCache, input.Parent.Key)
		})
	}
}

func TestHfArtifactPendingDeletionAfterParentRecreatedAtDifferentPath(t *testing.T) {
	for _, replacementUID := range []bool{false, true} {
		t.Run(map[bool]string{false: "same UID", true: "replacement UID"}[replacementUID], func(t *testing.T) {
			h, input, pending := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			parent, found, err := h.repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			require.NoError(t, h.files.RemoveChildSymlink(input.ChildModelPath, parent.LocalPath))
			require.NoError(t, h.files.RemoveParentDirectory(parent.LocalPath))
			deleted, err := h.repository.DeleteIfUnreferenced(ctx, parent)
			require.NoError(t, err)
			require.True(t, deleted)
			// Cleanup stopped before clearing its receipt. The same identity is
			// then downloaded for another child under a different model root.
			other := testHfArtifactTaskInput(t, t.TempDir(), "other-child")
			require.Equal(t, input.Parent.Key, other.Parent.Key)
			require.NotEqual(t, input.Parent.LocalPath, other.Parent.LocalPath)
			seedTestChildModelEntry(t, h.repository, other)
			require.NoError(t, runTestHfArtifactDownload(h, other))
			before, err := h.repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)
			marker, err := os.ReadFile(filepath.Join(other.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName))
			require.NoError(t, err)
			if replacementUID {
				input.ChildModelUID = "replacement-uid"
				h.isCurrentChildUID = func(current hfArtifactTaskInput) (bool, error) {
					return current.ChildModelUID == input.ChildModelUID, nil
				}
			}
			// A conflicting leftover directory is not proof of completed cleanup.
			require.NoError(t, os.MkdirAll(input.Parent.LocalPath, 0o755))
			result, err := h.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			stored, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, pending, stored)
			require.NoError(t, os.Remove(input.Parent.LocalPath))

			result, err = h.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome, "%v", result.RetryReason)
			stored, err = h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			assert.Nil(t, stored)
			after, err := h.repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, before.Data[other.Parent.Key], after.Data[other.Parent.Key])
			assert.Equal(t, before.Data[other.ChildModelKey], after.Data[other.ChildModelKey])
			afterMarker, err := os.ReadFile(filepath.Join(other.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName))
			require.NoError(t, err)
			assert.Equal(t, marker, afterMarker)
			assertChildSymlinkTarget(t, other.ChildModelPath, other.Parent.LocalPath)
			assert.DirExists(t, other.Parent.LocalPath)
		})
	}
}

func TestHfArtifactPendingDeletionPreservesReattachedPath(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "child")
	sibling := testHfArtifactTaskInput(t, input.ModelStoreRoot, "sibling")
	for _, child := range []hfArtifactTaskInput{input, sibling} {
		seedTestChildModelEntry(t, repository, child)
		require.NoError(t, runTestHfArtifactDownload(handler, child))
	}
	_, err := repository.removeModelReference(context.Background(), input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	// The old task stops after removing references. A different model then
	// attaches to the same still-Ready parent through the old child's path.
	replacement := input
	replacement.ChildModelKey = "default.basemodel.replacement"
	replacement.ChildModelUID = "replacement-uid"
	seedTestChildModelEntry(t, repository, replacement)
	require.NoError(t, runTestHfArtifactDownload(handler, replacement))
	result, err := handler.handleDelete(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	parent, found, err := repository.GetParentForChild(context.Background(), replacement.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusReady, parent.Status)
	_, found, err = repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestHfArtifactPendingDeletionRequiresLockedParent(t *testing.T) {
	for _, operation := range []string{"download", "repair", "delete"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			_, err := repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			before, err := repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)

			// A late task holds a different parent's lock when it discovers
			// the old receipt. Another process still owns the old path lock.
			lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
			require.NoError(t, err)
			require.True(t, acquired)
			t.Cleanup(func() { _ = lock.Close() })
			next := input
			next.Parent.Identity.ModelID = "Qwen/Qwen3-4B"
			next.Parent.Key = hfArtifactConfigMapKey(next.Parent.Identity)
			next.Parent.LocalPath = canonicalHfArtifactPath(next.ChildModelPath, next.Parent.Identity)
			var result hfArtifactTaskResult
			switch operation {
			case "download":
				result, err = handler.handleDownload(ctx, next, nil)
			case "repair":
				result, err = handler.handleDownloadOverride(ctx, next, nil, nil)
			case "delete":
				result, err = handler.handleDelete(ctx, next)
			}
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
			assert.Equal(t, input.Parent.Key, result.RetryParentKey)
			assert.ErrorContains(t, result.RetryReason, "shared artifact parent path changed")
			after, err := repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, before.Data, after.Data, "a different parent lock cannot authorize receipt cleanup")
			assert.DirExists(t, input.Parent.LocalPath)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)

			// Retrying with the receipt's parent can finish once its owner exits.
			require.NoError(t, lock.Close())
			result, err = handler.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			assertChildPathMissing(t, input.ChildModelPath)
			assert.NoDirExists(t, input.Parent.LocalPath)
			pending, err := repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			assert.Nil(t, pending)
		})
	}
}

func TestHfArtifactPendingDeletionBecomesLastChild(t *testing.T) {
	ctx := context.Background()
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	first := testHfArtifactTaskInput(t, t.TempDir(), "first")
	second := testHfArtifactTaskInput(t, first.ModelStoreRoot, "second")
	for _, child := range []hfArtifactTaskInput{first, second} {
		seedTestChildModelEntry(t, repository, child)
		require.NoError(t, runTestHfArtifactDownload(handler, child))
	}
	// The first delete stops after persisting its receipt, before unlinking.
	removal, err := repository.removeModelReference(ctx, first.Parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath, true)
	require.NoError(t, err)
	require.True(t, removal.ReferenceRemoved)
	require.False(t, removal.LastReferenceRemoved)
	result, err := handler.handleDelete(ctx, second)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.DirExists(t, first.Parent.LocalPath, "the first child's remaining symlink protects the parent")

	result, err = handler.handleDelete(ctx, first)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildPathMissing(t, first.ChildModelPath)
	assertChildPathMissing(t, second.ChildModelPath)
	_, found, err := repository.Get(ctx, first.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found, "the resumed delete must clean up the now-unreferenced parent")
	assert.NoDirExists(t, first.Parent.LocalPath)
	for _, child := range []hfArtifactTaskInput{first, second} {
		pending, err := repository.pendingDeletion(ctx, child.ChildModelKey)
		require.NoError(t, err)
		assert.Nil(t, pending)
	}
}

func TestHfArtifactPendingDeletionUsesCurrentParentStatus(t *testing.T) {
	handler, first, second := newTestHfArtifactRepair(t)
	repository := handler.repository
	ctx := context.Background()
	_, err := repository.removeModelReference(ctx, first.Parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath, true)
	require.NoError(t, err)
	result, err := handler.handleDelete(ctx, second)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	parent, acquired, err := repository.TryAcquireLockForRepair(ctx, first.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, repository.MarkFailed(ctx, parent))
	unknownChild := filepath.Join(first.ModelStoreRoot, "unrecorded-child")
	require.NoError(t, os.Symlink(parent.LocalPath, unknownChild))

	result, err = handler.handleDelete(ctx, first)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	stored, found, err := repository.Get(ctx, parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusFailed, stored.Status, "the old Ready receipt must not revive a Failed parent")
	assert.Empty(t, stored.LockID)
	assertChildPathMissing(t, first.ChildModelPath)
	assertChildSymlinkTarget(t, unknownChild, parent.LocalPath)
	assert.DirExists(t, parent.LocalPath)
	pending, err := repository.pendingDeletion(ctx, first.ChildModelKey)
	require.NoError(t, err)
	assert.Nil(t, pending)
}

func TestHfArtifactPendingDeletionRetriesLostLockResponse(t *testing.T) {
	handler, first, second := newTestHfArtifactRepair(t)
	repository := handler.repository
	ctx := context.Background()
	_, err := repository.removeModelReference(ctx, first.Parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath, true)
	require.NoError(t, err)
	result, err := handler.handleDelete(ctx, second)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	client := repository.configMaps.kubeClient.(*fake.Clientset)
	lostResponse := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		child, err := existingModelEntry(cm.Data, first.ChildModelKey)
		require.NoError(t, err)
		if !lostResponse && child.HfArtifactPendingDeletion != nil && child.HfArtifactPendingDeletion.ParentLockID != "" {
			lostResponse = true
			require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			return true, nil, errors.New("deletion lock response lost")
		}
		return false, nil, nil
	})
	result, err = handler.handleDelete(ctx, first)
	require.NoError(t, err)
	require.True(t, lostResponse)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildSymlinkTarget(t, first.ChildModelPath, first.Parent.LocalPath)
	result, err = handler.handleDelete(ctx, first)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildPathMissing(t, first.ChildModelPath)
	assert.NoDirExists(t, first.Parent.LocalPath)
	_, found, err := repository.Get(ctx, first.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found)
	pending, err := repository.pendingDeletion(ctx, first.ChildModelKey)
	require.NoError(t, err)
	assert.Nil(t, pending)
}

func TestHfArtifactPendingDeletionUIDTakeover(t *testing.T) {
	for _, scenario := range []string{"current UID", "unknown UID", "stale UID", "changed owner", "changed receipt"} {
		t.Run(scenario, func(t *testing.T) {
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			removal, err := repository.removeModelReference(context.Background(), input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			pending, err := repository.pendingDeletion(context.Background(), input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, pending)
			current := input
			current.ChildModelUID = "new-uid"
			if scenario != "unknown UID" {
				handler.isCurrentChildUID = func(input hfArtifactTaskInput) (bool, error) {
					return input.ChildModelUID == "new-uid", nil
				}
			}
			if scenario == "stale UID" {
				current.ChildModelUID = "not-current-uid"
			}
			if scenario == "changed owner" {
				require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
					parent := removal.Artifact
					parent.LockID = "other-owner"
					return writeHfArtifactEntry(cm.Data, parent)
				}))
			}
			if scenario == "changed receipt" {
				require.NoError(t, repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
					child, err := existingModelEntry(cm.Data, input.ChildModelKey)
					if err != nil {
						return false, err
					}
					child.HfArtifactPendingDeletion.ParentWasReady = !pending.ParentWasReady
					return writeModelEntry(cm.Data, input.ChildModelKey, child)
				}))
			}
			unlock, acquired, err := handler.tryArtifactOperation(current)
			require.NoError(t, err)
			require.True(t, acquired)
			result, err := handler.resumePendingDeletion(context.Background(), current, pending)
			unlock()
			require.NoError(t, err)
			if scenario == "current UID" {
				require.Equal(t, hfArtifactTaskDone, result.Outcome, "%v", result.RetryReason)
				assertChildPathMissing(t, input.ChildModelPath)
				require.NoError(t, runTestHfArtifactDownload(handler, current))
				assertChildSymlinkTarget(t, current.ChildModelPath, current.Parent.LocalPath)
			} else {
				assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
				stored, err := repository.pendingDeletion(context.Background(), input.ChildModelKey)
				require.NoError(t, err)
				require.NotNil(t, stored)
				assert.Equal(t, input.ChildModelUID, stored.ModelUID)
			}
		})
	}
}
