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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
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

func TestHfArtifactStaleAttachRemovesOnlyNewLink(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			repository, client := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			parent, acquired, err := repository.TryAcquireLock(context.Background(), input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
			require.NoError(t, handler.files.WriteParentReadyMarker(parent))
			require.NoError(t, repository.MarkReady(context.Background(), parent))
			if existing {
				require.NoError(t, handler.files.CreateChildSymlink(input.ChildModelPath, parent.LocalPath))
			}
			invalidateOnAttach(t, repository, client, input)
			result, err := handler.handleDownload(context.Background(), input, nil)
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskDone, result.Outcome)
			if existing {
				assertChildSymlinkTarget(t, input.ChildModelPath, parent.LocalPath)
			} else {
				assertChildPathMissing(t, input.ChildModelPath)
			}
		})
	}
}

func invalidateOnAttach(t *testing.T, repository *HfArtifactRepository, client *fake.Clientset, input hfArtifactTaskInput) {
	t.Helper()
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if (hfArtifactFiles{}).IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath) {
			// GET may run under mutationMutex. Inject the newer informer-cache
			// view directly; invoking the public invalidator here would deadlock.
			repository.configMaps.cacheMutex.Lock()
			repository.configMaps.modelCache[input.ChildModelKey] = &CacheEntry{ModelUID: types.UID("replacement-uid")}
			repository.configMaps.cacheMutex.Unlock()
		}
		return false, nil, nil
	})
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

func TestHfArtifactScanFindsUnknownLinkInOtherArtifact(t *testing.T) {
	root := t.TempDir()
	input := testHfArtifactTaskInput(t, root, "child")
	other := filepath.Join(root, "_artifacts", "other", "old-snapshot")
	require.NoError(t, os.MkdirAll(other, 0o755))
	require.NoError(t, os.Symlink(input.Parent.LocalPath, filepath.Join(other, "unknown-child")))
	found, err := (hfArtifactFiles{}).HasChildren(input.Parent.LocalPath, root)
	require.NoError(t, err)
	assert.True(t, found, "only the actual parent's contents may be skipped")
}
