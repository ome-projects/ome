package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func newRecoveryCoveragePendingDeletion(t *testing.T) (*hfArtifactTaskHandler, hfArtifactTaskInput, *HfArtifactPendingDeletion) {
	t.Helper()
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "child")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	_, err := repository.removeModelReference(context.Background(), input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	pending, err := repository.pendingDeletion(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, pending)
	return handler, input, pending
}

func TestRecoveryCoverageDownloadResumesAbandonedDeletion(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(map[bool]string{false: "download", true: "repair"}[repair], func(t *testing.T) {
			h, input, pending := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			oldLock := pending.ParentLockID
			require.NotEmpty(t, oldLock)
			// A restarted agent releases the abandoned ConfigMap owner before
			// a queued download discovers the interrupted deletion receipt.
			require.NoError(t, newHfArtifactStartup(h).recover(ctx))
			parent, _, err := h.repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			require.Equal(t, HfArtifactStatusFailed, parent.Status)
			var replacementLock string
			h.repository.configMaps.kubeClient.(*fake.Clientset).PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				if raw, present := cm.Data[input.Parent.Key]; present {
					parent, err := decodeHfArtifactEntry(input.Parent.Key, raw)
					require.NoError(t, err)
					if parent.Status == HfArtifactStatusUpdating {
						child, err := existingModelEntry(cm.Data, input.ChildModelKey)
						require.NoError(t, err)
						require.NotNil(t, child.HfArtifactPendingDeletion)
						assert.Equal(t, parent.LockID, child.HfArtifactPendingDeletion.ParentLockID)
						replacementLock = parent.LockID
					}
				}
				return false, nil, nil
			})
			unexpectedDownload := func(string) error {
				t.Fatal("cleanup must finish before starting another download")
				return nil
			}
			var result hfArtifactTaskResult
			if repair {
				result, err = h.handleDownloadOverride(ctx, input, func(string) (bool, error) {
					t.Fatal("cleanup must finish before validating a replacement")
					return true, nil
				}, unexpectedDownload)
			} else {
				result, err = h.handleDownload(ctx, input, unexpectedDownload)
			}
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.NotEmpty(t, replacementLock)
			assert.NotEqual(t, oldLock, replacementLock)
			assertChildPathMissing(t, input.ChildModelPath)
			assert.NoDirExists(t, input.Parent.LocalPath)
			pending, err = h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			assert.Nil(t, pending)
			_, found, err := h.repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			assert.False(t, found)
		})
	}
}

func TestRecoveryCoverageDeletionRejectsChangedOwnership(t *testing.T) {
	for _, scenario := range []string{"stale UID", "corrupt child", "changed receipt", "corrupt parent", "live reference", "other owner", "invalid status"} {
		t.Run(scenario, func(t *testing.T) {
			h, input, pending := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			c := h.repository.configMaps
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			parent, err := decodeHfArtifactEntry(input.Parent.Key, cm.Data[input.Parent.Key])
			require.NoError(t, err)
			switch scenario {
			case "stale UID":
				c.invalidateModelUIDAndEvictCache(input.ChildModelKey, input.ChildModelUID)
			case "corrupt child":
				cm.Data[input.ChildModelKey] = "broken child JSON"
			case "changed receipt":
				child, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				child.HfArtifactPendingDeletion.ParentWasReady = !pending.ParentWasReady
				_, err = writeModelEntry(cm.Data, input.ChildModelKey, child)
				require.NoError(t, err)
			case "corrupt parent":
				cm.Data[parent.Key] = "broken parent JSON"
			case "live reference":
				parent.Children = map[string]string{input.ChildModelKey: input.ChildModelPath}
			case "other owner":
				parent.LockID = "new-owner"
			case "invalid status":
				parent.Status = HfArtifactStatus("unknown")
			}
			if scenario == "live reference" || scenario == "other owner" || scenario == "invalid status" {
				raw, err := json.Marshal(parent)
				require.NoError(t, err)
				cm.Data[parent.Key] = string(raw)
			}
			_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			before := cm.DeepCopy()
			originalReceipt := *pending
			_, _, err = h.repository.deletionParent(ctx, input.ChildModelKey, pending, false)
			require.Error(t, err)
			assert.Equal(t, originalReceipt, *pending)
			if scenario == "stale UID" || scenario == "corrupt child" || scenario == "changed receipt" {
				require.Error(t, h.repository.finishPendingDeletion(ctx, input.ChildModelKey, originalReceipt))
			}
			after, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, before.Data, after.Data)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			assert.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestRecoveryCoverageDeletionParentCacheEvictionGuards(t *testing.T) {
	for _, scenario := range []string{"matching owner", "expected lock empty", "cached lock different", "cached children present", "cached path different"} {
		t.Run(scenario, func(t *testing.T) {
			h, input, pending := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			c := h.repository.configMaps
			cached, valid := hfArtifactRecoveryParent(input.Parent.Key, c.hfArtifactCache[input.Parent.Key])
			require.True(t, valid)
			require.NotEmpty(t, pending.ParentLockID)
			require.Equal(t, pending.ParentLockID, cached.LockID)
			require.Empty(t, cached.Children)
			switch scenario {
			case "expected lock empty":
				pending.ParentLockID, cached.LockID = "", ""
			case "cached lock different":
				cached.LockID = "another-owner"
			case "cached children present":
				cached.Children = map[string]string{input.ChildModelKey: input.ChildModelPath}
			case "cached path different":
				cached.LocalPath = testHfArtifactTaskInput(t, t.TempDir(), "other-child").Parent.LocalPath
			}
			raw, err := json.Marshal(cached)
			require.NoError(t, err)
			_, valid = hfArtifactRecoveryParent(input.Parent.Key, string(raw))
			require.True(t, valid, "exercise the ownership guard, not invalid-record rejection")
			c.hfArtifactCache[input.Parent.Key] = string(raw)

			unlock, acquired, err := h.tryArtifactOperation(input)
			require.NoError(t, err)
			require.True(t, acquired)
			defer unlock()
			require.NoError(t, h.files.RemoveChildSymlink(input.ChildModelPath, input.Parent.LocalPath))
			require.NoError(t, h.files.RemoveParentDirectory(input.Parent.LocalPath))
			require.NoDirExists(t, input.Parent.LocalPath)
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			child, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			child.HfArtifactPendingDeletion = pending
			_, err = writeModelEntry(cm.Data, input.ChildModelKey, child)
			require.NoError(t, err)
			delete(cm.Data, input.Parent.Key)
			// Bypass cache publication to retain the pre-delete parent snapshot.
			_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			originalReceipt := *pending

			_, found, err := h.repository.deletionParent(ctx, input.ChildModelKey, pending, true)

			require.NoError(t, err)
			assert.False(t, found)
			if scenario == "matching owner" {
				assert.NotContains(t, c.hfArtifactCache, input.Parent.Key)
			} else {
				assert.Equal(t, string(raw), c.hfArtifactCache[input.Parent.Key])
			}
			assert.Equal(t, originalReceipt, *pending)
			after, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, cm.Data, after.Data)
		})
	}
}

func TestRecoveryCoveragePendingDeletionFinishIsIdempotent(t *testing.T) {
	h, input, pending := newRecoveryCoveragePendingDeletion(t)
	ctx := context.Background()
	result, err := h.handleDelete(ctx, input)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.NoError(t, h.repository.finishPendingDeletion(ctx, input.ChildModelKey, *pending))
	stored, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
	require.NoError(t, err)
	assert.Nil(t, stored)
}

func TestHfArtifactPendingDeletionRejectsInvalidReplacement(t *testing.T) {
	for _, scenario := range []string{"unclean path", "invalid status", "missing owner"} {
		t.Run(scenario, func(t *testing.T) {
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			replacement := testHfArtifactTaskInput(t, t.TempDir(), "other-child").Parent
			replacement.Status = HfArtifactStatusReady
			switch scenario {
			case "unclean path":
				replacement.LocalPath += "/"
			case "invalid status":
				replacement.Status = HfArtifactStatus("unknown")
			case "missing owner":
				replacement.Status = HfArtifactStatusUpdating
				replacement.LockID = ""
			}
			raw, err := json.Marshal(replacement)
			require.NoError(t, err)
			pending := HfArtifactPendingDeletion{Identity: input.Parent.Identity, ParentPath: input.Parent.LocalPath}
			_, found, err := pendingDeletionParent(map[string]string{replacement.Key: string(raw)}, input.ChildModelKey, pending, true)
			require.Error(t, err)
			assert.False(t, found)
		})
	}
}

func TestRecoveryCoverageReceiptHandoffRechecksListerProof(t *testing.T) {
	for _, failure := range []string{"lookup error", "replaced UID", "invalidated UID"} {
		t.Run(failure, func(t *testing.T) {
			h, input, pending := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			c := h.repository.configMaps
			// Seed the old process-local owner, as an ordinary status update does.
			require.NoError(t, c.mutateModelEntryWithRetry(ctx, input.ChildModelKey, input.ChildModelUID, false, func(map[string]string) (bool, error) { return false, nil }))
			calls := 0
			proof := func() (bool, error) {
				calls++
				if calls == 2 {
					switch failure {
					case "lookup error":
						return false, errors.New("lister unavailable during adoption")
					case "replaced UID":
						return false, nil
					case "invalidated UID":
						c.cacheMutex.Lock()
						if c.invalidatedModelUIDs[input.ChildModelKey] == nil {
							c.invalidatedModelUIDs[input.ChildModelKey] = make(map[types.UID]struct{})
						}
						c.invalidatedModelUIDs[input.ChildModelKey]["replacement-uid"] = struct{}{}
						c.cacheMutex.Unlock()
					}
				}
				return true, nil
			}
			err := h.repository.claimPendingDeletion(ctx, input.ChildModelKey, "replacement-uid", pending, false, proof)
			require.Error(t, err)
			require.Equal(t, 2, calls)
			stored, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.EqualValues(t, "replacement-uid", stored.ModelUID, "the receipt CAS committed before proof changed")
			assert.Equal(t, input.ChildModelUID, c.modelCache[input.ChildModelKey].ModelUID)
			assert.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, "replacement-uid"))
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			if failure != "invalidated UID" {
				require.NoError(t, h.repository.claimPendingDeletion(ctx, input.ChildModelKey, "replacement-uid", stored, false, func() (bool, error) { return true, nil }))
				assert.False(t, h.repository.isChildMutationBlocked(input.ChildModelKey, "replacement-uid"))
				assert.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID))
			}
		})
	}
}

func TestHfArtifactPendingDeletionHandoffRejectsChangedReceiptAfterCommit(t *testing.T) {
	for _, change := range []string{"replaced", "removed"} {
		t.Run(change, func(t *testing.T) {
			h, input, pending := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			c := h.repository.configMaps
			require.NoError(t, c.mutateModelEntryWithRetry(ctx, input.ChildModelKey, input.ChildModelUID, false, func(map[string]string) (bool, error) { return false, nil }))
			client := c.kubeClient.(*fake.Clientset)
			committed := false
			var changedData map[string]string
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				child, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if committed || child.HfArtifactPendingDeletion == nil || child.HfArtifactPendingDeletion.ModelUID != "replacement-uid" {
					return false, nil, nil
				}
				committed = true
				// Return the successful CAS response, but make the next GET see
				// a competing write before the process-local UID can be adopted.
				stored := cm.DeepCopy()
				if change == "removed" {
					child.HfArtifactPendingDeletion = nil
				} else {
					child.HfArtifactPendingDeletion.ParentLockID = "another-owner"
				}
				_, err = writeModelEntry(stored.Data, input.ChildModelKey, child)
				require.NoError(t, err)
				require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), stored, stored.Namespace))
				changedData = stored.Data
				return true, cm, nil
			})

			err := h.repository.claimPendingDeletion(ctx, input.ChildModelKey, "replacement-uid", pending, false, func() (bool, error) { return true, nil })

			require.True(t, committed)
			require.ErrorContains(t, err, "changed before UID handoff")
			assert.Equal(t, input.ChildModelUID, c.modelCache[input.ChildModelKey].ModelUID)
			assert.False(t, h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID))
			assert.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, "replacement-uid"))
			after, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, changedData, after.Data)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			assert.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestRecoveryCoverageDeferredStartupRetriesAPIOutage(t *testing.T) {
	for _, failedVerb := range []string{"get", "update"} {
		t.Run(failedVerb, func(t *testing.T) {
			h, first, _ := newTestHfArtifactRepair(t)
			ctx := context.Background()
			parent, acquired, err := h.repository.TryAcquireLockForRepair(ctx, first.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			lock, acquired, err := tryHfArtifactParentFileLock(parent, first.ModelStoreRoot)
			require.NoError(t, err)
			require.True(t, acquired)
			t.Cleanup(func() { _ = lock.Close() })
			startup := newHfArtifactStartup(h)
			require.NoError(t, startup.recover(ctx))
			require.Contains(t, startup.deferred, parent.Key)
			require.NoError(t, lock.Close())
			unavailable := true
			h.repository.configMaps.kubeClient.(*fake.Clientset).PrependReactor(failedVerb, "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if unavailable {
					return true, nil, errors.New("deferred recovery unavailable")
				}
				return false, nil, nil
			})
			require.ErrorContains(t, startup.recoverParent(ctx, parent.Key), "deferred recovery unavailable")
			assert.Contains(t, startup.deferred, parent.Key)
			assert.True(t, startup.needsValidation(parent.Key))
			unavailable = false
			require.NoError(t, startup.recoverParent(ctx, parent.Key))
			assert.NotContains(t, startup.deferred, parent.Key)
			stored, _, err := h.repository.Get(ctx, parent.Identity)
			require.NoError(t, err)
			assert.Equal(t, HfArtifactStatusFailed, stored.Status)
			assert.Empty(t, stored.LockID)
			assertChildSymlinkTarget(t, first.ChildModelPath, parent.LocalPath)
		})
	}
}
