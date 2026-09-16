package modelagent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestHfArtifactRecoveryDoesNotPublishReadyFromStaleCompletion(t *testing.T) {
	for _, wholeMap := range []bool{false, true} {
		t.Run(fmt.Sprint(wholeMap), func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			c := h.repository.configMaps
			ctx := context.Background()
			if wholeMap {
				require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
			} else {
				cm, err := c.getConfigMap(ctx)
				require.NoError(t, err)
				delete(cm.Data, first.ChildModelKey)
				_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-1", Namespace: "default", UID: first.ChildModelUID}}
			err := c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady})
			require.ErrorContains(t, err, "requires validation")
			parent, found, err := h.repository.Get(ctx, first.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, HfArtifactStatusFailed, parent.Status)
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed})
		})
	}
}

func TestHfArtifactRecoveryDoesNotOverwriteCorruptChildState(t *testing.T) {
	for _, raw := range []string{"malformed", "null", `{}`} {
		t.Run(raw, func(t *testing.T) {
			h, first, _ := newTestHfArtifactRepair(t)
			c := h.repository.configMaps
			cm, err := c.getConfigMap(context.Background())
			require.NoError(t, err)
			cm.Data[first.ChildModelKey] = raw
			_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-1", Namespace: "default", UID: first.ChildModelUID}}
			err = c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady})
			require.ErrorContains(t, err, "requires reconciliation")
			cm, err = c.getConfigMap(context.Background())
			require.NoError(t, err)
			assert.Equal(t, raw, cm.Data[first.ChildModelKey])
			legacy := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default"}}
			cm.Data[getModelID(legacy, nil)] = raw
			_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: legacy, ModelStatus: ModelStatusReady}))
		})
	}
}

func TestHfArtifactRecoveryRestoresRelationshipsAsFailed(t *testing.T) {
	for _, missing := range []string{"ConfigMap", "parent", "child"} {
		t.Run(missing, func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			c := h.repository.configMaps
			ctx := context.Background()
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			if missing == "ConfigMap" {
				require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
			} else {
				key := first.Parent.Key
				if missing == "child" {
					key = first.ChildModelKey
				}
				delete(cm.Data, key)
				_, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}

			c.reconcileConfigMaps()

			parent, found, err := h.repository.Get(ctx, first.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, HfArtifactStatusFailed, parent.Status)
			assert.Empty(t, parent.LockID)
			assert.Empty(t, parent.LastCompletedLockID)
			assert.Equal(t, map[string]string{first.ChildModelKey: first.ChildModelPath, second.ChildModelKey: second.ChildModelPath}, parent.Children)
			assert.Equal(t, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady}, parent.ChildStatusesBeforeRepair)
			assertTestRepairChildStatuses(t, h, map[string]ModelStatus{first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed})
			for _, key := range []string{first.ChildModelKey, second.ChildModelKey} {
				linked, found, err := h.repository.GetParentForChild(ctx, key)
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, parent.Key, linked.Key)
			}
		})
	}
}

func TestHfArtifactRecoveryPreservesRelationshipsWhenMutationRecreatesConfigMap(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx := context.Background()
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))

	err := c.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		return writeModelEntry(cm.Data, "default.basemodel.other", ModelEntry{Name: "other", Status: ModelStatusUpdating})
	})

	require.NoError(t, err)
	parent, found, err := h.repository.Get(ctx, first.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.Equal(t, map[string]string{first.ChildModelKey: first.ChildModelPath, second.ChildModelKey: second.ChildModelPath}, parent.Children)
}

func TestHfArtifactRecoveryCachesStartupSnapshot(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	previous := h.repository.configMaps
	c := NewConfigMapReconciler(previous.nodeName, previous.namespace, previous.kubeClient, previous.logger)
	ctx := context.Background()
	assert.Empty(t, c.modelCache)
	assert.Empty(t, c.hfArtifactCache)

	cm, err := c.getConfigMapForRecovery(ctx)
	require.NoError(t, err)
	for _, key := range []string{first.ChildModelKey, second.ChildModelKey} {
		require.NotNil(t, c.modelCache[key])
		assert.Empty(t, c.modelCache[key].ModelUID, "persisted records do not identify the child CR UID")
	}
	// The caller's returned snapshot is not an alias for the recovery cache.
	delete(cm.Data, first.Parent.Key)
	delete(cm.Data, first.ChildModelKey)
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	c.reconcileConfigMaps()

	parent, found, err := newHfArtifactRepository(c).Get(ctx, first.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.Equal(t, map[string]string{first.ChildModelKey: first.ChildModelPath, second.ChildModelKey: second.ChildModelPath}, parent.Children)
	assert.Equal(t, map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady}, parent.ChildStatusesBeforeRepair)
	for _, key := range []string{first.ChildModelKey, second.ChildModelKey} {
		linked, found, err := newHfArtifactRepository(c).GetParentForChild(ctx, key)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, first.Parent.Key, linked.Key)
	}
}

func TestHfArtifactRecoveryDoesNotResurrectRemovedReferencesOrParents(t *testing.T) {
	for _, removeParent := range []bool{false, true} {
		t.Run(map[bool]string{false: "detached child", true: "deleted parent"}[removeParent], func(t *testing.T) {
			h, first, second := newTestHfArtifactRepair(t)
			c := h.repository.configMaps
			ctx := context.Background()
			staleSnapshot := *c.modelCache[first.ChildModelKey]
			_, err := h.repository.RemoveModelReference(ctx, first.Parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath)
			require.NoError(t, err)
			if removeParent {
				last, err := h.repository.RemoveModelReference(ctx, second.Parent, second.ChildModelKey, second.ChildModelUID, second.ChildModelPath)
				require.NoError(t, err)
				deleted, err := h.repository.DeleteIfUnreferenced(ctx, last.Artifact)
				require.NoError(t, err)
				require.True(t, deleted)
			}
			require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
			c.restoreModelInConfigMap(first.ChildModelKey, &staleSnapshot)
			c.reconcileConfigMaps()

			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			child, err := existingModelEntry(cm.Data, first.ChildModelKey)
			require.NoError(t, err)
			assert.Empty(t, child.HfArtifactKey, "old snapshot must not restore a detached reference")
			if removeParent {
				assert.NotContains(t, cm.Data, first.Parent.Key)
			} else {
				parent, err := decodeHfArtifactEntry(first.Parent.Key, cm.Data[first.Parent.Key])
				require.NoError(t, err)
				assert.Equal(t, map[string]string{second.ChildModelKey: second.ChildModelPath}, parent.Children)
				assert.NotContains(t, parent.ChildStatusesBeforeRepair, first.ChildModelKey)
			}
		})
	}
}

func TestHfArtifactRecoveryDoesNotResurrectInvalidatedUID(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx := context.Background()
	require.NoError(t, c.mutateModelEntryWithRetry(ctx, first.ChildModelKey, first.ChildModelUID, false, func(map[string]string) (bool, error) { return false, nil }))
	staleSnapshot := *c.modelCache[first.ChildModelKey]
	c.invalidateModelUIDAndEvictCache(first.ChildModelKey, first.ChildModelUID)
	// An unrelated successful write still observes the old child in Kubernetes.
	// It must not silently put that evicted child back into the recovery cache.
	require.NoError(t, c.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		return writeModelEntry(cm.Data, "default.basemodel.other", ModelEntry{Name: "other", Status: ModelStatusReady})
	}))
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	c.restoreModelInConfigMap(first.ChildModelKey, &staleSnapshot)
	c.reconcileConfigMaps()

	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	assert.NotContains(t, cm.Data, first.ChildModelKey)
	parent, err := decodeHfArtifactEntry(first.Parent.Key, cm.Data[first.Parent.Key])
	require.NoError(t, err)
	assert.Equal(t, map[string]string{second.ChildModelKey: second.ChildModelPath}, parent.Children)
	assert.NotContains(t, parent.ChildStatusesBeforeRepair, first.ChildModelKey)
}

func TestHfArtifactRecoveryIgnoresCorruptUnrelatedParent(t *testing.T) {
	for _, relevant := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated parent", true: "relevant parent quarantined"}[relevant], func(t *testing.T) {
			h, first, _ := newTestHfArtifactRepair(t)
			c := h.repository.configMaps
			ctx := context.Background()
			legacyKey := "default.basemodel.legacy"
			require.NoError(t, c.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
				return writeModelEntry(cm.Data, legacyKey, ModelEntry{Name: "legacy", Status: ModelStatusReady})
			}))
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			corruptKey := "artifact.huggingface.foreign"
			if relevant {
				corruptKey = first.Parent.Key
				delete(cm.Data, first.ChildModelKey)
			}
			cm.Data[corruptKey] = "{broken"
			delete(cm.Data, legacyKey)
			_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)

			c.reconcileConfigMaps()

			after, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, "{broken", after.Data[corruptKey])
			legacy, err := existingModelEntry(after.Data, legacyKey)
			require.NoError(t, err)
			assert.Equal(t, ModelStatusReady, legacy.Status)
			if relevant {
				assert.NotContains(t, after.Data, first.ChildModelKey)
			}
		})
	}
}

func TestHfArtifactRecoveryDoesNotCacheFailedReferenceMutation(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	client := c.kubeClient.(*fake.Clientset)
	ctx := context.Background()
	block := true
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if block {
			return true, nil, errors.New("write unavailable")
		}
		return false, nil, nil
	})
	_, err := h.repository.RemoveModelReference(ctx, first.Parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath)
	require.ErrorContains(t, err, "write unavailable")
	block = false
	require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	c.reconcileConfigMaps()

	parent := testRepairParent(t, h, first)
	assert.Equal(t, map[string]string{first.ChildModelKey: first.ChildModelPath, second.ChildModelKey: second.ChildModelPath}, parent.Children)
}

func TestHfArtifactRecoveryFailedRestoreRetainsTrustedCache(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	client := c.kubeClient.(*fake.Clientset)
	ctx := context.Background()
	require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	block := true
	client.PrependReactor("create", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if block {
			return true, nil, errors.New("create unavailable")
		}
		return false, nil, nil
	})
	c.reconcileConfigMaps()
	_, err := c.getConfigMap(ctx)
	require.Error(t, err)
	block = false
	c.reconcileConfigMaps()
	prior := map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady}
	assert.Equal(t, prior, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
	// Losing the restored Failed state again must not replace the original
	// child statuses with the temporary Failed statuses from the first recovery.
	require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	c.reconcileConfigMaps()
	assert.Equal(t, prior, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
	result, err := h.handleDownloadOverride(ctx, first, func(string) (bool, error) { return true, nil }, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertTestRepairChildStatuses(t, h, prior)
}

func TestHfArtifactRecoveryDoesNotAliasCallerMaps(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx := context.Background()
	parent := testRepairParent(t, h, first)
	childConfig := &ModelConfig{ModelFramework: map[string]string{"name": "original"}}
	require.NoError(t, c.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		child, err := existingModelEntry(cm.Data, first.ChildModelKey)
		if err != nil {
			return false, err
		}
		child.Config = childConfig
		if _, err := writeModelEntry(cm.Data, first.ChildModelKey, child); err != nil {
			return false, err
		}
		_, err = writeHfArtifactEntry(cm.Data, parent)
		return true, err
	}))
	parent.Children[first.ChildModelKey] = "/caller-mutated-path"
	childConfig.ModelFramework["name"] = "caller-mutated-framework"
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	c.reconcileConfigMaps()

	assert.Equal(t, first.ChildModelPath, testRepairParent(t, h, first).Children[first.ChildModelKey])
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	child, err := existingModelEntry(cm.Data, first.ChildModelKey)
	require.NoError(t, err)
	assert.Equal(t, "original", child.Config.ModelFramework["name"])
}

func TestHfArtifactRecoveryPreservesInterveningChildStatus(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx := context.Background()
	_, err := h.handleDownloadOverride(ctx, first,
		func(string) (bool, error) { return false, nil },
		func(string) error { return errors.New("download failed") })
	require.ErrorContains(t, err, "download failed")
	setTestRepairChildStatus(t, h.repository, second.ChildModelKey, ModelStatusUpdating)
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))

	c.reconcileConfigMaps()

	prior := map[string]ModelStatus{first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusUpdating}
	assert.Equal(t, prior, testRepairParent(t, h, first).ChildStatusesBeforeRepair)
	result, err := h.handleDownloadOverride(ctx, first, func(string) (bool, error) { return true, nil }, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertTestRepairChildStatuses(t, h, prior)
}

type hfRecoveryDelayedClient struct {
	kubernetes.Interface
	afterUpdate func(*corev1.ConfigMap)
}

func (c hfRecoveryDelayedClient) CoreV1() typedcorev1.CoreV1Interface {
	return hfRecoveryDelayedCore{c.Interface.CoreV1(), c.afterUpdate}
}

type hfRecoveryDelayedCore struct {
	typedcorev1.CoreV1Interface
	afterUpdate func(*corev1.ConfigMap)
}

func (c hfRecoveryDelayedCore) ConfigMaps(namespace string) typedcorev1.ConfigMapInterface {
	return hfRecoveryDelayedConfigMaps{c.CoreV1Interface.ConfigMaps(namespace), c.afterUpdate}
}

type hfRecoveryDelayedConfigMaps struct {
	typedcorev1.ConfigMapInterface
	afterUpdate func(*corev1.ConfigMap)
}

func (c hfRecoveryDelayedConfigMaps) Update(ctx context.Context, cm *corev1.ConfigMap, opts metav1.UpdateOptions) (*corev1.ConfigMap, error) {
	result, err := c.ConfigMapInterface.Update(ctx, cm, opts)
	if err == nil {
		c.afterUpdate(result)
	}
	return result, err
}

func TestHfArtifactRecoverySerializesWritesAndCachePublication(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	committed := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	c.kubeClient = hfRecoveryDelayedClient{Interface: c.kubeClient, afterUpdate: func(cm *corev1.ConfigMap) {
		child, _ := existingModelEntry(cm.Data, first.ChildModelKey)
		if child.Status == ModelStatusUpdating {
			close(committed)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}}
	writeStatus := func(status ModelStatus) error {
		return c.mutateModelEntryWithRetry(ctx, first.ChildModelKey, first.ChildModelUID, false, func(data map[string]string) (bool, error) {
			child, err := existingModelEntry(data, first.ChildModelKey)
			if err != nil {
				return false, err
			}
			child.Status = status
			return writeModelEntry(data, first.ChildModelKey, child)
		})
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- writeStatus(ModelStatusUpdating) }()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("first write did not commit")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- writeStatus(ModelStatusReady) }()
	select {
	case err := <-secondDone:
		t.Fatalf("new write overtook unpublished cache state: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	c.reconcileConfigMaps()

	assert.Equal(t, ModelStatusReady, testRepairParent(t, h, first).ChildStatusesBeforeRepair[first.ChildModelKey])
}

func TestHfArtifactRecoverySnapshotWaitsForMutation(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	committed, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	c.kubeClient = hfRecoveryDelayedClient{Interface: c.kubeClient, afterUpdate: func(*corev1.ConfigMap) {
		close(committed)
		select {
		case <-release:
		case <-ctx.Done():
		}
	}}
	writeDone := make(chan error, 1)
	go func() {
		_, err := h.repository.RemoveModelReference(ctx, first.Parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath)
		writeDone <- err
	}()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("reference removal did not commit")
	}
	readDone := make(chan error, 1)
	go func() {
		_, err := c.getConfigMapForRecovery(ctx)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		t.Fatalf("snapshot read overtook unpublished mutation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-writeDone)
	require.NoError(t, <-readDone)
	models, parents := c.cachedConfigMapEntries()
	child, err := existingModelEntry(models, first.ChildModelKey)
	require.NoError(t, err)
	assert.Empty(t, child.HfArtifactKey)
	parent, err := decodeHfArtifactEntry(first.Parent.Key, parents[first.Parent.Key])
	require.NoError(t, err)
	assert.NotContains(t, parent.Children, first.ChildModelKey)
}

func TestHfArtifactRecoveryReleasesCacheLockAcrossCallbacksAndAPI(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "mutation", true: "legacy mutation"}[legacy], func(t *testing.T) {
			h, _, _ := newTestHfArtifactRepair(t)
			c := h.repository.configMaps
			checkCacheUnlocked := func() {
				if c.cacheMutex.TryLock() {
					c.cacheMutex.Unlock()
				} else {
					t.Error("cacheMutex must not be held during callback or API call")
				}
			}
			apiChecked := false
			c.kubeClient = hfRecoveryDelayedClient{Interface: c.kubeClient, afterUpdate: func(*corev1.ConfigMap) {
				apiChecked = true
				checkCacheUnlocked()
			}}
			mutate := func(cm *corev1.ConfigMap) (bool, error) {
				checkCacheUnlocked()
				return writeModelEntry(cm.Data, "default.basemodel.legacy", ModelEntry{Name: "legacy", Status: ModelStatusReady})
			}
			var err error
			if legacy {
				err = c.updateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
					changed, err := mutate(cm)
					return changed, cm, err
				})
			} else {
				err = c.mutateConfigMapWithRetry(context.Background(), mutate)
			}
			require.NoError(t, err)
			assert.True(t, apiChecked)
		})
	}
}

func TestHfArtifactRecoveryConflictDoesNotRestoreAnOldRelationship(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	client := c.kubeClient.(*fake.Clientset)
	ctx := context.Background()
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	delete(cm.Data, first.ChildModelKey)
	_, err = client.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	conflicted := false
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if conflicted {
			return false, nil, nil
		}
		conflicted = true
		resource := corev1.SchemeGroupVersion.WithResource("configmaps")
		obj, err := client.Tracker().Get(resource, c.namespace, c.nodeName)
		require.NoError(t, err)
		latest := obj.(*corev1.ConfigMap).DeepCopy()
		parent, err := decodeHfArtifactEntry(first.Parent.Key, latest.Data[first.Parent.Key])
		require.NoError(t, err)
		delete(parent.Children, first.ChildModelKey)
		_, err = writeHfArtifactEntry(latest.Data, parent)
		require.NoError(t, err)
		_, err = writeModelEntry(latest.Data, first.ChildModelKey, ModelEntry{Name: "replacement", Status: ModelStatusUpdating})
		require.NoError(t, err)
		require.NoError(t, client.Tracker().Update(resource, latest, c.namespace))
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, c.nodeName, errors.New("new child relationship"))
	})

	c.reconcileConfigMaps()

	require.True(t, conflicted)
	parent := testRepairParent(t, h, first)
	assert.Equal(t, map[string]string{second.ChildModelKey: second.ChildModelPath}, parent.Children)
	cm, err = c.getConfigMap(ctx)
	require.NoError(t, err)
	child, err := existingModelEntry(cm.Data, first.ChildModelKey)
	require.NoError(t, err)
	assert.Empty(t, child.HfArtifactKey)
	assert.Equal(t, "replacement", child.Name)
	assert.Equal(t, ModelStatusUpdating, child.Status)
}

func TestHfArtifactRecoveryQuarantinesUnrestorableChildMutation(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx := context.Background()
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	delete(cm.Data, first.ChildModelKey)
	cm.Data[first.Parent.Key] = "{corrupt parent"
	_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)

	err = c.mutateModelEntryWithRetry(ctx, first.ChildModelKey, first.ChildModelUID, false, func(data map[string]string) (bool, error) {
		t.Error("must not overwrite a missing shared child as an unrelated model")
		return writeModelEntry(data, first.ChildModelKey, ModelEntry{Name: "model-1", Status: ModelStatusReady})
	})

	require.Error(t, err)
	after, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	assert.Equal(t, cm.Data, after.Data)
}
