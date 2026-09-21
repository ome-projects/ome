package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOrdinaryConfigMapCacheCompatibility(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	ctx := context.Background()
	model := createTestBaseModelCM()
	key := getModelID(model, nil)
	metadata := ModelMetadata{ModelType: "llm", ModelArchitecture: "transformer"}
	require.NoError(t, c.ReconcileModelMetadata(ctx, &ConfigMapMetadataOp{BaseModel: model, ModelMetadata: metadata}))
	require.NotNil(t, c.modelCache[key])
	assert.Empty(t, c.modelCache[key].ModelStatus)
	assert.Equal(t, &metadata, c.modelCache[key].ModelMetadata)
	assert.Empty(t, c.modelCache[key].ModelEntryJSON)
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusUpdating}))
	require.NoError(t, c.ReconcileModelProgress(ctx, &ConfigMapProgressOp{BaseModel: model, Progress: &DownloadProgress{CompletedBytes: 10}}))
	require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	c.reconcileConfigMaps()
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, key)
	require.NoError(t, err)
	assert.Equal(t, ModelStatusUpdating, entry.Status)
	require.NotNil(t, entry.Config)
	assert.Equal(t, "transformer", entry.Config.ModelArchitecture)
	assert.Nil(t, entry.Progress)
}

func TestOrdinaryConfigMapProgressDoesNotSeedRecovery(t *testing.T) {
	c, _, _ := setupConfigMapTest(t)
	require.NoError(t, c.ReconcileModelProgress(context.Background(), &ConfigMapProgressOp{
		BaseModel: createTestBaseModelCM(), Progress: &DownloadProgress{CompletedBytes: 10},
	}))
	assert.Empty(t, c.modelCache)
}

func TestOrdinaryConfigMapDeletionClearsStaleCachedUID(t *testing.T) {
	for _, scenario := range []string{"local removal", "CR deletion", "local removal after opt-out", "CR deletion after opt-out"} {
		t.Run(scenario, func(t *testing.T) {
			c, _, _ := setupConfigMapTest(t)
			ctx := context.Background()
			model := createTestBaseModelCM()
			model.UID = "old-uid"
			key := getModelID(model, nil)
			require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
			require.Empty(t, c.modelCache[key].ModelEntryJSON)
			if scenario == "local removal after opt-out" || scenario == "CR deletion after opt-out" {
				// A completed shared-to-ordinary transition retains full cached
				// JSON, but no longer has shared ownership or pending cleanup.
				cm, err := c.getConfigMap(ctx)
				require.NoError(t, err)
				c.modelCache[key].ModelEntryJSON = cm.Data[key]
			}
			model.UID = "new-uid"
			if scenario == "CR deletion" || scenario == "CR deletion after opt-out" {
				now := metav1.Now()
				model.DeletionTimestamp = &now
			}
			require.NoError(t, c.DeleteModelFromConfigMap(ctx, model, nil))
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.NotContains(t, cm.Data, key)
			assert.NotContains(t, c.modelCache, key)
		})
	}
}

func TestOrdinaryConfigMapRestoreRecreatesAllCachedModels(t *testing.T) {
	c, _, _ := setupConfigMapTest(t)
	c.modelCache["first"] = &CacheEntry{ModelName: "first", ModelStatus: ModelStatusReady}
	c.modelCache["second"] = &CacheEntry{ModelName: "second", ModelStatus: ModelStatusReady}
	c.restoreModelInConfigMap("first", c.modelCache["first"])
	cm, err := c.getConfigMap(context.Background())
	require.NoError(t, err)
	assert.Contains(t, cm.Data, "first")
	assert.Contains(t, cm.Data, "second")
}

func TestOrdinaryConfigMapWriteDoesNotRestoreCachedModels(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	ctx := context.Background()
	model := createTestBaseModelCM()
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
	require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	other := model.DeepCopy()
	other.Name, other.UID = "other", "other-uid"
	require.NoError(t, c.ReconcileModelProgress(ctx, &ConfigMapProgressOp{BaseModel: other, Progress: &DownloadProgress{CompletedBytes: 1}}))
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	assert.NotContains(t, cm.Data, getModelID(model, nil))
}

func TestOrdinaryConfigMapStartupDoesNotSeedRecovery(t *testing.T) {
	c, _, _ := setupConfigMapTest(t)
	ctx := context.Background()
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: createTestBaseModelCM(), ModelStatus: ModelStatusReady}))
	restarted := NewConfigMapReconciler(c.nodeName, c.namespace, c.kubeClient, c.logger)
	_, err := restarted.getConfigMapForRecovery(ctx)
	require.NoError(t, err)
	assert.Empty(t, restarted.modelCache)
}

func TestOrdinaryConfigMapMixedRecovery(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	ctx := context.Background()
	model := createTestBaseModelCM()
	key := getModelID(model, nil)
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
	assert.Empty(t, c.modelCache[key].ModelEntryJSON)
	restarted := NewConfigMapReconciler(c.nodeName, c.namespace, c.kubeClient, c.logger)
	_, err := restarted.getConfigMapForRecovery(ctx)
	require.NoError(t, err)
	assert.NotContains(t, restarted.modelCache, key)
	assert.Contains(t, restarted.modelCache, first.ChildModelKey)
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
	require.NoError(t, c.ReconcileModelProgress(ctx, &ConfigMapProgressOp{BaseModel: model, Progress: &DownloadProgress{CompletedBytes: 1}}))
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	ordinary, err := existingModelEntry(cm.Data, key)
	require.NoError(t, err)
	assert.Equal(t, ModelStatusUpdating, ordinary.Status, "missing ordinary entry must not inherit cached Ready")
	parent, err := decodeHfArtifactEntry(first.Parent.Key, cm.Data[first.Parent.Key])
	require.NoError(t, err)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.Contains(t, parent.Children, first.ChildModelKey)
}
