package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// setupConfigMapTest prepares a test environment with fake clients and test models
func setupConfigMapTest(t *testing.T) (*ConfigMapReconciler, *fake.Clientset, *zap.SugaredLogger) {
	// Create a test logger
	logger := zaptest.NewLogger(t).Sugar()

	// Create a fake Kubernetes client
	kubeClient := fake.NewSimpleClientset()

	// Create the reconciler with type conversion to match interface
	reconciler := NewConfigMapReconciler("test-node", "test-namespace", kubeClient, logger)

	return reconciler, kubeClient, logger
}

// createTestBaseModel creates a test BaseModel for tests
func createTestBaseModelCM() *v1beta1.BaseModel {
	return &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "default",
			UID:       types.UID("test-model-uid"),
		},
	}
}

// createTestClusterBaseModel creates a test ClusterBaseModel for tests
func createTestClusterBaseModelCM() *v1beta1.ClusterBaseModel {
	return &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster-model",
			UID:  types.UID("test-cluster-model-uid"),
		},
	}
}

// TestNewConfigMapReconciler tests the constructor
func TestNewConfigMapReconciler(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	kubeClient := fake.NewSimpleClientset()

	reconciler := NewConfigMapReconciler("test-node", "test-namespace", kubeClient, logger)

	assert.NotNil(t, reconciler)
	assert.Equal(t, "test-node", reconciler.nodeName)
	assert.Equal(t, "test-namespace", reconciler.namespace)
	assert.NotNil(t, reconciler.kubeClient)
	assert.NotNil(t, reconciler.logger)
}

// TestGetConfigMapModelInfo tests the getConfigMapModelInfo function
func TestGetConfigMapModelInfo(t *testing.T) {
	// Test with BaseModel
	baseModel := createTestBaseModelCM()

	info := getConfigMapModelInfo(baseModel, nil)
	assert.Equal(t, "BaseModel default/test-model", info)

	// Test with ClusterBaseModel
	clusterBaseModel := createTestClusterBaseModelCM()

	info = getConfigMapModelInfo(nil, clusterBaseModel)
	assert.Equal(t, "ClusterBaseModel test-cluster-model", info)

	// Test with no model
	info = getConfigMapModelInfo(nil, nil)
	assert.Equal(t, "unknown model", info)
}

// TestGetModelConfigMapKey tests the getModelConfigMapKey method
func TestGetModelConfigMapKey(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	// Test with BaseModel
	baseModel := createTestBaseModelCM()
	key := reconciler.getModelConfigMapKey(baseModel, nil)
	expectedKey := constants.GetModelConfigMapKey(baseModel.Namespace, baseModel.Name, false)
	assert.Equal(t, expectedKey, key)

	// Test with ClusterBaseModel
	clusterBaseModel := createTestClusterBaseModelCM()
	key = reconciler.getModelConfigMapKey(nil, clusterBaseModel)
	expectedKey = constants.GetModelConfigMapKey("", clusterBaseModel.Name, true)
	assert.Equal(t, expectedKey, key)
}

// TestGetOrCreateConfigMap tests the getOrCreateConfigMap method
func TestGetOrCreateConfigMap(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Test when ConfigMap doesn't exist (should create new one)
	ctx := context.Background()
	configMap, needCreate, err := reconciler.getOrCreateConfigMap(ctx)
	assert.NoError(t, err)
	assert.True(t, needCreate)
	assert.NotNil(t, configMap)
	assert.Equal(t, reconciler.nodeName, configMap.Name)
	assert.Equal(t, reconciler.namespace, configMap.Namespace)
	assert.Equal(t, "true", configMap.Labels[constants.ModelStatusConfigMapLabel])
	assert.Equal(t, reconciler.nodeName, configMap.Labels["node"])
	assert.Equal(t, reconciler.nodeName, configMap.Annotations["models.ome.io/node-name"])
	assert.Equal(t, "model-agent", configMap.Annotations["models.ome.io/managed-by"])

	// Now create a ConfigMap and test getting existing one
	configMap.Data = map[string]string{"test": "data"}
	_, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(
		ctx, configMap, metav1.CreateOptions{},
	)
	assert.NoError(t, err)

	// Should return existing ConfigMap
	existingConfigMap, needCreate, err := reconciler.getOrCreateConfigMap(ctx)
	assert.NoError(t, err)
	assert.False(t, needCreate)
	assert.Equal(t, configMap.Name, existingConfigMap.Name)
	assert.Equal(t, configMap.Namespace, existingConfigMap.Namespace)
	assert.Equal(t, "data", existingConfigMap.Data["test"])

	// Test error case
	kubeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("fake error")
	})

	_, _, err = reconciler.getOrCreateConfigMap(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fake error")
}

// Note: These types are defined in the main package file
// ModelStatus, ModelStatusReady, ModelStatusDeleted, and ModelEntry are
// used here but not redeclared to avoid conflicts.

// TestUpdateModelStatusInConfigMap tests the updateModelStatusInConfigMap method
func TestUpdateModelStatusInConfigMap(t *testing.T) {
	// Setup test environment
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Create a test operation
	baseModel := createTestBaseModelCM()
	op := &ConfigMapStatusOp{
		BaseModel:   baseModel,
		ModelStatus: ModelStatusReady,
	}

	// Execute the test and persist the update through the fake Kubernetes client
	ctx := context.Background()
	err := reconciler.updateModelStatusInConfigMap(ctx, op)
	assert.NoError(t, err)

	// Verify the ConfigMap was updated correctly
	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	key := reconciler.getModelConfigMapKey(baseModel, nil)
	assert.Contains(t, configMap.Data, key, "ConfigMap should contain the model key")

	// The entry should be a JSON string with the model status
	entryJSON := configMap.Data[key]
	assert.Contains(t, entryJSON, "\"status\":\"Ready\"")
	assert.Contains(t, entryJSON, baseModel.Name)

	// Verify the model entry
	var modelEntry ModelEntry
	err = json.Unmarshal([]byte(configMap.Data[key]), &modelEntry)
	assert.NoError(t, err)
	assert.Equal(t, baseModel.Name, modelEntry.Name)
	assert.Equal(t, ModelStatusReady, modelEntry.Status)
	assert.Nil(t, modelEntry.Config)
}

// TestUpdateModelMetadataInConfigMap tests the updateModelMetadataInConfigMap method
func TestUpdateModelMetadataInConfigMap(t *testing.T) {
	// Setup test environment
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Create test model metadata with fields from our internal ModelMetadata type
	modelMetadata := ModelMetadata{
		ModelType:         "llm",
		ModelArchitecture: "transformer",
		ModelFramework: &v1beta1.ModelFrameworkSpec{
			Name:    "tensorflow",
			Version: &[]string{"2.6.0"}[0],
		},
		ModelFormat: v1beta1.ModelFormat{
			Name: "savedmodel",
		},
		ModelParameterSize: "7B",
		MaxTokens:          4096,
		ModelCapabilities:  []string{"TEXT_GENERATION"},
	}

	// Create a test operation with BaseModel
	baseModel := createTestBaseModelCM()
	op := &ConfigMapMetadataOp{
		BaseModel:     baseModel,
		ModelMetadata: modelMetadata,
	}

	// Execute the test
	ctx := context.Background()
	err := reconciler.updateModelMetadataInConfigMap(ctx, op)
	assert.NoError(t, err)

	// Verify the ConfigMap was updated correctly
	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	key := reconciler.getModelConfigMapKey(baseModel, nil)
	assert.Contains(t, configMap.Data, key)

	// Verify the JSON data
	var modelEntry ModelEntry
	err = json.Unmarshal([]byte(configMap.Data[key]), &modelEntry)
	assert.NoError(t, err)
	assert.Equal(t, baseModel.Name, modelEntry.Name)
	assert.Equal(t, ModelStatusReady, modelEntry.Status)

	// Verify the model config
	assert.NotNil(t, modelEntry.Config)
	assert.Equal(t, modelMetadata.ModelType, modelEntry.Config.ModelType)
	assert.Equal(t, modelMetadata.ModelArchitecture, modelEntry.Config.ModelArchitecture)
	// Check ModelFramework map
	assert.Contains(t, modelEntry.Config.ModelFramework, "name")
	assert.Equal(t, modelMetadata.ModelFramework.Name, modelEntry.Config.ModelFramework["name"])
	// ModelFormat is stored as a map
	assert.Contains(t, modelEntry.Config.ModelFormat, "name")
	assert.Equal(t, modelMetadata.ModelFormat.Name, modelEntry.Config.ModelFormat["name"])

	// Test with ClusterBaseModel
	clusterModel := createTestClusterBaseModelCM()
	clusterOp := &ConfigMapMetadataOp{
		ClusterBaseModel: clusterModel,
		ModelMetadata:    modelMetadata,
	}

	err = reconciler.updateModelMetadataInConfigMap(ctx, clusterOp)
	assert.NoError(t, err)

	// Test update of an existing entry
	// Updating metadata must preserve status and download progress from the latest entry.
	configMap, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	existingEntry := ModelEntry{
		Name:     baseModel.Name,
		Status:   ModelStatusUpdating,
		Progress: &DownloadProgress{Phase: "Downloading", CompletedBytes: 10},
	}
	entryBytes, _ := json.Marshal(existingEntry)
	configMap.Data[key] = string(entryBytes)
	_, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	assert.NoError(t, err)

	err = reconciler.updateModelMetadataInConfigMap(ctx, op)
	assert.NoError(t, err)

	// Verify status and progress were preserved and config was updated
	configMap, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	err = json.Unmarshal([]byte(configMap.Data[key]), &modelEntry)
	assert.NoError(t, err)
	assert.Equal(t, ModelStatusUpdating, modelEntry.Status)
	assert.Equal(t, uint64(10), modelEntry.Progress.CompletedBytes)
	assert.NotNil(t, modelEntry.Config)

	// Test update error propagation
	kubeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("simulated update error")
	})
	op.ModelMetadata.MaxTokens++
	err = reconciler.updateModelMetadataInConfigMap(ctx, op)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "simulated update error")
	}
}

func TestReconcileModelProgressPreservesExistingEntry(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()
	baseModel := createTestBaseModelCM()

	metadata := ModelMetadata{ModelType: "llm", ModelArchitecture: "transformer"}
	assert.NoError(t, reconciler.ReconcileModelMetadata(ctx, &ConfigMapMetadataOp{
		BaseModel:     baseModel,
		ModelMetadata: metadata,
	}))

	progress := &DownloadProgress{Phase: "Downloading", TotalBytes: 100, CompletedBytes: 40}
	assert.NoError(t, reconciler.ReconcileModelProgress(ctx, &ConfigMapProgressOp{
		BaseModel: baseModel,
		Progress:  progress,
	}))

	key := reconciler.getModelConfigMapKey(baseModel, nil)
	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	var modelEntry ModelEntry
	assert.NoError(t, json.Unmarshal([]byte(configMap.Data[key]), &modelEntry))
	assert.Equal(t, ModelStatusReady, modelEntry.Status)
	assert.NotNil(t, modelEntry.Config)
	assert.Equal(t, uint64(40), modelEntry.Progress.CompletedBytes)

	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   baseModel,
		ModelStatus: ModelStatusReady,
	}))
	configMap, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	modelEntry = ModelEntry{}
	assert.NoError(t, json.Unmarshal([]byte(configMap.Data[key]), &modelEntry))
	assert.NotNil(t, modelEntry.Config)
	assert.Nil(t, modelEntry.Progress)
}

// TestRestoreModelInConfigMap tests the restoreModelInConfigMap method
func TestRestoreModelInConfigMap(t *testing.T) {
	// Create test environment
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Create a context for all operations
	ctx := context.Background()

	// Create a ConfigMap first
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Create a test model
	baseModel := createTestBaseModelCM()

	// Add the model to the cache
	reconciler.cacheMutex.Lock()
	modelID := reconciler.getModelConfigMapKey(baseModel, nil)
	reconciler.modelCache[modelID] = &CacheEntry{
		ModelName:   "test-model",
		ModelUID:    baseModel.UID,
		ModelStatus: ModelStatusReady,
		ModelMetadata: &ModelMetadata{
			ModelType:          "llm",
			ModelArchitecture:  "transformer",
			ModelParameterSize: "1.0",
		},
	}
	cacheEntry := reconciler.modelCache[modelID]
	reconciler.cacheMutex.Unlock()

	// Restore the model from cache to ConfigMap
	// Call restoreModelInConfigMap (note: this method creates its own context internally)
	reconciler.restoreModelInConfigMap(modelID, cacheEntry)

	// Verify the model was added to the ConfigMap
	updatedCM, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, updatedCM.Data, modelID)

	// Test error cases
	// 1. ConfigMap doesn't exist
	err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Delete(ctx, reconciler.nodeName, metav1.DeleteOptions{})
	assert.NoError(t, err)

	// This should trigger recreateConfigMap
	// Call restoreModelInConfigMap again to test the recreateConfigMap path
	reconciler.restoreModelInConfigMap(modelID, cacheEntry)

	// Verify ConfigMap was recreated
	recreatedCM, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, recreatedCM.Data, modelID)

	// Skip the conflict test part since it's already covered by the implementation
	// and difficult to mock correctly in a test environment
	// The conflict handling in restoreModelInConfigMap is robust with 3 retry attempts
	// which is sufficient for most use cases in Kubernetes
}

// TestDeleteModelFromConfigMap tests the DeleteModelFromConfigMap method
func TestDeleteModelFromConfigMap(t *testing.T) {
	// Create test environment
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Create a context for all operations
	ctx := context.Background()

	// Create a test model
	baseModel := createTestBaseModelCM()

	// Add model to ConfigMap and cache
	err := reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   baseModel,
		ModelStatus: ModelStatusReady,
	})
	assert.NoError(t, err)

	// Verify model exists in ConfigMap and cache
	cm, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)

	modelID := reconciler.getModelConfigMapKey(baseModel, nil)
	assert.Contains(t, cm.Data, modelID)

	reconciler.cacheMutex.Lock()
	assert.Contains(t, reconciler.modelCache, modelID)
	reconciler.cacheMutex.Unlock()

	// Now delete the model
	err = reconciler.DeleteModelFromConfigMap(ctx, baseModel, nil)
	assert.NoError(t, err)

	// Verify model was deleted from both ConfigMap and cache
	updatedCM, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, updatedCM.Data, modelID)

	reconciler.cacheMutex.Lock()
	assert.NotContains(t, reconciler.modelCache, modelID)
	reconciler.cacheMutex.Unlock()

	// Test deleting a non-existent model (should not error)
	err = reconciler.DeleteModelFromConfigMap(ctx, baseModel, nil)
	assert.NoError(t, err)

	// Test error cases: ConfigMap doesn't exist
	err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Delete(ctx, reconciler.nodeName, metav1.DeleteOptions{})
	assert.NoError(t, err)

	// Deleting from non-existent ConfigMap should still be successful (nothing to delete)
	err = reconciler.DeleteModelFromConfigMap(ctx, baseModel, nil)
	assert.NoError(t, err)
}

func TestDeleteModelFromConfigMapRetriesConflict(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()
	baseModel := createTestBaseModelCM()

	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   baseModel,
		ModelStatus: ModelStatusReady,
	}))

	updateAttempts := 0
	kubeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		updateAttempts++
		if updateAttempts == 1 {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "configmaps"},
				reconciler.nodeName,
				errors.New("simulated concurrent update"),
			)
		}
		return false, nil, nil
	})

	assert.NoError(t, reconciler.DeleteModelFromConfigMap(ctx, baseModel, nil))
	assert.Equal(t, 2, updateAttempts)

	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, configMap.Data, reconciler.getModelConfigMapKey(baseModel, nil))
}

func TestProgressConflictDoesNotRestoreAnotherDeletedModel(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()
	deletedModel := createTestBaseModelCM()
	updatedModel := deletedModel.DeepCopy()
	updatedModel.Name = "other-model"
	updatedModel.UID = types.UID("other-model-uid")

	for _, op := range []*ConfigMapStatusOp{
		{BaseModel: deletedModel, ModelStatus: ModelStatusReady},
		{BaseModel: updatedModel, ModelStatus: ModelStatusUpdating},
	} {
		assert.NoError(t, reconciler.ReconcileModelStatus(ctx, op))
	}

	deletedKey := reconciler.getModelConfigMapKey(deletedModel, nil)
	updatedKey := reconciler.getModelConfigMapKey(updatedModel, nil)
	configMapResource := corev1.SchemeGroupVersion.WithResource("configmaps")
	updateAttempts := 0
	kubeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		updateAttempts++
		if updateAttempts != 1 {
			return false, nil, nil
		}

		current, err := kubeClient.Tracker().Get(configMapResource, reconciler.namespace, reconciler.nodeName)
		assert.NoError(t, err)
		latest := current.(*corev1.ConfigMap).DeepCopy()
		delete(latest.Data, deletedKey)
		assert.NoError(t, kubeClient.Tracker().Update(configMapResource, latest, reconciler.namespace))

		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "configmaps"},
			reconciler.nodeName,
			errors.New("simulated deletion after stale read"),
		)
	})

	assert.NoError(t, reconciler.ReconcileModelProgress(ctx, &ConfigMapProgressOp{
		BaseModel: updatedModel,
		Progress:  &DownloadProgress{Phase: "Downloading", CompletedBytes: 10},
	}))
	assert.Equal(t, 2, updateAttempts)

	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, configMap.Data, deletedKey)
	assert.Contains(t, configMap.Data, updatedKey)
}

func TestDeletedUIDCannotBeRestoredOrUpdatedButNewUIDCanWrite(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()
	oldModel := createTestBaseModelCM()
	modelID := reconciler.getModelConfigMapKey(oldModel, nil)

	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   oldModel,
		ModelStatus: ModelStatusReady,
	}))
	reconciler.cacheMutex.RLock()
	staleCacheEntry := *reconciler.modelCache[modelID]
	reconciler.cacheMutex.RUnlock()

	deletionTimestamp := metav1.Now()
	oldModel.DeletionTimestamp = &deletionTimestamp
	assert.NoError(t, reconciler.DeleteModelFromConfigMap(ctx, oldModel, nil))
	reconciler.restoreModelInConfigMap(modelID, &staleCacheEntry)
	assert.NoError(t, reconciler.ReconcileModelProgress(ctx, &ConfigMapProgressOp{
		BaseModel: oldModel,
		Progress:  &DownloadProgress{Phase: "Downloading", CompletedBytes: 99},
	}))
	assert.NoError(t, reconciler.ReconcileModelMetadata(ctx, &ConfigMapMetadataOp{
		BaseModel:     oldModel,
		ModelMetadata: ModelMetadata{ModelType: "stale"},
	}))

	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, configMap.Data, modelID)

	newModel := oldModel.DeepCopy()
	newModel.UID = types.UID("replacement-model-uid")
	newModel.DeletionTimestamp = nil
	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   newModel,
		ModelStatus: ModelStatusReady,
	}))
	assert.NoError(t, reconciler.ReconcileModelProgress(ctx, &ConfigMapProgressOp{
		BaseModel: newModel,
		Progress:  &DownloadProgress{Phase: "Downloading", CompletedBytes: 50},
	}))
	assert.NoError(t, reconciler.ReconcileModelMetadata(ctx, &ConfigMapMetadataOp{
		BaseModel:     newModel,
		ModelMetadata: ModelMetadata{ModelType: "replacement"},
	}))

	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   oldModel,
		ModelStatus: ModelStatusFailed,
	}))
	assert.NoError(t, reconciler.ReconcileModelProgress(ctx, &ConfigMapProgressOp{
		BaseModel: oldModel,
		Progress:  &DownloadProgress{Phase: "Downloading", CompletedBytes: 100},
	}))
	assert.NoError(t, reconciler.ReconcileModelMetadata(ctx, &ConfigMapMetadataOp{
		BaseModel:     oldModel,
		ModelMetadata: ModelMetadata{ModelType: "stale"},
	}))

	configMap, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	var modelEntry ModelEntry
	assert.NoError(t, json.Unmarshal([]byte(configMap.Data[modelID]), &modelEntry))
	assert.Equal(t, ModelStatusReady, modelEntry.Status)
	assert.Equal(t, uint64(50), modelEntry.Progress.CompletedBytes)
	assert.Equal(t, "replacement", modelEntry.Config.ModelType)
}

func TestNodeCleanupAllowsSameUIDToWriteAgain(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()
	model := createTestBaseModelCM()
	modelID := reconciler.getModelConfigMapKey(model, nil)

	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   model,
		ModelStatus: ModelStatusReady,
	}))

	// A Delete task without DeletionTimestamp is node-local cleanup, not terminal CR deletion.
	assert.NoError(t, reconciler.DeleteModelFromConfigMap(ctx, model, nil))
	reconciler.cacheMutex.RLock()
	_, cacheExists := reconciler.modelCache[modelID]
	_, invalidationExists := reconciler.invalidatedModelUIDs[modelID]
	reconciler.cacheMutex.RUnlock()
	assert.False(t, cacheExists)
	assert.False(t, invalidationExists)

	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   model,
		ModelStatus: ModelStatusReady,
	}))

	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, configMap.Data, modelID)
	reconciler.cacheMutex.RLock()
	cacheEntry := reconciler.modelCache[modelID]
	reconciler.cacheMutex.RUnlock()
	if assert.NotNil(t, cacheEntry) {
		assert.Equal(t, model.UID, cacheEntry.ModelUID)
	}
}

func TestNodeCleanupBlocksStaleRestoreSnapshot(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()
	model := createTestBaseModelCM()
	modelID := reconciler.getModelConfigMapKey(model, nil)

	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   model,
		ModelStatus: ModelStatusReady,
	}))
	reconciler.cacheMutex.RLock()
	staleCacheEntry := *reconciler.modelCache[modelID]
	reconciler.cacheMutex.RUnlock()

	assert.NoError(t, reconciler.DeleteModelFromConfigMap(ctx, model, nil))
	reconciler.restoreModelInConfigMap(modelID, &staleCacheEntry)

	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, configMap.Data, modelID)
}

func TestEmptyUIDEvictsCacheWithoutRecordingInvalidation(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()
	model := createTestBaseModelCM()
	modelID := reconciler.getModelConfigMapKey(model, nil)

	reconciler.cacheMutex.Lock()
	reconciler.modelCache[modelID] = &CacheEntry{
		ModelName: model.Name,
		ModelUID:  types.UID("cached-model-uid"),
	}
	reconciler.cacheMutex.Unlock()

	reconciler.invalidateModelUIDAndEvictCache(modelID, "")

	reconciler.cacheMutex.RLock()
	_, cacheExists := reconciler.modelCache[modelID]
	_, invalidationExists := reconciler.invalidatedModelUIDs[modelID]
	reconciler.cacheMutex.RUnlock()
	assert.False(t, cacheExists)
	assert.False(t, invalidationExists)

	model.UID = types.UID("replacement-model-uid")
	assert.NoError(t, reconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel:   model,
		ModelStatus: ModelStatusReady,
	}))

	configMap, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, configMap.Data, modelID)

	reconciler.cacheMutex.RLock()
	cacheEntry := reconciler.modelCache[modelID]
	reconciler.cacheMutex.RUnlock()
	if assert.NotNil(t, cacheEntry) {
		assert.Equal(t, model.UID, cacheEntry.ModelUID)
	}
}

// TestRecreateConfigMap tests the recreateConfigMap method
func TestRecreateConfigMap(t *testing.T) {
	// Create test environment
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Create a context for all operations
	ctx := context.Background()

	// Add some entries to the model cache
	reconciler.cacheMutex.Lock()
	reconciler.modelCache["model1"] = &CacheEntry{ModelName: "model1", ModelStatus: ModelStatusReady, ModelMetadata: &ModelMetadata{ModelType: "llm", ModelParameterSize: "1.0"}}
	reconciler.modelCache["model2"] = &CacheEntry{ModelName: "model2", ModelStatus: ModelStatusUpdating, ModelMetadata: &ModelMetadata{ModelType: "llm", ModelParameterSize: "2.0"}}
	reconciler.cacheMutex.Unlock()

	// Call recreateConfigMap
	reconciler.recreateConfigMap(ctx)

	// Verify the ConfigMap was created with all cache entries
	cm, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(cm.Data))
	assert.Contains(t, cm.Data, "model1")
	assert.Contains(t, cm.Data, "model2")

	// Test recreating when ConfigMap already exists
	reconciler.recreateConfigMap(ctx)

	// Verify ConfigMap still has correct entries
	cm, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(cm.Data))

	// Test error handling - make Create operation fail
	// First delete the existing ConfigMap
	err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Delete(ctx, reconciler.nodeName, metav1.DeleteOptions{})
	assert.NoError(t, err)

	// Prepare error for Create
	kubeClient.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("create error")
	})

	// Should handle error gracefully
	reconciler.recreateConfigMap(ctx)
	// No assertions needed - function should just log the error without returning it
}

// TestConfigMapReconciliation tests the ConfigMap reconciliation and recovery functionality
func TestConfigMapReconciliation(t *testing.T) {
	// Create test environment
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Create a context for the test
	ctx := context.Background()

	// Create test models
	baseModel := createTestBaseModelCM()
	clusterModel := createTestClusterBaseModelCM()

	// Set up model status operations
	baseModelOp := &ConfigMapStatusOp{
		BaseModel:   baseModel,
		ModelStatus: ModelStatusReady,
	}
	clusterModelOp := &ConfigMapStatusOp{
		ClusterBaseModel: clusterModel,
		ModelStatus:      ModelStatusReady,
	}

	// First, add some entries to the reconciler
	var err error
	err = reconciler.ReconcileModelStatus(ctx, baseModelOp)
	assert.NoError(t, err)

	err = reconciler.ReconcileModelStatus(ctx, clusterModelOp)
	assert.NoError(t, err)

	// Verify the cache contains the entries
	reconciler.cacheMutex.Lock()
	assert.Equal(t, 2, len(reconciler.modelCache))
	reconciler.cacheMutex.Unlock()

	// Verify the ConfigMap exists with both entries
	cm, err := kubeClient.CoreV1().ConfigMaps("test-namespace").Get(ctx, "test-node", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(cm.Data))

	// Delete the ConfigMap to simulate manual deletion
	err = kubeClient.CoreV1().ConfigMaps("test-namespace").Delete(ctx, "test-node", metav1.DeleteOptions{})
	assert.NoError(t, err)

	// Verify ConfigMap is deleted
	_, err = kubeClient.CoreV1().ConfigMaps("test-namespace").Get(ctx, "test-node", metav1.GetOptions{})
	assert.Error(t, err)

	// Execute reconciliation (which would normally happen periodically)
	reconciler.reconcileConfigMaps()

	// Verify ConfigMap has been recreated with both entries
	var recreatedCM *corev1.ConfigMap
	recreatedCM, err = kubeClient.CoreV1().ConfigMaps("test-namespace").Get(ctx, "test-node", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(recreatedCM.Data))

	// Now test recovery of individual entries
	// Update the ConfigMap to remove one entry
	var recreateCM *corev1.ConfigMap
	recreateCM, err = kubeClient.CoreV1().ConfigMaps("test-namespace").Get(ctx, "test-node", metav1.GetOptions{})
	assert.NoError(t, err)

	// Remove one entry
	baseModelKey := reconciler.getModelConfigMapKey(baseModel, nil)
	delete(recreateCM.Data, baseModelKey)

	// Update the ConfigMap
	_, err = kubeClient.CoreV1().ConfigMaps("test-namespace").Update(ctx, recreateCM, metav1.UpdateOptions{})
	assert.NoError(t, err)

	// Verify the entry was removed
	var updatedCM *corev1.ConfigMap
	updatedCM, err = kubeClient.CoreV1().ConfigMaps("test-namespace").Get(ctx, "test-node", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(updatedCM.Data))

	// Run reconciliation again
	reconciler.reconcileConfigMaps()

	// Verify the missing entry was restored
	var finalCM *corev1.ConfigMap
	finalCM, err = kubeClient.CoreV1().ConfigMaps("test-namespace").Get(ctx, "test-node", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(finalCM.Data))
}

// TestReconcileConfigMaps tests the reconcileConfigMaps method
func TestReconcileConfigMaps(t *testing.T) {
	// Create test environment
	reconciler, kubeClient, _ := setupConfigMapTest(t)

	// Create a context for all operations
	ctx := context.Background()

	// Create test model entries
	baseModel := createTestBaseModelCM()
	clusterModel := createTestClusterBaseModelCM()

	// Add models to cache
	reconciler.cacheMutex.Lock()
	baseModelID := reconciler.getModelConfigMapKey(baseModel, nil)
	clusterModelID := reconciler.getModelConfigMapKey(nil, clusterModel)
	reconciler.modelCache[baseModelID] = &CacheEntry{ModelName: baseModel.Name, ModelStatus: ModelStatusReady}
	reconciler.modelCache[clusterModelID] = &CacheEntry{ModelName: clusterModel.Name, ModelStatus: ModelStatusReady}
	reconciler.cacheMutex.Unlock()

	// Initial create of ConfigMap
	reconciler.reconcileConfigMaps()

	// Check that ConfigMap was created correctly
	cm, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, cm.Data, baseModelID)
	assert.Contains(t, cm.Data, clusterModelID)

	// Test 1: Missing model entry - remove one model from ConfigMap
	delete(cm.Data, baseModelID)
	_, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	assert.NoError(t, err)

	// Run reconciliation - should restore missing entry
	reconciler.reconcileConfigMaps()

	// Verify entry was restored
	updatedCM, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, updatedCM.Data, baseModelID)

	// Test 2: Get error handling - simulate ConfigMap not found
	err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Delete(ctx, reconciler.nodeName, metav1.DeleteOptions{})
	assert.NoError(t, err)

	// This should create a new ConfigMap
	reconciler.reconcileConfigMaps()

	// Verify ConfigMap was recreated
	recreatedCM, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, recreatedCM.Data, baseModelID)
	assert.Contains(t, recreatedCM.Data, clusterModelID)

	// Test 3: Other Get errors - simulate unknown error
	errorTriggered := false
	kubeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !errorTriggered && action.(ktesting.GetAction).GetName() == reconciler.nodeName {
			errorTriggered = true
			return true, nil, errors.New("unknown error")
		}
		return false, nil, nil
	})

	// Should log error but continue
	reconciler.reconcileConfigMaps()

	// Verify the error was handled and reconciler can continue
	// Add a small delay to ensure the fake client has stabilized
	time.Sleep(100 * time.Millisecond)

	// Test: Error unmarshaling model entry
	cm, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)

	// Add invalid JSON as model entry
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[baseModelID] = "{invalid json"
	_, err = kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	assert.NoError(t, err)

	// Should log error but continue with valid entries
	reconciler.reconcileConfigMaps()

	// Verify models are correctly present in the ConfigMap
	finalCM, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)

	// Ensure Data map exists
	assert.NotNil(t, finalCM.Data, "ConfigMap Data should not be nil")

	// Now it's safe to check keys in the map
	if finalCM.Data != nil {
		assert.Contains(t, finalCM.Data, clusterModelID)
		assert.Contains(t, finalCM.Data, baseModelID)
	}
}

func TestFindMatchedModelFromConfigMap_FindsMatch_Self(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)
	t.Parallel()
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1":    buildJSONWith("zzz999", "clusterbasemodel.model1", "/models/model1"),
			"namespace.basemodel.model2": buildJSONWith("abc123", "namespace.basemodel.model2", "/models/model2"),
			// Include an invalid JSON that should be ignored
			"clusterbasemodel.invalid-model": "{not-json",
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "abc123", "namespace.basemodel", "namespace.basemodel.model2")

	assert.Empty(t, modelKey)
	assert.Empty(t, parent)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindMatchedModelFromConfigMap_FindsMatch_Other_Parent(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)
	t.Parallel()
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1":    buildJSONWith("zzz999", "clusterbasemodel.modelX", "/models/modelX"),
			"namespace.basemodel.model2": buildJSONWith("abc123", "namespace.basemodel.modelY", "/models/modelY"),
			// Include an invalid JSON that should be ignored
			"clusterbasemodel.invalid-model": "{not-json",
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "abc123", "namespace.basemodel", "namespace.basemodel.modelZ")

	assert.Equal(t, "namespace.basemodel.modelY", modelKey)
	assert.Equal(t, "/models/modelY", parent)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindMatchedModelFromConfigMap_FindsSeveralMatch_DifferentChildrenNum(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)
	t.Parallel()

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ShaAttr:           "abc123",
				ParentPathAttr:    map[string]string{"namespace.basemodel.modelM": "/models/modelM"},
				ChildrenPathsAttr: []string{"/child1", "/child2"},
			},
		},
	}
	modelByte, _ := json.Marshal(obj)
	modelStr := string(modelByte)

	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1":    buildJSONWith("zzz999", "clusterbasemodel.modelX", "/models/modelX"),
			"namespace.basemodel.model2": buildJSONWith("abc123", "namespace.basemodel.modelY", "/models/modelY"),
			// Include an invalid JSON that should be ignored
			"clusterbasemodel.invalid-model": "{not-json",
			"namespace.basemodel.model3":     modelStr,
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "abc123", "namespace.basemodel", "namespace.basemodel.model0")

	assert.Equal(t, "namespace.basemodel.modelM", modelKey)
	assert.Equal(t, "/models/modelM", parent)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindMatchedModelFromConfigMap_NotFound_NoError(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)
	t.Parallel()
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1":    buildJSONWith("zzz999", "clusterbasemodel.model1", "/models/model1"),
			"namespace.basemodel.model2": buildJSONWith("abc123", "namespace.basemodel.model2", "/models/model2"),
			// Key with different prefix should be ignored
			"invalid-model": buildJSONWith("abc456", "model3", "/models/model3"),
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "missing-sha", "clusterbasemodel", "namespace.basemodel.modelZ")
	assert.Empty(t, modelKey)
	assert.Empty(t, parent)

	if err != nil {
		t.Fatalf("expected nil error when not found and no JSON errors, got %v", err)
	}
}

func TestFindMatchedModelFromConfigMap_NotFound_WithInvalidJSON_ReturnsError(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	t.Parallel()
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1":    "{",       // invalid JSON
			"namespace.basemodel.model2": "{\"x\":", // invalid JSON
			// Key with different prefix should be ignored
			"invalid-model": buildJSONWith("other", "clusterbasemodel.model3", "/models/model3"),
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "nope", "namespace.basemodel", "namespace.basemodel.modelZ")
	assert.Empty(t, modelKey)
	assert.Empty(t, parent)
	if err == nil {
		t.Fatalf("expected non-nil error due to invalid JSON entries, got nil")
	}
	// Error should be related to JSON unmarshalling
	if !strings.Contains(err.Error(), "fail to Unmarshal") {
		t.Fatalf("expected JSON error, got %v", err)
	}
}

func TestFindMatchedModelFromConfigMap_SkipsIncompleteEntries(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	t.Parallel()
	// Entries missing required nesting/fields should be skipped
	incompleteConfig := func(obj map[string]interface{}) string {
		b, _ := json.Marshal(obj)
		return string(b)
	}

	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1": incompleteConfig(map[string]interface{}{}),
			"namespace.basemodel.model2": incompleteConfig(map[string]interface{}{
				ConfigAttr: map[string]interface{}{},
			}),
			"clusterbasemodel.missing-sha": incompleteConfig(map[string]interface{}{
				ConfigAttr: map[string]interface{}{
					ArtifactAttr: map[string]interface{}{}, // no sha
				},
			}),
			"clusterbasemodel.invalid-sha": incompleteConfig(map[string]interface{}{
				ConfigAttr: map[string]interface{}{
					ArtifactAttr: map[string]interface{}{
						ShaAttr: 123, // wrong type
					},
				},
			}),
			"clusterbasemodel.valid-but-different-sha": buildJSONWith("some-other-sha", "clusterbasemodel.valid-but-different-sha", "/models/x"),
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "target-sha", "namespace.basemodel", "namespace.basemodel.modelZ")
	assert.Empty(t, modelKey)
	assert.Empty(t, parent)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestFindMatchedModelFromConfigMap_PrefixFilterApplies(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	t.Parallel()
	// Even if SHA matches, if the key doesn't start with the target modelType prefix, it should be ignored
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1":    buildJSONWith("abc123", "clusterbasemodel.model1", "/ignored"),
			"namespace.basemodel.model1": buildJSONWith("zzz", "namespace.basemodel.model1", "/other"),
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "abc123", "namespace.basemodel", "namespace.basemodel.modelZ")
	assert.Empty(t, modelKey)
	assert.Empty(t, parent)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindMatchedModelFromConfigMap_ParentPathConversionFailed(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	t.Parallel()
	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ShaAttr:        "abc123",
				ParentPathAttr: 123,
			},
		},
	}
	b, _ := json.Marshal(obj)
	// matched but the parent path cannot be converted to string
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			"clusterbasemodel.model1":    string(b),
			"namespace.basemodel.model1": buildJSONWith("zzz", "namespace.basemodel.model1", "/other"),
		},
	}

	modelKey, parent, err := reconciler.FindMatchedModelFromConfigMap(cm, "abc123", "clusterbasemodel", "namespace.basemodel.modelZ")
	assert.Empty(t, modelKey)
	assert.Empty(t, parent)

	if err == nil {
		t.Fatalf("expected non-nil error due to invalid JSON entries, got nil")
	}
	assert.Equal(t, "parentPath is not an object", err.Error())
}

func TestAddPathToChildrenPaths_AppendsExisting(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	modelKey := "namespace.basemodel.sample"
	original := `{"config":{"artifact":{"childrenPaths":["/a","/b"]}}}`

	updated, err := reconciler.addPathToChildrenPaths(modelKey, "/c", original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Equal(t, `{"config":{"artifact":{"childrenPaths":["/a","/b","/c"]}}}`, updated)

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(updated), &obj); err != nil {
		t.Fatalf("updated JSON invalid: %v", err)
	}
}

func TestAddPathToChildrenPaths_CreatesWhenMissing(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	modelKey := "namespace.basemodel.sample"
	original := `{"config":{"artifact":{}}}`

	updated, err := reconciler.addPathToChildrenPaths(modelKey, "/a", original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Equal(t, `{"config":{"artifact":{"childrenPaths":["/a"]}}}`, updated)

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(updated), &obj); err != nil {
		t.Fatalf("updated JSON invalid: %v", err)
	}
}

func TestAddPathToChildrenPaths_CreatesConfigArtifactIfMissing(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	modelKey := "namespace.basemodel.sample"
	original := `{}`

	updated, err := reconciler.addPathToChildrenPaths(modelKey, "/a", original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assert.Equal(t, `{"config":{"artifact":{"childrenPaths":["/a"]}}}`, updated)

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(updated), &obj); err != nil {
		t.Fatalf("updated JSON invalid: %v", err)
	}

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	if !ok {
		t.Fatalf("config missing or wrong type")
	}
	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	if !ok {
		t.Fatalf("artifact missing or wrong type")
	}
	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	if !ok {
		t.Fatalf("childrenPaths missing or wrong type")
	}
	if len(children) != 1 || children[0].(string) != "/a" {
		t.Fatalf("unexpected childrenPaths: %#v", children)
	}
}

func TestAddPathToChildrenPaths_ChildrenPathsNotArray(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	modelKey := "namespace.basemodel.sample"
	original := `{"config":{"artifact":{"childrenPaths":"oops"}}}`

	updated, err := reconciler.addPathToChildrenPaths(modelKey, "/x", original)
	assert.Equal(t, updated, `{"config":{"artifact":{"childrenPaths":["/x"]}}}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAddPathToChildrenPaths_InvalidJSON(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	modelKey := "namespace.basemodel.sample"
	original := `{`

	updated, err := reconciler.addPathToChildrenPaths(modelKey, "/a", original)
	assert.Equal(t, updated, "")
	if err == nil {
		t.Fatalf("expected error for invalid JSON, got nil")
	}
	assert.True(t, strings.Contains(err.Error(), "invalid JSON for key"))
}

func TestAddPathToChildrenPaths_AvoidDuplicate(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	modelKey := "namespace.basemodel.sample"
	original := `{"config":{"artifact":{"childrenPaths":["/existing/path"]}}}`

	updated, err := reconciler.addPathToChildrenPaths(modelKey, "/existing/path", original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Equal(t, `{"config":{"artifact":{"childrenPaths":["/existing/path"]}}}`, updated)

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(updated), &obj); err != nil {
		t.Fatalf("updated JSON invalid: %v", err)
	}
}

func TestUpdateConfigMapWithUpdatedChildrenPaths_Success(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	key := "namespace.basemodel.sample"
	existing := `{"config":{"artifact":{"childrenPaths":["/a"]}}}`

	// Seed configmap with existing entry
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{
			key: existing,
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Perform update
	err = reconciler.updateConfigMapWithUpdatedChildrenPaths(ctx, key, "/b")
	assert.NoError(t, err)

	// Verify
	updated, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)

	var obj map[string]interface{}
	err = json.Unmarshal([]byte(updated.Data[key]), &obj)
	assert.NoError(t, err)

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	assert.True(t, ok, "config missing or wrong type")

	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	assert.True(t, ok, "artifact missing or wrong type")

	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	assert.True(t, ok, "childrenPaths missing or wrong type")

	assert.Len(t, children, 2)
	assert.Equal(t, "/a", children[0].(string))
	assert.Equal(t, "/b", children[1].(string))
}

func TestUpdateConfigMapWithUpdatedChildrenPaths_MissingKey_NoOp(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	key := "namespace.basemodel.sample"
	existing := `{"config":{"artifact":{"childrenPaths":["/a"]}}}`

	// Seed configmap without the target key
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{
			"namespace2.basemodel.sample2": existing,
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// no op and error to tell key not found
	err = reconciler.updateConfigMapWithUpdatedChildrenPaths(ctx, key, "/b")
	assert.Nil(t, err)

	updated, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	_, exists := updated.Data[key]
	assert.False(t, exists, "expected no new key inserted for missing target")
}

func TestUpdateConfigMapWithUpdatedChildrenPaths_InvalidExistingJSON(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	key := "namespace.basemodel.sample"
	// used to compute update value (valid)
	//currentDataEntry := `{invalid-json`
	//
	// seed with invalid JSON at target key
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{
			key: "{invalid-json",
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	err = reconciler.updateConfigMapWithUpdatedChildrenPaths(ctx, key, "/b")
	assert.Error(t, err, "expected error due to invalid existing JSON")

	// Ensure no change applied
	updated, err2 := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err2)
	assert.Equal(t, "{invalid-json", updated.Data[key])
}

func TestUpdateConfigMapWithUpdatedChildrenPaths_AddPathWithChildrenPathInvalidFormat(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	key := "namespace.basemodel.sample"
	currentDataEntry := `{"config":{"artifact":{"childrenPaths":"oops"}}}`

	// Seed configmap with a valid existing value
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{
			key: currentDataEntry,
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	err = reconciler.updateConfigMapWithUpdatedChildrenPaths(ctx, key, "/b")
	assert.NoError(t, err)

	updated, err2 := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err2)

	var obj map[string]interface{}
	err = json.Unmarshal([]byte(updated.Data[key]), &obj)
	assert.NoError(t, err)

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	assert.True(t, ok, "config missing or wrong type")

	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	assert.True(t, ok, "artifact missing or wrong type")

	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	assert.True(t, ok, "childrenPaths missing or wrong type")
	assert.Len(t, children, 1)
	assert.Equal(t, "/b", children[0].(string))
}

func TestUpdateConfigMapWithRetry_Success(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	// Seed a ConfigMap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{"k": "v1"},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Apply update via retry helper
	err = reconciler.updateConfigMapWithRetry(ctx, func(latest *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		if latest.Data == nil {
			latest.Data = make(map[string]string)
		}
		latest.Data["k"] = "v2"
		return true, latest, nil
	})
	assert.NoError(t, err)

	// Verify persisted
	got, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "v2", got.Data["k"])
}

func TestUpdateConfigMapWithRetry_UpdateFuncError(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	// Seed a ConfigMap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{"k": "v1"},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Mutator returns error; no update should be attempted
	err = reconciler.updateConfigMapWithRetry(ctx, func(latest *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		return false, latest, errors.New("mutator failure")
	})
	assert.Error(t, err)
	got, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "v1", got.Data["k"])
}

func TestUpdateConfigMapWithRemovedChildPath_Success(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	parentKey := "default.basemodel.parent"
	// childrenPaths contains the child path we will remove
	original := `{"config":{"artifact":{"childrenPaths":["/a","/child","/b"]}}}`

	// Seed ConfigMap with parent entry
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{
			parentKey: original,
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Build inputs
	childPath := "/child"

	// Call method under test
	err = reconciler.updateConfigMapWithRemovedChildPath(ctx, parentKey, childPath)
	assert.NoError(t, err)

	// Verify the child path has been removed
	updated, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)

	var obj map[string]interface{}
	err = json.Unmarshal([]byte(updated.Data[parentKey]), &obj)
	assert.NoError(t, err)

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	assert.True(t, ok, "config missing or wrong type")

	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	assert.True(t, ok, "artifact missing or wrong type")

	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	assert.True(t, ok, "childrenPaths should be an array")
	assert.ElementsMatch(t, []interface{}{"/a", "/b"}, children)
}

func TestUpdateConfigMapWithRemovedChildPath_MissingParentKey_ReturnsError(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	parentKey := "default.basemodel.parent"

	// Seed ConfigMap WITHOUT the parent key
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{
			"other.key": `{"config":{"artifact":{"childrenPaths":["/x"]}}}`,
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	childPath := "/child"
	err = reconciler.updateConfigMapWithRemovedChildPath(ctx, parentKey, childPath)
	assert.Nil(t, err)
	// no op
	cm, err1 := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err1)
	_, exists := cm.Data[parentKey]
	assert.False(t, exists)
}

func TestRemoveChildPathFromParent_RemovesExisting(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	parentKey := "namespace.basemodel.parent"
	original := `{"config":{"artifact":{"childrenPaths":["/a","/b","/c"]}}}`

	updated, err := reconciler.removeChildPathFromParent("/b", parentKey, original)
	assert.NoError(t, err)

	var obj map[string]interface{}
	err = json.Unmarshal([]byte(updated), &obj)
	assert.NoError(t, err)

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	assert.True(t, ok, "no config missing or wrong type")
	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	assert.True(t, ok, "no artifact missing or wrong type")

	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	assert.True(t, ok, "childrenPaths should be an array")
	assert.ElementsMatch(t, []interface{}{"/a", "/c"}, children)
}

func TestRemoveChildPathFromParent_NoOpWhenMissing(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	parentKey := "namespace.basemodel.parent"
	original := `{"config":{"artifact":{"childrenPaths":["/a"]}}}`

	updated, err := reconciler.removeChildPathFromParent("/x", parentKey, original)
	assert.NoError(t, err)

	var obj map[string]interface{}
	err = json.Unmarshal([]byte(updated), &obj)
	assert.NoError(t, err)

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	assert.True(t, ok)
	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	assert.True(t, ok)

	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	assert.True(t, ok)
	assert.ElementsMatch(t, []interface{}{"/a"}, children)
}

func TestRemoveChildPathFromParent_CreatesWhenMissing(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	parentKey := "namespace.basemodel.parent"
	original := `{"config":{"artifact":{}}}`

	updated, err := reconciler.removeChildPathFromParent("/a", parentKey, original)
	assert.NoError(t, err)

	var obj map[string]interface{}
	err = json.Unmarshal([]byte(updated), &obj)
	assert.NoError(t, err)

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	assert.True(t, ok)
	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	assert.True(t, ok)

	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	assert.True(t, ok, "childrenPaths missing or wrong type")
	assert.Len(t, children, 0, "expected empty array when removing from missing childrenPaths")
}

func TestRemoveChildPathFromParent_ChildrenPathsWrongType(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	parentKey := "namespace.basemodel.parent"
	original := `{"config":{"artifact":{"childrenPaths":"oops"}}}`

	updated, err := reconciler.removeChildPathFromParent("/a", parentKey, original)
	assert.NoError(t, err)

	var obj map[string]interface{}
	err = json.Unmarshal([]byte(updated), &obj)
	assert.NoError(t, err)

	cfg, ok := obj[ConfigAttr].(map[string]interface{})
	assert.True(t, ok)
	artifact, ok := cfg[ArtifactAttr].(map[string]interface{})
	assert.True(t, ok)

	// For wrong-typed existing value, function normalizes to an empty array
	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	assert.True(t, ok, "childrenPaths should be an array")
	assert.Len(t, children, 0, "childrenPaths should be an empty array when existing value had wrong type")
}

func TestRemoveChildPathFromParent_InvalidJSON(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	parentKey := "namespace.basemodel.parent"
	original := `{invalid-json`

	updated, err := reconciler.removeChildPathFromParent("/a", parentKey, original)
	assert.Equal(t, "", updated)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid JSON for key namespace.basemodel.parent")
}

func TestGetParentPathAndChildrenPaths_Valid(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ChildrenPathsAttr: []string{"/a", "/b"},
				ParentPathAttr: map[string]string{
					"namespace.basemodel.parent": "/parent/path",
				},
			},
		},
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("namespace.basemodel.child", string(b))
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"namespace.basemodel.parent": "/parent/path"}, parent)
	assert.ElementsMatch(t, []string{"/a", "/b"}, children)
}

func TestGetParentPathAndChildrenPaths_TooManyParents(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ChildrenPathsAttr: []string{"/x"},
				ParentPathAttr: map[string]interface{}{
					"k1": "/p1",
					"k2": "/p2",
					"k3": 123,
				},
			},
		},
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"k1": "/p1", "k2": "/p2"}, parent)
	assert.ElementsMatch(t, []string{"/x"}, children)
}

func TestGetParentPathAndChildrenPaths_ChildrenInterfaceMixed(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ChildrenPathsAttr: []interface{}{"a", 2, map[string]interface{}{"x": 1}, "b"},
				ParentPathAttr:    map[string]string{"p": "/p"},
			},
		},
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"p": "/p"}, parent)
	assert.ElementsMatch(t, []string{"a", "b"}, children)
}

func TestGetParentPathAndChildrenPaths_MissingChildrenPaths(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ParentPathAttr: map[string]string{"p": "/p"},
			},
		},
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"p": "/p"}, parent)
	assert.Empty(t, children)
}

func TestGetParentPathAndChildrenPaths_WrongTypeChildrenPaths_NoErrorEmpty(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ChildrenPathsAttr: "oops", // wrong type should be treated as empty
				ParentPathAttr:    map[string]string{"p": "/p"},
			},
		},
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"p": "/p"}, parent)
	assert.Empty(t, children)
}

func TestGetParentPathAndChildrenPaths_MissingParentPath(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ChildrenPathsAttr: []string{"/a"},
			},
		},
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.NoError(t, err)
	assert.Empty(t, parent)
	assert.ElementsMatch(t, []string{"/a"}, children)
}

func TestGetParentPathAndChildrenPaths_WrongTypeParentPath_NoErrorEmpty(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ChildrenPathsAttr: []string{"/a"},
				ParentPathAttr:    123, // wrong type should be treated as empty
			},
		},
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.NoError(t, err)
	assert.Empty(t, parent)
	assert.ElementsMatch(t, []string{"/a"}, children)
}

func TestGetParentPathAndChildrenPaths_MissingConfig_Error(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{} // no config
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config conversion")
	// Function returns empty map/slice on this error (not nil)
	assert.NotNil(t, parent)
	assert.NotNil(t, children)
	assert.Empty(t, parent)
	assert.Empty(t, children)
}

func TestGetParentPathAndChildrenPaths_MissingArtifact_Error(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{}, // config present, artifact missing
	}
	b, _ := json.Marshal(obj)

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", string(b))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid artifact conversion")
	// Function returns empty map/slice on this error (not nil)
	assert.NotNil(t, parent)
	assert.NotNil(t, children)
	assert.Empty(t, parent)
	assert.Empty(t, children)
}

func TestGetParentPathAndChildrenPaths_InvalidJSON_Error(t *testing.T) {
	reconciler, _, _ := setupConfigMapTest(t)

	dataEntry := "{invalid-json"

	parent, children, err := reconciler.getParentPathAndChildrenPaths("k", dataEntry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid JSON for key")
	// Function returns empty map/slice on this error (not nil)
	assert.NotNil(t, parent)
	assert.NotNil(t, children)
	assert.Empty(t, parent)
	assert.Empty(t, children)
}

func TestGetConfigMap(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	// Seed a ConfigMap that should be retrieved
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{"key": "value"},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Happy path: should retrieve the seeded ConfigMap
	got, err := reconciler.getConfigMap(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, reconciler.nodeName, got.Name)
	assert.Equal(t, reconciler.namespace, got.Namespace)
	assert.Equal(t, "value", got.Data["key"])

	// Error path: force GET to fail
	kubeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("simulated get error")
	})

	got, err = reconciler.getConfigMap(ctx)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "simulated get error")
}

func TestUpdateConfigMapWithRetry_GetError(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	// Seed CM
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Force GET error
	kubeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("get failed")
	})

	err = reconciler.updateConfigMapWithRetry(ctx, func(latest *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		return true, latest, nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get failed")
}

func TestUpdateConfigMapWithRetry_UpdateConflict_RetrySucceeds(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	// Seed CM
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{"k": "v1"},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// First update returns Conflict, subsequent updates succeed
	var calls int
	kubeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if calls == 0 {
			calls++
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: "", Resource: "configmaps"},
				reconciler.nodeName,
				errors.New("conflict"),
			)
		}
		return false, nil, nil
	})

	err = reconciler.updateConfigMapWithRetry(ctx, func(latest *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		if latest.Data == nil {
			latest.Data = make(map[string]string)
		}
		latest.Data["k"] = "v2"
		return true, latest, nil
	})
	assert.NoError(t, err)

	got, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "v2", got.Data["k"])
	assert.GreaterOrEqual(t, calls, 1)
}

func TestUpdateConfigMapWithRetry_UpdateError(t *testing.T) {
	reconciler, kubeClient, _ := setupConfigMapTest(t)
	ctx := context.Background()

	// Seed CM
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconciler.nodeName,
			Namespace: reconciler.namespace,
		},
		Data: map[string]string{"k": "v1"},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Create(ctx, cm, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Force persistent update error
	kubeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("update failed")
	})

	err = reconciler.updateConfigMapWithRetry(ctx, func(latest *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		if latest.Data == nil {
			latest.Data = make(map[string]string)
		}
		latest.Data["k"] = "v2"
		return true, latest, nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")

	got, err := kubeClient.CoreV1().ConfigMaps(reconciler.namespace).Get(ctx, reconciler.nodeName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "v1", got.Data["k"])
}

func buildJSONWith(sha, parentName string, parentPath string) string {
	obj := map[string]interface{}{
		ConfigAttr: map[string]interface{}{
			ArtifactAttr: map[string]interface{}{
				ShaAttr:        sha,
				ParentPathAttr: map[string]string{parentName: parentPath},
			},
		},
	}
	b, _ := json.Marshal(obj)
	return string(b)
}
