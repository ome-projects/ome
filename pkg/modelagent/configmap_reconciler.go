package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/ome/pkg/utils"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// constants related to attribute name in the configmap data
const (
	ConfigAttr        = "config"
	ArtifactAttr      = "artifact"
	ShaAttr           = "sha"
	ParentPathAttr    = "parentPath"
	ChildrenPathsAttr = "childrenPaths"
)

// CacheEntry represents an entry in the model cache for ConfigMap reconciliation.
type CacheEntry struct {
	ModelName     string         // Name of the model
	ModelUID      types.UID      // UID of the model resource that owns this entry
	ModelStatus   ModelStatus    // Current status of the model
	ModelMetadata *ModelMetadata // Model metadata if available
}

// ConfigMapReconciler handles all ConfigMap operations for storing model state and metadata.
// It provides self-healing capabilities through periodic reconciliation to recover from
// manual ConfigMap deletions or modifications without requiring agent restarts.
type ConfigMapReconciler struct {
	kubeClient kubernetes.Interface   // Kubernetes client for ConfigMap CRUD operations
	nodeName   string                 // The name of the node (used as ConfigMap name)
	namespace  string                 // The namespace to store the ConfigMap in
	logger     *zap.SugaredLogger     // Logger for recording operations
	modelCache map[string]*CacheEntry // In-memory cache of model information
	// invalidatedModelUIDs records CR instances whose deletion has begun. A
	// model name may later be recreated with a new UID; invalidating the old UID
	// does not invalidate the new UID.
	invalidatedModelUIDs map[string]map[types.UID]struct{}
	cacheMutex           sync.RWMutex  // Mutex to protect concurrent access to the cache
	hfArtifactOperations sync.Map      // Parent key -> *sync.Mutex; serializes handler side effects.
	reconcileInterval    time.Duration // Interval for periodic reconciliation
	isReconciling        bool          // Flag to prevent concurrent reconciliations
	stopCh               chan struct{} // Channel to signal reconciliation goroutine to stop
}

// ConfigMapStatusOp represents an operation to update model status in ConfigMap.
// It contains the necessary information to identify the model and its new status.
type ConfigMapStatusOp struct {
	ModelStatus      ModelStatus               // The updated status of the model
	BaseModel        *v1beta1.BaseModel        // Reference to a namespace-scoped BaseModel (nil if using ClusterBaseModel)
	ClusterBaseModel *v1beta1.ClusterBaseModel // Reference to a cluster-scoped BaseModel (nil if using BaseModel)
}

// ConfigMapMetadataOp represents an operation to update model metadata in ConfigMap.
// It contains the necessary information to identify the model and its metadata.
type ConfigMapMetadataOp struct {
	ModelMetadata    ModelMetadata             // The metadata to be stored for the model
	BaseModel        *v1beta1.BaseModel        // Reference to a namespace-scoped BaseModel (nil if using ClusterBaseModel)
	ClusterBaseModel *v1beta1.ClusterBaseModel // Reference to a cluster-scoped BaseModel (nil if using BaseModel)
}

// ConfigMapProgressOp represents an operation to update model download progress in ConfigMap.
// It contains the necessary information to identify the model and its progress.
type ConfigMapProgressOp struct {
	Progress         *DownloadProgress         // The download progress to be stored
	BaseModel        *v1beta1.BaseModel        // Reference to a namespace-scoped BaseModel (nil if using ClusterBaseModel)
	ClusterBaseModel *v1beta1.ClusterBaseModel // Reference to a cluster-scoped BaseModel (nil if using BaseModel)
}

// NewConfigMapReconciler creates a new ConfigMapReconciler with the given parameters.
// It initializes the in-memory model cache and sets up the reconciliation interval.
//
// Parameters:
//   - nodeName: Name of the node, used as the ConfigMap name
//   - namespace: Kubernetes namespace where the ConfigMap will be stored
//   - kubeClient: Interface to the Kubernetes API
//   - logger: Structured logger for operation recording
//
// Returns:
//   - A configured ConfigMapReconciler ready to use
func NewConfigMapReconciler(nodeName string, namespace string, kubeClient kubernetes.Interface, logger *zap.SugaredLogger) *ConfigMapReconciler {
	return &ConfigMapReconciler{
		kubeClient:           kubeClient,
		nodeName:             nodeName,
		namespace:            namespace,
		logger:               logger,
		modelCache:           make(map[string]*CacheEntry),
		invalidatedModelUIDs: make(map[string]map[types.UID]struct{}),
		reconcileInterval:    5 * time.Minute, // Perform reconciliation every 5 minutes by default
		stopCh:               make(chan struct{}),
	}
}

// StartReconciliation begins the periodic reconciliation of ConfigMaps.
// This launches a background goroutine that checks for ConfigMap consistency
// at regular intervals and repairs any detected issues without requiring agent restarts.
// The interval is configurable through the reconcileInterval field (default: 5 minutes).
//
// This method should be called once during component initialization,
// typically from the model agent's main startup sequence.
func (c *ConfigMapReconciler) StartReconciliation() {
	c.logger.Infof("Starting ConfigMap reconciliation with interval %v", c.reconcileInterval)
	go func() {
		ticker := time.NewTicker(c.reconcileInterval)
		defer ticker.Stop()

		// Perform initial reconciliation immediately
		c.reconcileConfigMaps()

		for {
			select {
			case <-ticker.C:
				c.reconcileConfigMaps()
			case <-c.stopCh:
				c.logger.Info("Stopping ConfigMap reconciliation")
				return
			}
		}
	}()
}

// StopReconciliation safely stops the periodic reconciliation process.
// This should be called during graceful shutdown of the component to ensure
// that background goroutines are properly terminated.
// This method is idempotent - calling it multiple times has no additional effect.
func (c *ConfigMapReconciler) StopReconciliation() {
	select {
	case <-c.stopCh:
		// Channel already closed, no action needed
		return
	default:
		close(c.stopCh)
		c.logger.Debug("ConfigMap reconciliation stopped")
	}
}

// reconcileConfigMaps performs the reconciliation between the in-memory cache and the actual ConfigMaps.
// It detects and repairs two types of issues:
//  1. Missing ConfigMap: If the ConfigMap is completely missing, it recreates it with all cached models.
//  2. Missing model entries: If the ConfigMap exists but some model entries are missing, it restores just those entries.
//
// This method is thread-safe and prevents concurrent reconciliations to avoid resource contention.
// It is called periodically by the reconciliation goroutine started in StartReconciliation.
func (c *ConfigMapReconciler) reconcileConfigMaps() {
	// Create a new context for this reconciliation
	ctx := context.Background()
	// Prevent concurrent reconciliations
	if c.isReconciling {
		c.logger.Debug("Reconciliation already in progress, skipping")
		return
	}

	c.isReconciling = true
	defer func() { c.isReconciling = false }()

	c.logger.Debug("Starting ConfigMap reconciliation")

	// Get the current ConfigMap
	cm, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			c.logger.Warn("ConfigMap not found during reconciliation, will recreate it")
			// ConfigMap doesn't exist, recreate it from scratch
			c.recreateConfigMap(ctx)
			return
		}
		c.logger.Errorf("Failed to get ConfigMap during reconciliation: %v", err)
		return
	}

	// Copy missing entries before doing API writes so deletion can update the cache concurrently.
	type missingModel struct {
		modelID    string
		cacheEntry CacheEntry
	}
	var missingModels []missingModel
	c.cacheMutex.RLock()
	for modelID, cacheEntry := range c.modelCache {
		if _, exists := cm.Data[modelID]; !exists {
			missingModels = append(missingModels, missingModel{
				modelID:    modelID,
				cacheEntry: *cacheEntry,
			})
		}
	}
	c.cacheMutex.RUnlock()

	for _, missing := range missingModels {
		c.logger.Warnf("Model %s missing from ConfigMap, will restore it", missing.modelID)
		c.restoreModelInConfigMap(missing.modelID, &missing.cacheEntry)
	}

	c.logger.Debug("ConfigMap reconciliation completed successfully")
}

// recreateConfigMap creates a new ConfigMap from the in-memory model cache.
// This is called when the ConfigMap is completely missing (e.g., manually deleted),
// and needs to be reconstructed from the cached model data.
//
// The method handles the following tasks:
// 1. Creates a new ConfigMap with the correct name and namespace
// 2. Populates it with all model entries from the cache
// 3. Correctly maps cached metadata to ModelConfig entries
// 4. Creates the ConfigMap in the Kubernetes API
//
// Thread safety is ensured through the read lock on the cache mutex.
func (c *ConfigMapReconciler) recreateConfigMap(ctx context.Context) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	// Skip if cache is empty
	if len(c.modelCache) == 0 {
		c.logger.Info("No models in cache to recreate ConfigMap")
		return
	}

	// Create a new ConfigMap with all models from the cache
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.nodeName,
			Namespace: c.namespace,
		},
		Data: make(map[string]string),
	}

	// Add all models from cache to the ConfigMap
	for modelID, cacheEntry := range c.modelCache {
		// Create model entry from cache data
		modelEntry := &ModelEntry{
			Name:   cacheEntry.ModelName,
			Status: cacheEntry.ModelStatus,
		}

		// Convert metadata to ModelConfig if available
		if cacheEntry.ModelMetadata != nil {
			config := &ModelConfig{}
			// Copy metadata fields to config
			config.ModelType = cacheEntry.ModelMetadata.ModelType
			config.ModelArchitecture = cacheEntry.ModelMetadata.ModelArchitecture
			config.ModelCapabilities = cacheEntry.ModelMetadata.ModelCapabilities
			config.ModelParameterSize = cacheEntry.ModelMetadata.ModelParameterSize
			config.MaxTokens = cacheEntry.ModelMetadata.MaxTokens
			config.Quantization = string(cacheEntry.ModelMetadata.Quantization)
			config.ApiCapabilities = cacheEntry.ModelMetadata.ApiCapabilities
			config.Artifact = cacheEntry.ModelMetadata.Artifact
			modelEntry.Config = config
		}

		// Serialize the model entry to JSON
		modelEntryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			c.logger.Errorf("Failed to marshal model entry for %s: %v", modelID, err)
			continue
		}

		cm.Data[modelID] = string(modelEntryJSON)
	}

	// Create the ConfigMap
	_, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err != nil {
		c.logger.Errorf("Failed to recreate ConfigMap: %v", err)
		return
	}

	c.logger.Info("Successfully recreated ConfigMap from cache")
}

// restoreModelInConfigMap adds or updates a specific model in the ConfigMap.
// This is called when an individual model entry is missing from an existing ConfigMap.
// Unlike recreateConfigMap, this method only updates a single model entry while preserving
// the rest of the ConfigMap content.
//
// The method:
// 1. Retrieves the current ConfigMap from the API
// 2. Constructs a ModelEntry from the cached model data
// 3. Serializes the entry to JSON and adds it to the ConfigMap
// 4. Updates the ConfigMap through the Kubernetes API
//
// If the ConfigMap is missing entirely, this will trigger a fallback to recreateConfigMap.
func (c *ConfigMapReconciler) restoreModelInConfigMap(modelID string, cacheEntry *CacheEntry) {
	// The entry may be a snapshot copied before deletion or node-local cleanup started.
	if c.isModelRestoreBlocked(modelID, cacheEntry.ModelUID) {
		c.logger.Debugf("Skipping stale restore for model %s", modelID)
		return
	}

	// Construct model entry from cache data
	modelEntry := &ModelEntry{
		Name:   cacheEntry.ModelName,
		Status: cacheEntry.ModelStatus,
	}

	// Convert metadata to ModelConfig if available
	if cacheEntry.ModelMetadata != nil {
		config := &ModelConfig{}
		// Copy metadata fields to config
		config.ModelType = cacheEntry.ModelMetadata.ModelType
		config.ModelArchitecture = cacheEntry.ModelMetadata.ModelArchitecture
		config.ModelCapabilities = cacheEntry.ModelMetadata.ModelCapabilities
		config.ModelParameterSize = cacheEntry.ModelMetadata.ModelParameterSize
		config.MaxTokens = cacheEntry.ModelMetadata.MaxTokens
		config.Quantization = string(cacheEntry.ModelMetadata.Quantization)
		config.ApiCapabilities = cacheEntry.ModelMetadata.ApiCapabilities
		config.Artifact = cacheEntry.ModelMetadata.Artifact

		modelEntry.Config = config
	}

	// Serialize the model entry to JSON
	modelEntryJSON, err := json.Marshal(modelEntry)
	if err != nil {
		c.logger.Errorf("Failed to marshal model entry for %s: %v", modelID, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get the current ConfigMap
	cm, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// The whole ConfigMap is missing, so restore every cached model.
			c.logger.Warn("ConfigMap not found during model restore, falling back to full recreation")
			c.recreateConfigMap(ctx)
			return
		}
		c.logger.Errorf("Failed to get ConfigMap during model restore: %v", err)
		return
	}

	// Initialize the Data map if necessary.
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	// Restore only a missing entry; do not overwrite a concurrent writer's newer value.
	if _, exists := cm.Data[modelID]; exists {
		return
	}
	cm.Data[modelID] = string(modelEntryJSON)

	// Update the ConfigMap with retry logic (3 attempts)
	for attempts := 0; attempts < 3; attempts++ {
		// Re-check live-cache ownership before every write so cleanup can invalidate a copied snapshot.
		if c.isModelRestoreBlocked(modelID, cacheEntry.ModelUID) {
			c.logger.Debugf("Skipping stale restore for model %s", modelID)
			return
		}

		_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
		if err == nil {
			// Successfully updated
			break
		}

		// Check if we need to retry due to a resourceVersion conflict
		if errors.IsConflict(err) && attempts < 2 {
			c.logger.Warnf("Conflict during model restore (attempt %d), retrying: %v", attempts+1, err)
			// Get the latest version of the ConfigMap
			cm, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
			if err != nil {
				c.logger.Errorf("Failed to get ConfigMap for conflict resolution: %v", err)
				return
			}
			if c.isModelRestoreBlocked(modelID, cacheEntry.ModelUID) {
				c.logger.Debugf("Skipping stale restore for model %s", modelID)
				return
			}
			if cm.Data == nil {
				cm.Data = make(map[string]string)
			}
			// Re-apply only this model's entry if it is still missing from the latest ConfigMap.
			if _, exists := cm.Data[modelID]; exists {
				return
			}
			cm.Data[modelID] = string(modelEntryJSON)
			continue
		}

		// Non-conflict error or final attempt
		c.logger.Errorf("Failed to update ConfigMap with restored model %s after %d attempts: %v", modelID, attempts+1, err)
		return
	}

	c.logger.Infof("Successfully restored model %s in ConfigMap", modelID)
}

// ReconcileModelStatus updates the ConfigMap with model status information and synchronizes the in-memory cache.
//
// This method performs two key operations:
// 1. Updates the model status in the Kubernetes ConfigMap, creating it if necessary
// 2. Synchronizes the in-memory model cache with the updated status information
//
// The cache updates are atomic, protected by mutex, ensuring thread safety even with concurrent reconciliation.
// Both operations must succeed for the method to return nil, otherwise an error is returned.
//
// Parameters:
//   - op: ConfigMapStatusOp containing model references and new status
//
// Returns:
//   - error: nil if both ConfigMap and cache updates succeed, error otherwise
func (c *ConfigMapReconciler) ReconcileModelStatus(ctx context.Context, statusOp *ConfigMapStatusOp) error {
	modelInfo := getConfigMapModelInfo(statusOp.BaseModel, statusOp.ClusterBaseModel)
	c.logger.Infof("Reconciling model status in ConfigMap for %s with status: %s", modelInfo, statusOp.ModelStatus)

	// Update the ConfigMap with status
	err := c.updateModelStatusInConfigMap(ctx, statusOp)
	if err != nil {
		c.logger.Errorf("Failed to update model status in ConfigMap for %s: %v", modelInfo, err)
		return err
	}

	// Update the in-memory cache
	modelID := getModelID(statusOp.BaseModel, statusOp.ClusterBaseModel)
	modelUID := getModelResourceUID(statusOp.BaseModel, statusOp.ClusterBaseModel)
	c.cacheMutex.Lock()
	// Deletion may start after the ConfigMap mutation; do not repopulate the cache with a stale generation.
	if !c.prepareActiveModelLocked(modelID, modelUID) {
		c.cacheMutex.Unlock()
		c.logger.Debugf("Skipping cache status update for model %s", modelInfo)
		return nil
	}
	if c.modelCache == nil {
		c.modelCache = make(map[string]*CacheEntry)
	}

	// Get existing cache entry or create a new one
	cacheEntry, exists := c.modelCache[modelID]
	if !exists {
		// Extract model name for the cache entry
		modelName := ""
		if statusOp.BaseModel != nil {
			modelName = statusOp.BaseModel.Name
		} else if statusOp.ClusterBaseModel != nil {
			modelName = statusOp.ClusterBaseModel.Name
		}

		cacheEntry = &CacheEntry{
			ModelName:   modelName,
			ModelUID:    modelUID,
			ModelStatus: statusOp.ModelStatus,
		}
		c.modelCache[modelID] = cacheEntry
	} else {
		// Just update the status in existing entry
		cacheEntry.ModelUID = modelUID
		cacheEntry.ModelStatus = statusOp.ModelStatus
	}
	c.cacheMutex.Unlock()

	c.logger.Infof("Successfully updated ConfigMap and cache for %s with status: %s", modelInfo, statusOp.ModelStatus)
	return nil
}

// getModelID generates a unique deterministic identifier string for a model.
// It handles both namespace-scoped BaseModel and cluster-scoped ClusterBaseModel objects.
//
// For BaseModel: The format is {namespace}.basemodel.{model_name}.
// For ClusterBaseModel: The format is clusterbasemodel.{model_name}.
//
// These IDs serve as consistent keys for the in-memory cache and ConfigMap entries,
// ensuring proper reconciliation between cache and ConfigMap state.
//
// Parameters:
//   - baseModel: A namespace-scoped BaseModel object (nil if using ClusterBaseModel)
//   - clusterBaseModel: A cluster-scoped ClusterBaseModel object (nil if using BaseModel)
//
// Returns:
//   - A unique string identifier for the model, or empty string if both inputs are nil
func getModelID(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) string {
	var namespace, modelName string
	var isClusterBaseModel bool

	if baseModel != nil {
		modelName = baseModel.Name
		namespace = baseModel.Namespace
		isClusterBaseModel = false
	} else if clusterBaseModel != nil {
		modelName = clusterBaseModel.Name
		namespace = ""
		isClusterBaseModel = true
	} else {
		return ""
	}

	return constants.GetModelConfigMapKey(namespace, modelName, isClusterBaseModel)
}

func getModelResourceUID(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) types.UID {
	if baseModel != nil {
		return baseModel.UID
	}
	if clusterBaseModel != nil {
		return clusterBaseModel.UID
	}
	return ""
}

// isModelResourceDeleting distinguishes terminal CR deletion from reversible cleanup.
// Reversible cleanup may be followed by another download for the same CR instance.
func isModelResourceDeleting(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) bool {
	if baseModel != nil {
		return baseModel.DeletionTimestamp != nil
	}
	return clusterBaseModel != nil && clusterBaseModel.DeletionTimestamp != nil
}

// ReconcileModelMetadata updates the ConfigMap with model metadata
func (c *ConfigMapReconciler) ReconcileModelMetadata(ctx context.Context, op *ConfigMapMetadataOp) error {
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)
	c.logger.Infof("Reconciling model metadata in ConfigMap for %s", modelInfo)

	// Update the ConfigMap with metadata
	err := c.updateModelMetadataInConfigMap(ctx, op)
	if err != nil {
		c.logger.Errorf("Failed to update model metadata in ConfigMap for %s: %v", modelInfo, err)
		return err
	}

	// Update the in-memory cache with metadata
	modelID := getModelID(op.BaseModel, op.ClusterBaseModel)
	modelUID := getModelResourceUID(op.BaseModel, op.ClusterBaseModel)
	c.cacheMutex.Lock()
	// Deletion may start after the ConfigMap mutation; do not repopulate the cache with a stale generation.
	if !c.prepareActiveModelLocked(modelID, modelUID) {
		c.cacheMutex.Unlock()
		c.logger.Debugf("Skipping cache metadata update for model %s", modelInfo)
		return nil
	}
	if c.modelCache == nil {
		c.modelCache = make(map[string]*CacheEntry)
	}

	cacheEntry, exists := c.modelCache[modelID]
	if !exists {
		modelName := ""
		if op.BaseModel != nil {
			modelName = op.BaseModel.Name
		} else if op.ClusterBaseModel != nil {
			modelName = op.ClusterBaseModel.Name
		}

		cacheEntry = &CacheEntry{
			ModelName:     modelName,
			ModelUID:      modelUID,
			ModelMetadata: &op.ModelMetadata,
		}
		c.modelCache[modelID] = cacheEntry
	} else {
		// Update the metadata
		cacheEntry.ModelUID = modelUID
		cacheEntry.ModelMetadata = &op.ModelMetadata
	}
	c.cacheMutex.Unlock()

	c.logger.Infof("Successfully updated ConfigMap and cache for %s with metadata", modelInfo)
	return nil
}

// ReconcileModelProgress updates the ConfigMap with model download progress.
// This is called periodically during model downloads to track progress.
// Uses retry logic to handle concurrent updates gracefully.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - op: ConfigMapProgressOp containing model references and progress data
//
// Returns:
//   - error: nil if update succeeds, error otherwise
func (c *ConfigMapReconciler) ReconcileModelProgress(ctx context.Context, op *ConfigMapProgressOp) error {
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)

	err := c.updateModelProgressInConfigMap(ctx, op)
	if err != nil {
		c.logger.Errorf("Failed to update model progress in ConfigMap for %s: %v", modelInfo, err)
		return err
	}

	return nil
}

// updateModelProgressInConfigMap updates the model progress in the ConfigMap.
func (c *ConfigMapReconciler) updateModelProgressInConfigMap(ctx context.Context, op *ConfigMapProgressOp) error {
	key := c.getModelConfigMapKey(op.BaseModel, op.ClusterBaseModel)
	modelUID := getModelResourceUID(op.BaseModel, op.ClusterBaseModel)
	var modelName string
	if op.BaseModel != nil {
		modelName = op.BaseModel.Name
	} else {
		modelName = op.ClusterBaseModel.Name
	}

	return c.mutateModelEntryWithRetry(ctx, key, modelUID, false, func(data map[string]string) (bool, error) {
		modelEntry := ModelEntry{
			Name:   modelName,
			Status: ModelStatusUpdating,
		}
		if existingData, exists := data[key]; exists {
			if err := json.Unmarshal([]byte(existingData), &modelEntry); err != nil {
				modelEntry = ModelEntry{
					Name:   modelName,
					Status: ModelStatusUpdating,
				}
			}
		}
		modelEntry.Progress = op.Progress

		entryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			return false, err
		}
		newValue := string(entryJSON)
		if data[key] == newValue {
			return false, nil
		}
		data[key] = newValue
		return true, nil
	})
}

// DeleteModelFromConfigMap removes a model entry from the ConfigMap
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - baseModel: The BaseModel reference
//   - clusterBaseModel: The ClusterBaseModel reference
//
// Returns:
//   - error: nil if deletion succeeds or model doesn't exist, error otherwise
func (c *ConfigMapReconciler) DeleteModelFromConfigMap(ctx context.Context, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) error {
	modelInfo := getConfigMapModelInfo(baseModel, clusterBaseModel)
	c.logger.Infof("Deleting model from ConfigMap: %s", modelInfo)

	modelID := c.getModelConfigMapKey(baseModel, clusterBaseModel)
	modelUID := getModelResourceUID(baseModel, clusterBaseModel)
	if isModelResourceDeleting(baseModel, clusterBaseModel) {
		// A deleting CR instance must never be restored or updated again.
		c.invalidateModelUIDAndEvictCache(modelID, modelUID)
	} else {
		// A later selector or affinity update may require new work using the same UID.
		c.evictModelCache(modelID)
	}

	// Delete must bypass its own UID invalidation; retries still operate on the latest ConfigMap.
	err := c.mutateModelEntryWithRetry(ctx, modelID, modelUID, true, func(data map[string]string) (bool, error) {
		if _, exists := data[modelID]; !exists {
			return false, nil
		}
		delete(data, modelID)
		return true, nil
	})
	if err != nil {
		c.logger.Errorf("Failed to update ConfigMap after model deletion: %v", err)
		return err
	}

	c.logger.Infof("Successfully deleted model %s from ConfigMap and cache", modelInfo)
	return nil
}

// getOrCreateConfigMap gets an existing ConfigMap or creates a new one if it doesn't exist
func (c *ConfigMapReconciler) getOrCreateConfigMap(ctx context.Context) (*corev1.ConfigMap, bool, error) {
	var notFound = false
	existingConfigMap, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			notFound = true
		} else {
			return nil, false, err
		}
	}

	if notFound {
		data := make(map[string]string)
		labels := make(map[string]string)
		labels[constants.ModelStatusConfigMapLabel] = "true"

		// Add node name as label for easier querying
		labels["node"] = c.nodeName

		annotations := make(map[string]string)
		// Add annotation to track which node this ConfigMap belongs to
		annotations["models.ome.io/node-name"] = c.nodeName
		annotations["models.ome.io/managed-by"] = "model-agent"

		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        c.nodeName,
				Namespace:   c.namespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Data: data,
		}, true, nil
	}

	return existingConfigMap, false, nil
}

/*
getConfigMap retrieves the node-scoped ConfigMap from Kubernetes.

It fetches the ConfigMap named after the agent's node (c.nodeName) in the configured
namespace (c.namespace). On failure, the error is logged for observability and
returned to the caller.

Parameters:
  - ctx: Context for cancellation and deadlines.

Returns:
  - *corev1.ConfigMap: The retrieved ConfigMap if found.
  - error: Non-nil if the retrieval failed.
*/
func (c *ConfigMapReconciler) getConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	existingConfigMap, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			c.logger.Infof("Node %s configmap does not exist yet", c.nodeName)
			return nil, err
		}
		c.logger.Errorf("Failed retrieve node %s configmap: %v", c.nodeName, err)
		return nil, err
	}
	return existingConfigMap, err
}

// getModelConfigMapKey gets the deterministic key for a model in the ConfigMap
func (c *ConfigMapReconciler) getModelConfigMapKey(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) string {
	var modelName, namespace string
	var isClusterBaseModel bool

	if baseModel != nil {
		modelName = baseModel.Name
		namespace = baseModel.Namespace
		isClusterBaseModel = false
	} else {
		modelName = clusterBaseModel.Name
		namespace = ""
		isClusterBaseModel = true
	}

	return constants.GetModelConfigMapKey(namespace, modelName, isClusterBaseModel)
}

type configMapMutation func(configMap *corev1.ConfigMap) (bool, error)

type modelEntryMutation func(data map[string]string) (bool, error)

// mutateConfigMapWithRetry applies mutate to the latest ConfigMap and retries optimistic-concurrency conflicts.
// The mutation's bool reports whether an API write is needed; false with a nil error is a successful no-op.
func (c *ConfigMapReconciler) mutateConfigMapWithRetry(ctx context.Context, mutate configMapMutation) error {
	return retry.OnError(retry.DefaultRetry, func(err error) bool {
		return errors.IsConflict(err) || errors.IsAlreadyExists(err)
	}, func() error {
		configMap, needCreate, err := c.getOrCreateConfigMap(ctx)
		if err != nil {
			return err
		}

		changed, err := mutate(configMap)
		if err != nil || !changed {
			return err
		}

		if needCreate {
			_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Create(ctx, configMap, metav1.CreateOptions{})
			return err
		}

		_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, configMap, metav1.UpdateOptions{})
		return err
	})
}

// mutateModelEntryWithRetry applies a per-model mutation unless deletion has
// invalidated its UID or the model key now belongs to another CR instance.
// allowDeleting lets the delete operation bypass the invalidation it installs
// for its own UID.
func (c *ConfigMapReconciler) mutateModelEntryWithRetry(
	ctx context.Context,
	modelID string,
	modelUID types.UID,
	allowDeleting bool,
	mutate modelEntryMutation,
) error {
	return c.mutateConfigMapWithRetry(ctx, func(configMap *corev1.ConfigMap) (bool, error) {
		if !allowDeleting && c.isModelMutationBlocked(modelID, modelUID) {
			c.logger.Debugf("Skipping stale ConfigMap mutation for model %s", modelID)
			return false, nil
		}
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		return mutate(configMap.Data)
	})
}

// evictModelCache removes the source used by periodic self-healing without
// permanently invalidating the UID.
// This permits new work for the same CR instance after a selector or affinity change.
func (c *ConfigMapReconciler) evictModelCache(modelID string) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	delete(c.modelCache, modelID)
}

// invalidateModelUIDAndEvictCache atomically prevents further mutations from
// this CR instance and removes its cached entry. Invalidation applies to
// modelUID, not the reusable model name, so a recreated CR with a new UID
// remains valid.
// Evicting the cache prevents periodic reconciliation from restoring the old
// ConfigMap entry during deletion.
func (c *ConfigMapReconciler) invalidateModelUIDAndEvictCache(modelID string, modelUID types.UID) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	delete(c.modelCache, modelID)
	if modelUID == "" {
		c.logger.Warnf("invalidateModelUIDAndEvictCache called with empty UID for %s; skipping UID invalidation", modelID)
		return
	}
	if c.invalidatedModelUIDs == nil {
		c.invalidatedModelUIDs = make(map[string]map[types.UID]struct{})
	}
	if c.invalidatedModelUIDs[modelID] == nil {
		c.invalidatedModelUIDs[modelID] = make(map[types.UID]struct{})
	}
	c.invalidatedModelUIDs[modelID][modelUID] = struct{}{}
}

func (c *ConfigMapReconciler) isModelMutationBlocked(modelID string, modelUID types.UID) bool {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	return c.isModelMutationBlockedLocked(modelID, modelUID)
}

// isModelRestoreBlocked rejects a copied reconciliation snapshot after its live cache entry was evicted.
// Unlike status and metadata updates, self-healing restore is valid only while the model remains in the cache.
func (c *ConfigMapReconciler) isModelRestoreBlocked(modelID string, modelUID types.UID) bool {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	if c.isModelMutationBlockedLocked(modelID, modelUID) {
		return true
	}
	_, exists := c.modelCache[modelID]
	return !exists
}

// isModelMutationBlockedLocked rejects a task when deletion invalidated its UID
// or the cached model key belongs to another CR instance. The caller must hold
// cacheMutex.
func (c *ConfigMapReconciler) isModelMutationBlockedLocked(modelID string, modelUID types.UID) bool {
	if c.isModelUIDInvalidatedLocked(modelID, modelUID) {
		return true
	}
	cacheEntry, exists := c.modelCache[modelID]
	// This relies on deletion finalizers preventing same-name recreation before ConfigMap cleanup;
	// force deletion may block the new UID until another model event or agent restart.
	return exists && cacheEntry.ModelUID != "" && modelUID != "" && cacheEntry.ModelUID != modelUID
}

// isModelUIDInvalidatedLocked reports whether deletion has invalidated this UID.
// A task without a UID is also blocked when this model key has invalidated UIDs,
// because it cannot identify which CR instance it belongs to.
// The caller must hold cacheMutex.
func (c *ConfigMapReconciler) isModelUIDInvalidatedLocked(modelID string, modelUID types.UID) bool {
	invalidatedUIDs, exists := c.invalidatedModelUIDs[modelID]
	if !exists {
		return false
	}
	if modelUID == "" {
		return true
	}
	_, exists = invalidatedUIDs[modelUID]
	return exists
}

func (c *ConfigMapReconciler) prepareActiveModelLocked(modelID string, modelUID types.UID) bool {
	// The caller holds cacheMutex so deletion cannot begin between this check and the cache write.
	return !c.isModelMutationBlockedLocked(modelID, modelUID)
}

// updateModelStatusInConfigMap updates the model status in the ConfigMap
func (c *ConfigMapReconciler) updateModelStatusInConfigMap(ctx context.Context, op *ConfigMapStatusOp) error {
	key := c.getModelConfigMapKey(op.BaseModel, op.ClusterBaseModel)
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)
	modelUID := getModelResourceUID(op.BaseModel, op.ClusterBaseModel)
	c.logger.Debugf("Using key '%s' for model %s", key, modelInfo)

	var modelName string
	if op.BaseModel != nil {
		modelName = op.BaseModel.Name
	} else {
		modelName = op.ClusterBaseModel.Name
	}

	return c.mutateModelEntryWithRetry(ctx, key, modelUID, false, func(data map[string]string) (bool, error) {
		if op.ModelStatus == ModelStatusDeleted {
			if _, exists := data[key]; !exists {
				return false, nil
			}
			delete(data, key)
			return true, nil
		}

		modelEntry := ModelEntry{Name: modelName}
		if existingData, exists := data[key]; exists {
			if err := json.Unmarshal([]byte(existingData), &modelEntry); err != nil {
				modelEntry = ModelEntry{Name: modelName}
			}
		}
		modelEntry.Status = op.ModelStatus
		if op.ModelStatus == ModelStatusReady || op.ModelStatus == ModelStatusFailed {
			modelEntry.Progress = nil
		}

		entryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			return false, err
		}
		newValue := string(entryJSON)
		if data[key] == newValue {
			return false, nil
		}
		data[key] = newValue
		return true, nil
	})
}

// updateModelMetadataInConfigMap updates the model metadata in the ConfigMap
func (c *ConfigMapReconciler) updateModelMetadataInConfigMap(ctx context.Context, op *ConfigMapMetadataOp) error {
	key := c.getModelConfigMapKey(op.BaseModel, op.ClusterBaseModel)
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)
	modelUID := getModelResourceUID(op.BaseModel, op.ClusterBaseModel)
	c.logger.Debugf("Using key '%s' for model %s", key, modelInfo)

	var modelName string
	if op.BaseModel != nil {
		modelName = op.BaseModel.Name
	} else {
		modelName = op.ClusterBaseModel.Name
	}

	modelConfig := ConvertMetadataToModelConfig(op.ModelMetadata)
	return c.mutateModelEntryWithRetry(ctx, key, modelUID, false, func(data map[string]string) (bool, error) {
		modelEntry := ModelEntry{
			Name:   modelName,
			Status: ModelStatusReady,
		}
		if existingData, exists := data[key]; exists {
			if err := json.Unmarshal([]byte(existingData), &modelEntry); err != nil {
				modelEntry = ModelEntry{
					Name:   modelName,
					Status: ModelStatusReady,
				}
			}
		}
		modelEntry.Config = modelConfig

		entryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			return false, err
		}
		newValue := string(entryJSON)
		if data[key] == newValue {
			return false, nil
		}
		data[key] = newValue
		return true, nil
	})
}

// updateConfigMapWithRetry A fundamental method to update configmap via a read-modify-write update with retry.
// Parameters:
//   - ctx: Context for cancellation / timeouts
//   - updateConfigmap: a pure function that takes the latest ConfigMap object and
//     returns the mutated ConfigMap (or an error). If it returns an error, no update
//     is attempted and the error is immediately returned.
//
// Returns:
//   - error: Any error of updateConfigmap function, operation error of Kube, or final retry exhaustion.
func (c *ConfigMapReconciler) updateConfigMapWithRetry(ctx context.Context, updateConfigmap func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error)) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Re-fetch the latest ConfigMap to get current ResourceVersion
		latestCM, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
		if err != nil {
			c.logger.Errorf("failed to get ConfigMap from Kube API server: %s", err)
			return err
		}

		needUpdate, updatedConfigmap, err := updateConfigmap(latestCM)
		if err != nil {
			c.logger.Errorf("failed to compute updated ConfigMap: %s", err)
			return err
		}
		if !needUpdate {
			c.logger.Infof("no need to update ConfigMap to Kube API server")
			return nil
		}

		// Update with the merged data
		_, updateErr := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, updatedConfigmap, metav1.UpdateOptions{})
		if updateErr != nil {
			if errors.IsConflict(updateErr) {
				// Avoid noisy error logs for expected conflicts that will be retried
				c.logger.Debugf("ConfigMap update conflict for %s/%s, will retry: %v", c.namespace, c.nodeName, updateErr)
			} else {
				c.logger.Errorf("failed to update ConfigMap to Kube API server: %s", updateErr)
			}
			return updateErr
		}
		return nil
	})
	if err != nil {
		if errors.IsConflict(err) {
			c.logger.Warnf("exhausted retries updating ConfigMap %s/%s due to conflicts", c.namespace, c.nodeName)
		}
	}
	return err
}

// Helper function to get a string representation of the model for logging
func getConfigMapModelInfo(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) string {
	if baseModel != nil {
		return fmt.Sprintf("BaseModel %s/%s", baseModel.Namespace, baseModel.Name)
	} else if clusterBaseModel != nil {
		return fmt.Sprintf("ClusterBaseModel %s", clusterBaseModel.Name)
	}
	return "unknown model"
}

/*
FindMatchedModelFromConfigMap scans the provided ConfigMap Data for the first entry
whose key has the provided namespaced modelType and whose JSON value contains config.artifact.sha equal to targetSha and has the most children paths(Exclude self).

Returns:
  - modelKey:   The parent of the matched ConfigMap Data key (modelType + model identifier).
  - parentPath: The value of config.artifact.parentPath for the matched entry.
  - err:        The last JSON parsing error encountered during scanning; nil if none.
*/
func (c *ConfigMapReconciler) FindMatchedModelFromConfigMap(configMap *corev1.ConfigMap, targetSha string, modelType string, currentModelTypeAndNodeName string) (string, string, error) {
	var searchingError error // the last

	var matchedParentName, matchedParentPath string
	var matchedParentChildrenNum = -1
	for modelKey, jsonStr := range configMap.Data {
		if !strings.HasPrefix(strings.ToLower(modelKey), strings.ToLower(modelType)) {
			continue
		}
		// parsed JSON for this entry
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
			// Ignore entries containing invalid JSON
			searchingError = fmt.Errorf("fail to Unmarshal %s during FindMatchedModelFromConfigMap: %s", jsonStr, err)
			c.logger.Errorf(searchingError.Error())
			continue
		}
		// Navigate: config → artifact → sha
		config, ok := obj[ConfigAttr].(map[string]interface{})
		if !ok {
			continue
		}
		artifact, ok := config[ArtifactAttr].(map[string]interface{})
		if !ok {
			continue
		}
		sha, ok := artifact[ShaAttr].(string)
		if !ok {
			continue
		}

		if sha != targetSha {
			continue
		}

		rawParent, ok := artifact[ParentPathAttr].(map[string]interface{})
		if !ok {
			searchingError = fmt.Errorf("parentPath is not an object")
			continue
		}

		if len(rawParent) != 1 {
			searchingError = fmt.Errorf("expected exactly one parentPath entry, got %d", len(rawParent))
			continue
		}

		for k, v := range rawParent {
			path, ok := v.(string)
			if !ok {
				searchingError = fmt.Errorf("parentPath value for %q is not a string", k)
				continue
			}
			if strings.EqualFold(k, currentModelTypeAndNodeName) {
				continue
			}

			childrenNum := len(extractChildrenPaths(artifact))
			if childrenNum > matchedParentChildrenNum {
				matchedParentChildrenNum = childrenNum
				matchedParentName = k
				matchedParentPath = path
			}
		}
	}
	c.logger.Infof("matchedParentName: %s, matchedParentPath: %s", matchedParentName, matchedParentPath)
	return matchedParentName, matchedParentPath, searchingError
}

func extractChildrenPaths(artifact map[string]interface{}) []string {
	// Read childrenPaths leniently and convert to []string
	children := make([]string, 0)
	if rawChildren, exists := artifact[ChildrenPathsAttr]; exists {
		switch vv := rawChildren.(type) {
		case []interface{}:
			for _, v := range vv {
				if s, ok := v.(string); ok {
					children = append(children, s)
				}
			}
		case []string:
			children = append(children, vv...)
		}
	}
	return children
}

// getModelDataByArtifactSha fetches the node-scoped ConfigMap (namespace "ome", name c.nodeName) and searches it for a model entry whose artifact SHA equals
// targetSha and whose key is prefixed by modelType (case-insensitive).
// Returns:
// - modelKey:   The matched ConfigMap Data key (modelType + model identifier).
// - parentPath: The value of config.artifact.parentPath for the matched entry.
// - err:        The last JSON parsing error encountered during scanning; nil if none.
func (c *ConfigMapReconciler) getModelDataByArtifactSha(ctx context.Context, targetSha string, modelType string, currentModelTypeAndNodeName string) (string, string, error) {
	cm, err := c.kubeClient.CoreV1().ConfigMaps("ome").Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			c.logger.Warn("cannot find configmap %s", c.nodeName)
			// ConfigMap doesn't exist, recreate it from scratch
			return "", "", fmt.Errorf("cannot find configmap %s", c.nodeName)
		}
		c.logger.Errorf("Failed to get ConfigMap %s: %v", c.nodeName, err)
		return "", "", fmt.Errorf("failed to get ConfigMap %s: %v", c.nodeName, err)
	}
	return c.FindMatchedModelFromConfigMap(cm, targetSha, modelType, currentModelTypeAndNodeName)
}

// addPathToChildrenPaths appends newPath to the config.artifact.childrenPaths array if the newPath is not contained in the children paths
func (c *ConfigMapReconciler) addPathToChildrenPaths(modelTypeAndModelName string, newPath string, dataEntry string) (string, error) {
	// Parse the JSON into a generic map
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(dataEntry), &obj); err != nil {
		c.logger.Errorf("invalid JSON for key %s: %w", modelTypeAndModelName, err)
		return "", fmt.Errorf("invalid JSON for key %s: %w", modelTypeAndModelName, err)
	}
	c.logger.Infof("current data: modelTypeAndModelName: %s, dataEntry: %s", modelTypeAndModelName, dataEntry)
	// Navigate or create nested structure: config → artifact
	config, ok := obj[ConfigAttr].(map[string]interface{})
	if !ok {
		config = map[string]interface{}{}
		obj[ConfigAttr] = config
	}
	artifact, ok := config[ArtifactAttr].(map[string]interface{})
	if !ok {
		artifact = map[string]interface{}{}
		config[ArtifactAttr] = artifact
	}

	// Ensure childrenPaths exists
	children, ok := artifact[ChildrenPathsAttr].([]interface{})
	if !ok {
		children = make([]interface{}, 0)
		artifact[ChildrenPathsAttr] = children
	}
	if !utils.ContainsString(children, newPath, false) {
		children = append(children, newPath)
	}
	artifact[ChildrenPathsAttr] = children

	updated, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	c.logger.Infof("will update: modelTypeAndModelName: %s, dataEntry: %s", modelTypeAndModelName, string(updated))
	return string(updated), nil
}

/*
getParentPathAndChildrenPaths extracts parentPath and childrenPaths from a model entry's JSON.

Parameters:
  - modelTypeAndModelName: ConfigMap data key (used for error messages).
  - dataEntry: JSON string to parse.

Returns:
  - parentPath: Normalized map extracted from config.artifact.parentPath (possibly empty).
  - children: Slice extracted from config.artifact.childrenPaths (possibly empty).
  - error: Parsing or structure conversion errors as described above.
*/
func (c *ConfigMapReconciler) getParentPathAndChildrenPaths(modelTypeAndModelName string, dataEntry string) (map[string]string, []string, error) {
	// Parse the JSON into a generic map
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(dataEntry), &obj); err != nil {
		c.logger.Errorf("invalid JSON for key %s: %w", modelTypeAndModelName, err)
		return make(map[string]string), []string{}, fmt.Errorf("invalid JSON for key %s: %w", modelTypeAndModelName, err)
	}
	c.logger.Infof("current data: modelTypeAndModelName: %s, dataEntry: %s", modelTypeAndModelName, dataEntry)
	// Navigate nested structure: config → artifact
	config, okConfig := obj[ConfigAttr].(map[string]interface{})
	if !okConfig {
		// No config => no children paths
		return make(map[string]string), []string{}, fmt.Errorf("invalid config conversion")
	}
	artifact, okArtifact := config[ArtifactAttr].(map[string]interface{})
	if !okArtifact {
		// No artifact => no children paths
		return make(map[string]string), []string{}, fmt.Errorf("invalid artifact conversion")
	}

	children := extractChildrenPaths(artifact)

	// Read childrenPaths leniently and convert to []string
	parentPath := make(map[string]string)
	if rawParent, exists := artifact[ParentPathAttr]; exists {
		switch mp := rawParent.(type) {
		case map[string]interface{}:
			for k, v := range mp {
				if s, ok := v.(string); ok {
					parentPath[k] = s
				}
			}
		case map[string]string:
			for k, v := range mp {
				parentPath[k] = v
			}
		}
	}
	c.logger.Infof("get parent paths is %v and children paths are %v", parentPath, children)
	return parentPath, children, nil
}

/*
removeChildPathFromParent removes the specified childPath from the JSON entry's
config.artifact.childrenPaths. The method is lenient: it creates missing
config/artifact structures, normalizes childrenPaths if it's missing or in a
non-array form, and writes the updated list back.

Parameters:
  - childPath:  The path to remove from childrenPaths.
  - parentName: The ConfigMap data key (used for logging/error messages).
  - dataEntry:  The JSON string of the model entry to modify.

Returns:
  - string: The updated JSON entry with childrenPaths modified.
  - error:  If the input JSON is invalid or JSON marshalling fails.
*/
func (c *ConfigMapReconciler) removeChildPathFromParent(childPath string, parentName string, dataEntry string) (string, error) {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(dataEntry), &obj); err != nil {
		c.logger.Errorf("invalid JSON for key %s: %w", parentName, err)
		return "", fmt.Errorf("invalid JSON for key %s: %w", parentName, err)
	}
	c.logger.Infof("current data: modelTypeAndModelName: %s, dataEntry: %s", parentName, dataEntry)

	// Navigate or create nested structure: config → artifact
	config, okConfig := obj[ConfigAttr].(map[string]interface{})
	if !okConfig {
		config = map[string]interface{}{}
		obj[ConfigAttr] = config
	}
	artifact, okArtifact := config[ArtifactAttr].(map[string]interface{})
	if !okArtifact {
		artifact = map[string]interface{}{}
		config[ArtifactAttr] = artifact
	}

	// Normalize childrenPaths to []string
	var children []string
	if raw, exists := artifact[ChildrenPathsAttr]; exists {
		switch vv := raw.(type) {
		case []interface{}:
			for _, v := range vv {
				if s, okStr := v.(string); okStr {
					children = append(children, s)
				}
			}
		case []string:
			children = append(children, vv...)
		}
	} else {
		children = make([]string, 0)
	}

	// Remove target path and write back (ensure empty array, not null)
	// Remove target path and write back as JSON array (not null)
	result := utils.RemoveString(children, childPath)
	if len(result) == 0 {
		artifact[ChildrenPathsAttr] = []interface{}{}
	} else {
		arr := make([]interface{}, 0, len(result))
		for _, s := range result {
			arr = append(arr, s)
		}
		artifact[ChildrenPathsAttr] = arr
	}

	updated, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	c.logger.Infof("will update: modelTypeAndModelName: %s, dataEntry: %s", parentName, string(updated))
	return string(updated), nil
}

// updateConfigMapWithUpdatedChildrenPaths appends newPath into the JSON array
// config.artifact.childrenPaths for the given modelTypeAndModelName entry and
// persists the change to the Kubernetes API server with retry.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - modelTypeAndModelName: Key inside ConfigMap.Data to update
//   - newPath: The path to append into config.artifact.childrenPaths
//
// Returns:
//   - error: Any JSON parsing/validation error or API server update error
func (c *ConfigMapReconciler) updateConfigMapWithUpdatedChildrenPaths(ctx context.Context, modelTypeAndModelName string, newPath string) error {
	// Recompute the merged JSON inside the retry closure using the freshest data to avoid
	// stomping concurrent updates and to reduce conflict retries.
	updateConfigMap := func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		existingDataEntry, exists := currentConfigMap.Data[modelTypeAndModelName]
		if !exists {
			c.logger.Infof("key %s not found in ConfigMap", modelTypeAndModelName)
			return false, currentConfigMap, nil
		}
		mergedEntry, err := c.addPathToChildrenPaths(modelTypeAndModelName, newPath, existingDataEntry)
		if err != nil {
			return false, currentConfigMap, err
		}
		currentConfigMap.Data[modelTypeAndModelName] = mergedEntry
		return true, currentConfigMap, nil
	}
	err := c.updateConfigMapWithRetry(ctx, updateConfigMap)
	if err != nil {
		c.logger.Errorf("failed to add child paths %s to modelTypeAndModelName %s", newPath, modelTypeAndModelName)
	}
	return err
}

/*
updateConfigMapWithRemovedChildPath removes the provided childPath from the parent
model's config.artifact.childrenPaths within the node-scoped ConfigMap.

Parameters:
  - ctx:        Context for cancellation/timeouts.
  - parentPath: A map expected to contain exactly one entry whose key is the parent
    model's ConfigMap key (e.g. "namespace.basemodel.parent" or
    "clusterbasemodel.parent"). The map value is ignored.
  - childPath:  The absolute path of the child model to remove from the parent's childrenPaths.
*/
func (c *ConfigMapReconciler) updateConfigMapWithRemovedChildPath(ctx context.Context, parentName string, childPath string) error {
	updateConfigMap := func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		existingDataEntry, exists := currentConfigMap.Data[parentName]
		if !exists {
			c.logger.Infof("key %s not found in ConfigMap", parentName)
			return false, currentConfigMap, nil
		}
		removedEntry, err := c.removeChildPathFromParent(childPath, parentName, existingDataEntry)
		if err != nil {
			return false, currentConfigMap, err
		}
		currentConfigMap.Data[parentName] = removedEntry
		return true, currentConfigMap, nil
	}
	err := c.updateConfigMapWithRetry(ctx, updateConfigMap)
	if err != nil {
		c.logger.Errorf("failed to remove child path %s from data entry %s", childPath, parentName)
	}
	return err
}

func (c *ConfigMapReconciler) getDataEntryBasedOnModelKey(ctx context.Context, modelKey string) (bool, string, error) {
	// get cm
	existingConfigMap, err := c.getConfigMap(ctx)
	if err != nil {
		c.logger.Errorf("cannot retrieve node configmap: %v", err)
		return false, "", fmt.Errorf("cannot retrieve node configmap: %v", err)
	}
	dataEntry, exists := existingConfigMap.Data[modelKey]
	c.logger.Infof("modelKey %s exists: %v", modelKey, exists)
	return exists, dataEntry, nil
}

func parseParent(parentMap map[string]string) (string, string) {
	var parentName, parentDir string
	if len(parentMap) != 0 {
		for key, value := range parentMap {
			parentName = key
			parentDir = value
			break
		}
	}
	return parentName, parentDir
}
