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
	ModelStatus   ModelStatus    // Current status of the model
	ModelMetadata *ModelMetadata // Model metadata if available
}

// ConfigMapReconciler handles all ConfigMap operations for storing model state and metadata.
// It provides self-healing capabilities through periodic reconciliation to recover from
// manual ConfigMap deletions or modifications without requiring agent restarts.
type ConfigMapReconciler struct {
	kubeClient        kubernetes.Interface   // Kubernetes client for ConfigMap CRUD operations
	nodeName          string                 // The name of the node (used as ConfigMap name)
	namespace         string                 // The namespace to store the ConfigMap in
	logger            *zap.SugaredLogger     // Logger for recording operations
	modelCache        map[string]*CacheEntry // In-memory cache of model information
	cacheMutex        sync.RWMutex           // Mutex to protect concurrent access to the cache
	reconcileInterval time.Duration          // Interval for periodic reconciliation
	isReconciling     bool                   // Flag to prevent concurrent reconciliations
	stopCh            chan struct{}          // Channel to signal reconciliation goroutine to stop
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
		kubeClient:        kubeClient,
		nodeName:          nodeName,
		namespace:         namespace,
		logger:            logger,
		modelCache:        make(map[string]*CacheEntry),
		reconcileInterval: 5 * time.Minute, // Perform reconciliation every 5 minutes by default
		stopCh:            make(chan struct{}),
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

	// Check if all models in the cache are present in the ConfigMap
	c.cacheMutex.RLock()
	for modelID, cacheEntry := range c.modelCache {
		// Check if model exists in ConfigMap
		if _, exists := cm.Data[modelID]; !exists {
			c.logger.Warnf("Model %s missing from ConfigMap, will restore it", modelID)
			c.restoreModelInConfigMap(modelID, cacheEntry)
		}
	}
	c.cacheMutex.RUnlock()

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
			// ConfigMap doesn't exist anymore, fallback to full recreation
			c.logger.Warn("ConfigMap not found during model restore, falling back to full recreation")
			c.recreateConfigMap(ctx)
			return
		}
		c.logger.Errorf("Failed to get ConfigMap during model restore: %v", err)
		return
	}

	// Add or update the model entry - initialize Data map if nil
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	// Add the restored model entry to the ConfigMap
	cm.Data[modelID] = string(modelEntryJSON)

	// Update the ConfigMap with retry logic (3 attempts)
	for attempts := 0; attempts < 3; attempts++ {
		_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
		if err == nil {
			// Successfully updated
			break
		}

		// Check if we need to retry due to conflict
		if errors.IsConflict(err) && attempts < 2 {
			c.logger.Warnf("Conflict during model restore (attempt %d), retrying: %v", attempts+1, err)
			// Get the latest version of the ConfigMap
			cm, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
			if err != nil {
				c.logger.Errorf("Failed to get ConfigMap for conflict resolution: %v", err)
				return
			}
			// Re-apply our changes to the latest version
			if cm.Data == nil {
				cm.Data = make(map[string]string)
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

	// Get or create the ConfigMap
	configMap, needCreate, err := c.getOrCreateConfigMap(ctx)
	if err != nil {
		c.logger.Errorf("Failed to get or create ConfigMap for %s: %v", modelInfo, err)
		return err
	}
	c.logger.Debugf("Got ConfigMap (needCreate=%v) for %s: %+v", needCreate, modelInfo, configMap.Name)

	// Update the ConfigMap with status
	err = c.updateModelStatusInConfigMap(ctx, configMap, statusOp, needCreate)
	if err != nil {
		c.logger.Errorf("Failed to update model status in ConfigMap for %s: %v", modelInfo, err)
		return err
	}

	// Update the in-memory cache
	modelID := getModelID(statusOp.BaseModel, statusOp.ClusterBaseModel)
	c.cacheMutex.Lock()
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
			ModelStatus: statusOp.ModelStatus,
		}
		c.modelCache[modelID] = cacheEntry
	} else {
		// Just update the status in existing entry
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

// ReconcileModelMetadata updates the ConfigMap with model metadata
func (c *ConfigMapReconciler) ReconcileModelMetadata(ctx context.Context, op *ConfigMapMetadataOp) error {
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)
	c.logger.Infof("Reconciling model metadata in ConfigMap for %s", modelInfo)

	// Get or create the ConfigMap
	configMap, needCreate, err := c.getOrCreateConfigMap(ctx)
	if err != nil {
		c.logger.Errorf("Failed to get or create ConfigMap for %s: %v", modelInfo, err)
		return err
	}
	c.logger.Debugf("Got ConfigMap (needCreate=%v) for %s: %+v", needCreate, modelInfo, configMap.Name)

	// Update the ConfigMap with metadata
	err = c.updateModelMetadataInConfigMap(ctx, configMap, op, needCreate)
	if err != nil {
		c.logger.Errorf("Failed to update model metadata in ConfigMap for %s: %v", modelInfo, err)
		return err
	}

	// Update the in-memory cache with metadata
	modelID := getModelID(op.BaseModel, op.ClusterBaseModel)
	c.cacheMutex.Lock()
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
			ModelMetadata: &op.ModelMetadata,
		}
		c.modelCache[modelID] = cacheEntry
	} else {
		// Update the metadata
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

	// Use retry logic to handle concurrent updates (optimistic locking conflicts)
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// Get or create the ConfigMap (fresh on each retry)
		configMap, needCreate, err := c.getOrCreateConfigMap(ctx)
		if err != nil {
			return err
		}

		// Update the ConfigMap with progress
		return c.updateModelProgressInConfigMap(ctx, configMap, op, needCreate)
	})

	if err != nil {
		c.logger.Errorf("Failed to update model progress in ConfigMap for %s: %v", modelInfo, err)
		return err
	}

	return nil
}

// updateModelProgressInConfigMap updates the model progress in the ConfigMap
func (c *ConfigMapReconciler) updateModelProgressInConfigMap(ctx context.Context, configMap *corev1.ConfigMap, op *ConfigMapProgressOp, needCreate bool) error {
	// Get model information and key
	key := c.getModelConfigMapKey(op.BaseModel, op.ClusterBaseModel)
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)

	if configMap.Data == nil {
		configMap.Data = make(map[string]string)
	}

	// Get the existing model entry or create a new one
	var modelEntry ModelEntry
	var modelName string
	if op.BaseModel != nil {
		modelName = op.BaseModel.Name
	} else {
		modelName = op.ClusterBaseModel.Name
	}

	// Check if there's already an entry for this model
	if existingData, exists := configMap.Data[key]; exists {
		// If entry exists, try to unmarshal it
		if err := json.Unmarshal([]byte(existingData), &modelEntry); err != nil {
			// If it's not in our format yet, create a new entry
			modelEntry = ModelEntry{
				Name:   modelName,
				Status: ModelStatusUpdating,
			}
		}
	} else {
		// No existing entry, create a new one
		modelEntry = ModelEntry{
			Name:   modelName,
			Status: ModelStatusUpdating,
		}
	}

	// Update the progress (can be nil to clear it)
	modelEntry.Progress = op.Progress

	// Marshal the model entry back to JSON
	entryJSON, err := json.Marshal(modelEntry)
	if err != nil {
		c.logger.Errorf("Failed to marshal model entry for %s: %v", modelInfo, err)
		return err
	}

	// Store the model entry in the ConfigMap
	configMap.Data[key] = string(entryJSON)

	c.logger.Infof("will update model progress in configmap for key %s", key)
	return c.saveConfigMap(ctx, configMap, modelInfo, needCreate)
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

	// Get ConfigMap
	configMap, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// ConfigMap doesn't exist, nothing to delete
			return nil
		}
		c.logger.Errorf("Failed to get ConfigMap for model deletion: %v", err)
		return err
	}

	// Determine the model ID in the ConfigMap
	modelID := c.getModelConfigMapKey(baseModel, clusterBaseModel)

	// Check if model exists in ConfigMap
	if _, exists := configMap.Data[modelID]; !exists {
		c.logger.Infof("Model %s doesn't exist in ConfigMap, nothing to delete", modelInfo)
		return nil
	}

	// Delete the model from ConfigMap
	delete(configMap.Data, modelID)

	// Update the ConfigMap
	_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		c.logger.Errorf("Failed to update ConfigMap after model deletion: %v", err)
		return err
	}

	// Also update our in-memory cache to remove the model
	c.cacheMutex.Lock()
	if c.modelCache != nil {
		modelCacheID := getModelID(baseModel, clusterBaseModel)
		delete(c.modelCache, modelCacheID)
		c.logger.Debugf("Removed model %s from cache", modelInfo)
	}
	c.cacheMutex.Unlock()

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

// updateModelStatusInConfigMap updates the model status in the ConfigMap
func (c *ConfigMapReconciler) updateModelStatusInConfigMap(ctx context.Context, configMap *corev1.ConfigMap, op *ConfigMapStatusOp, needCreate bool) error {
	// Get model information and key
	key := c.getModelConfigMapKey(op.BaseModel, op.ClusterBaseModel)
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)
	c.logger.Debugf("Using key '%s' for model %s", key, modelInfo)

	if configMap.Data == nil {
		c.logger.Debugf("ConfigMap Data is nil, initializing it for %s", modelInfo)
		configMap.Data = make(map[string]string)
	}

	// Get the existing model entry or create a new one
	var modelEntry ModelEntry
	var modelName string
	if op.BaseModel != nil {
		modelName = op.BaseModel.Name
	} else {
		modelName = op.ClusterBaseModel.Name
	}

	// Check if there's already an entry for this model
	if existingData, exists := configMap.Data[key]; exists {
		// If entry exists, try to unmarshal it
		if err := json.Unmarshal([]byte(existingData), &modelEntry); err != nil {
			// If it's not in our format yet, create a new entry with just the status
			modelEntry = ModelEntry{
				Name:   modelName,
				Status: op.ModelStatus,
				Config: nil,
			}
		} else {
			// Update just the status, preserving the config
			modelEntry.Status = op.ModelStatus
			// Clear progress when status becomes Ready or Failed (download complete)
			// This ensures the controller sees the final status update atomically
			if op.ModelStatus == ModelStatusReady || op.ModelStatus == ModelStatusFailed {
				modelEntry.Progress = nil
			}
		}
	} else {
		// No existing entry, create a new one
		modelEntry = ModelEntry{
			Name:   modelName,
			Status: op.ModelStatus,
			Config: nil,
		}
	}

	// For 'ModelStatusDeleted' status, we might want to entirely remove the entry
	if op.ModelStatus == ModelStatusDeleted {
		c.logger.Debugf("Deleting ConfigMap data[%s] for %s", key, modelInfo)
		delete(configMap.Data, key)
	} else {
		// Marshal the model entry to JSON
		entryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			c.logger.Errorf("Failed to marshal model entry for %s: %v", modelInfo, err)
			return err
		}
		c.logger.Debugf("Setting ConfigMap data[%s] to %s for %s", key, string(entryJSON), modelInfo)
		configMap.Data[key] = string(entryJSON)
	}

	c.logger.Infof("will update model status in configmap for key %s", key)
	return c.saveConfigMap(ctx, configMap, modelInfo, needCreate)
}

// updateModelMetadataInConfigMap updates the model metadata in the ConfigMap
func (c *ConfigMapReconciler) updateModelMetadataInConfigMap(ctx context.Context, configMap *corev1.ConfigMap, op *ConfigMapMetadataOp, needCreate bool) error {
	// Get model information and key
	key := c.getModelConfigMapKey(op.BaseModel, op.ClusterBaseModel)
	modelInfo := getConfigMapModelInfo(op.BaseModel, op.ClusterBaseModel)
	c.logger.Debugf("Using key '%s' for model %s", key, modelInfo)

	if configMap.Data == nil {
		c.logger.Debugf("ConfigMap Data is nil, initializing it for %s", modelInfo)
		configMap.Data = make(map[string]string)
	}

	// Get the existing model entry or create a new one
	var modelEntry ModelEntry
	var modelName string
	if op.BaseModel != nil {
		modelName = op.BaseModel.Name
	} else {
		modelName = op.ClusterBaseModel.Name
	}

	// Check if there's already an entry for this model
	if existingData, exists := configMap.Data[key]; exists {
		// If entry exists, try to unmarshal it
		if err := json.Unmarshal([]byte(existingData), &modelEntry); err != nil {
			// If it's not in our format yet, create a new entry
			modelEntry = ModelEntry{
				Name:   modelName,
				Status: ModelStatusReady, // Default to ready when adding metadata
				Config: nil,
			}
		}
		// Keep existing status
	} else {
		// No existing entry, create a new one
		modelEntry = ModelEntry{
			Name:   modelName,
			Status: ModelStatusReady, // Default to ready when adding metadata
			Config: nil,
		}
	}

	// Create model config from metadata
	modelConfig := ConvertMetadataToModelConfig(op.ModelMetadata)

	// Update the config in the model entry
	modelEntry.Config = modelConfig

	// Marshal the model entry back to JSON
	entryJSON, err := json.Marshal(modelEntry)
	if err != nil {
		c.logger.Errorf("Failed to marshal model entry for %s: %v", modelInfo, err)
		return err
	}

	// Store the model entry in the ConfigMap
	configMap.Data[key] = string(entryJSON)
	c.logger.Debugf("Setting ConfigMap data[%s] = %s for %s", key, string(entryJSON), modelInfo)

	return c.saveConfigMap(ctx, configMap, modelInfo, needCreate)
}

// saveConfigMap creates or updates the ConfigMap in Kubernetes
// Uses retry.RetryOnConflict to handle concurrent updates to the same ConfigMap
func (c *ConfigMapReconciler) saveConfigMap(ctx context.Context, configMap *corev1.ConfigMap, modelInfo string, needCreate bool) error {
	// Create or update the ConfigMap
	if needCreate {
		c.logger.Infof("Creating new ConfigMap '%s' in namespace '%s' for %s", c.nodeName, c.namespace, modelInfo)
		_, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Create(ctx, configMap, metav1.CreateOptions{})
		if err != nil {
			c.logger.Errorf("Failed to create ConfigMap '%s' in namespace '%s' for %s: %v", c.nodeName, c.namespace, modelInfo, err)
			return err
		}
		c.logger.Infof("Successfully created ConfigMap '%s' in namespace '%s' for %s", c.nodeName, c.namespace, modelInfo)
	} else {
		c.logger.Infof("Updating ConfigMap '%s' in namespace '%s' for %s", c.nodeName, c.namespace, modelInfo)
		// Store the data we want to apply - this is the caller's intended changes
		dataToApply := configMap.Data

		updateConfigMap := func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
			if currentConfigMap.Data == nil {
				currentConfigMap.Data = make(map[string]string)
			}
			for key, value := range dataToApply {
				currentConfigMap.Data[key] = value
			}
			return true, currentConfigMap, nil
		}
		err := c.updateConfigMapWithRetry(ctx, updateConfigMap)

		if err != nil {
			c.logger.Errorf("Failed to update ConfigMap '%s' in namespace '%s' for %s: %v", c.nodeName, c.namespace, modelInfo, err)
			return err
		}
		c.logger.Infof("Successfully updated ConfigMap '%s' in namespace '%s' for %s", c.nodeName, c.namespace, modelInfo)
	}
	return nil
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
			if err == errHuggingFaceArtifactParentUpdating {
				c.logger.Infof("ConfigMap update deferred because Hugging Face artifact parent is Updating")
			} else {
				c.logger.Errorf("failed to compute updated ConfigMap: %s", err)
			}
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

func (c *ConfigMapReconciler) upsertHuggingFaceArtifactParentEntry(ctx context.Context, parentName string, identity ArtifactIdentity, parentPath string, childPath string) error {
	updateConfigMap := func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		if currentConfigMap.Data == nil {
			currentConfigMap.Data = make(map[string]string)
		}
		recordingChild := strings.TrimSpace(childPath) != ""

		modelEntry := ModelEntry{
			Name:   parentName,
			Status: ModelStatusReady,
			Config: &ModelConfig{
				Artifact: Artifact{
					Sha:           strings.ToLower(identity.HFCommitSHA),
					Origin:        identity.toOrigin(),
					ParentPath:    map[string]string{parentName: parentPath},
					ChildrenPaths: []string{},
				},
			},
		}
		if existingDataEntry, exists := currentConfigMap.Data[parentName]; exists {
			var existing ModelEntry
			if err := json.Unmarshal([]byte(existingDataEntry), &existing); err != nil {
				c.logger.Warnf("failed to parse existing Hugging Face artifact parent entry %s, rebuilding it: %v", parentName, err)
			} else {
				if recordingChild && existing.Status == ModelStatusUpdating {
					return false, currentConfigMap, errHuggingFaceArtifactParentUpdating
				}
				modelEntry = existing
				modelEntry.Name = parentName
				modelEntry.Status = ModelStatusReady
				if modelEntry.Config == nil {
					modelEntry.Config = &ModelConfig{}
				}
				modelEntry.Config.Artifact.Sha = strings.ToLower(identity.HFCommitSHA)
				modelEntry.Config.Artifact.Origin = identity.toOrigin()
				modelEntry.Config.Artifact.ParentPath = map[string]string{parentName: parentPath}
				if modelEntry.Config.Artifact.ChildrenPaths == nil {
					modelEntry.Config.Artifact.ChildrenPaths = []string{}
				}
			}
		}

		if recordingChild && !containsString(modelEntry.Config.Artifact.ChildrenPaths, childPath) {
			modelEntry.Config.Artifact.ChildrenPaths = append(modelEntry.Config.Artifact.ChildrenPaths, childPath)
		}

		entryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			return false, currentConfigMap, err
		}
		currentConfigMap.Data[parentName] = string(entryJSON)
		return true, currentConfigMap, nil
	}
	return c.updateConfigMapWithRetry(ctx, updateConfigMap)
}

// setHuggingFaceArtifactParentStatus updates the synthetic Hugging Face parent
// status while preserving its origin, parent path, and childrenPaths metadata.
func (c *ConfigMapReconciler) setHuggingFaceArtifactParentStatus(ctx context.Context, parentName string, identity ArtifactIdentity, parentPath string, status ModelStatus, skipIfUpdating bool) (string, bool, error) {
	observedParentPath := parentPath
	updated := false
	updateConfigMap := func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		observedParentPath = parentPath
		updated = false
		acquired := true
		if currentConfigMap.Data == nil {
			currentConfigMap.Data = make(map[string]string)
		}

		modelEntry := ModelEntry{
			Name:   parentName,
			Status: status,
			Config: &ModelConfig{
				Artifact: Artifact{
					Sha:           strings.ToLower(identity.HFCommitSHA),
					Origin:        identity.toOrigin(),
					ParentPath:    map[string]string{parentName: parentPath},
					ChildrenPaths: []string{},
				},
			},
		}
		if existingDataEntry, exists := currentConfigMap.Data[parentName]; exists {
			var existing ModelEntry
			if err := json.Unmarshal([]byte(existingDataEntry), &existing); err != nil {
				c.logger.Warnf("Replacing corrupt Hugging Face artifact parent entry %s while setting status %s: %v", parentName, status, err)
			} else {
				var existingParentPath string
				var existingChildrenPaths []string
				if existing.Config != nil {
					existingParentPath = existing.Config.Artifact.ParentPath[parentName]
					existingChildrenPaths = existing.Config.Artifact.ChildrenPaths
				}
				if existing.Config == nil || existing.Config.Artifact.Origin == nil {
					c.logger.Warnf("Repairing invalid Hugging Face artifact parent entry %s while setting status %s because origin metadata is missing", parentName, status)
					if strings.TrimSpace(existingParentPath) != "" {
						observedParentPath = existingParentPath
					}
					modelEntry.Config.Artifact.ChildrenPaths = existingChildrenPaths
					if skipIfUpdating && existing.Status == ModelStatusUpdating {
						acquired = false
					}
				} else {
					origin := existing.Config.Artifact.Origin
					if !strings.EqualFold(origin.Type, identity.OriginType) || origin.HFModelID != identity.HFModelID || !strings.EqualFold(origin.HFCommitSHA, identity.HFCommitSHA) {
						return false, currentConfigMap, fmt.Errorf("existing Hugging Face artifact parent entry %s has mismatched origin metadata", parentName)
					}
					parentPathMissing := strings.TrimSpace(existingParentPath) == ""
					if strings.TrimSpace(existingParentPath) != "" {
						observedParentPath = existingParentPath
					} else {
						c.logger.Warnf("Repairing invalid Hugging Face artifact parent entry %s while setting status %s because parent path is missing", parentName, status)
					}
					if skipIfUpdating && existing.Status == ModelStatusUpdating {
						if !parentPathMissing {
							return false, currentConfigMap, nil
						}
						acquired = false
					}
					modelEntry = existing
					modelEntry.Name = parentName
					modelEntry.Status = status
					modelEntry.Config.Artifact.Sha = strings.ToLower(identity.HFCommitSHA)
					modelEntry.Config.Artifact.Origin = identity.toOrigin()
					if modelEntry.Config.Artifact.ChildrenPaths == nil {
						modelEntry.Config.Artifact.ChildrenPaths = []string{}
					}
				}
				modelEntry.Config.Artifact.ParentPath = map[string]string{parentName: observedParentPath}
				if modelEntry.Config.Artifact.ChildrenPaths == nil {
					modelEntry.Config.Artifact.ChildrenPaths = []string{}
				}
			}
			modelEntry.Config.Artifact.Sha = strings.ToLower(identity.HFCommitSHA)
			modelEntry.Config.Artifact.Origin = identity.toOrigin()
		}

		entryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			return false, currentConfigMap, err
		}
		currentConfigMap.Data[parentName] = string(entryJSON)
		updated = acquired
		return true, currentConfigMap, nil
	}

	err := c.updateConfigMapWithRetry(ctx, updateConfigMap)
	return observedParentPath, updated, err
}

// markHuggingFaceArtifactParentUpdating marks the synthetic Hugging Face parent
// as Updating before a DownloadOverride repair writes to the shared parent path.
// Existing childrenPaths are preserved so ready child entries keep pointing to
// the same parent metadata while the repair runs. If another worker already
// owns the Updating parent, acquired is false and the caller should wait.
func (c *ConfigMapReconciler) markHuggingFaceArtifactParentUpdating(ctx context.Context, parentName string, identity ArtifactIdentity, parentPath string) (string, bool, error) {
	return c.setHuggingFaceArtifactParentStatus(ctx, parentName, identity, parentPath, ModelStatusUpdating, true)
}

func (c *ConfigMapReconciler) markHuggingFaceArtifactParentFailed(ctx context.Context, parentName string, identity ArtifactIdentity, parentPath string) error {
	_, _, err := c.setHuggingFaceArtifactParentStatus(ctx, parentName, identity, parentPath, ModelStatusFailed, false)
	return err
}

// reserveHuggingFaceArtifactParentEntry creates the synthetic parent in Updating
// state before the first worker downloads to the canonical path. Other workers
// see the reservation and wait instead of writing to the same directory.
func (c *ConfigMapReconciler) reserveHuggingFaceArtifactParentEntry(ctx context.Context, parentName string, identity ArtifactIdentity, parentPath string) (string, ModelStatus, bool, error) {
	var observedParentPath string
	var observedStatus ModelStatus
	var reserved bool

	writeReservation := func(currentConfigMap *corev1.ConfigMap, reservationParentPath string, childrenPaths []string, acquire bool) (bool, *corev1.ConfigMap, error) {
		if strings.TrimSpace(reservationParentPath) == "" {
			reservationParentPath = parentPath
		}
		if childrenPaths == nil {
			childrenPaths = []string{}
		}
		modelEntry := ModelEntry{
			Name:   parentName,
			Status: ModelStatusUpdating,
			Config: &ModelConfig{
				Artifact: Artifact{
					Sha:           strings.ToLower(identity.HFCommitSHA),
					Origin:        identity.toOrigin(),
					ParentPath:    map[string]string{parentName: reservationParentPath},
					ChildrenPaths: childrenPaths,
				},
			},
		}
		entryJSON, err := json.Marshal(modelEntry)
		if err != nil {
			return false, currentConfigMap, err
		}
		currentConfigMap.Data[parentName] = string(entryJSON)
		observedParentPath = reservationParentPath
		observedStatus = ModelStatusUpdating
		reserved = acquire
		return true, currentConfigMap, nil
	}

	updateConfigMap := func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		observedParentPath = ""
		observedStatus = ""
		reserved = false
		if currentConfigMap.Data == nil {
			currentConfigMap.Data = make(map[string]string)
		}

		if existingDataEntry, exists := currentConfigMap.Data[parentName]; exists {
			var existing ModelEntry
			if err := json.Unmarshal([]byte(existingDataEntry), &existing); err != nil {
				c.logger.Warnf("Replacing corrupt Hugging Face artifact parent entry %s before reservation: %v", parentName, err)
				return writeReservation(currentConfigMap, parentPath, nil, true)
			}
			var existingParentPath string
			var existingChildrenPaths []string
			if existing.Config != nil {
				existingParentPath = existing.Config.Artifact.ParentPath[parentName]
				existingChildrenPaths = existing.Config.Artifact.ChildrenPaths
			}
			if existing.Config == nil || existing.Config.Artifact.Origin == nil {
				c.logger.Warnf("Replacing invalid Hugging Face artifact parent entry %s before reservation because origin metadata is missing", parentName)
				return writeReservation(currentConfigMap, existingParentPath, existingChildrenPaths, existing.Status != ModelStatusUpdating)
			}
			origin := existing.Config.Artifact.Origin
			if !strings.EqualFold(origin.Type, identity.OriginType) || origin.HFModelID != identity.HFModelID || !strings.EqualFold(origin.HFCommitSHA, identity.HFCommitSHA) {
				return false, currentConfigMap, fmt.Errorf("existing Hugging Face artifact parent entry %s has mismatched origin metadata", parentName)
			}
			observedParentPath = existingParentPath
			if strings.TrimSpace(observedParentPath) == "" {
				c.logger.Warnf("Replacing invalid Hugging Face artifact parent entry %s before reservation because parent path is missing", parentName)
				return writeReservation(currentConfigMap, parentPath, existingChildrenPaths, existing.Status != ModelStatusUpdating)
			}
			observedStatus = existing.Status
			reserved = false
			return false, currentConfigMap, nil
		}

		return writeReservation(currentConfigMap, parentPath, nil, true)
	}

	err := c.updateConfigMapWithRetry(ctx, updateConfigMap)
	return observedParentPath, observedStatus, reserved, err
}

func (c *ConfigMapReconciler) deleteConfigMapDataEntry(ctx context.Context, key string) error {
	updateConfigMap := func(currentConfigMap *corev1.ConfigMap) (bool, *corev1.ConfigMap, error) {
		if currentConfigMap.Data == nil {
			return false, currentConfigMap, nil
		}
		if _, exists := currentConfigMap.Data[key]; !exists {
			return false, currentConfigMap, nil
		}
		delete(currentConfigMap.Data, key)
		return true, currentConfigMap, nil
	}
	return c.updateConfigMapWithRetry(ctx, updateConfigMap)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
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
