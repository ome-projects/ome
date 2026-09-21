package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
	// ModelEntryJSON preserves committed shared model state.
	// Ordinary models use the typed status and metadata fields for recovery.
	ModelEntryJSON string
}

// ConfigMapReconciler handles all ConfigMap operations for storing model state and metadata.
// It provides self-healing capabilities through periodic reconciliation to recover from
// manual ConfigMap deletions or modifications without requiring agent restarts.
type ConfigMapReconciler struct {
	kubeClient      kubernetes.Interface   // Kubernetes client for ConfigMap CRUD operations
	nodeName        string                 // The name of the node (used as ConfigMap name)
	namespace       string                 // The namespace to store the ConfigMap in
	logger          *zap.SugaredLogger     // Logger for recording operations
	modelCache      map[string]*CacheEntry // In-memory cache of model information
	hfArtifactCache map[string]string      // Immutable committed parent JSON, guarded by cacheMutex.
	evictedModels   map[string]struct{}    // Prevent observing an evicted model back into the cache.
	// Lock order: parent operation mutex (when held), configMapMutationMutex,
	// cacheMutex. Never hold cacheMutex across an API call or a mutation callback.
	configMapMutationMutex sync.Mutex
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
		hfArtifactCache:      make(map[string]string),
		evictedModels:        make(map[string]struct{}),
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

// reconcileConfigMaps restores missing records without treating cached parents as ready.
func (c *ConfigMapReconciler) reconcileConfigMaps() {
	c.cacheMutex.Lock()
	if c.isReconciling {
		c.cacheMutex.Unlock()
		return
	}
	c.isReconciling = true
	c.cacheMutex.Unlock()
	defer func() {
		c.cacheMutex.Lock()
		c.isReconciling = false
		c.cacheMutex.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.recreateConfigMap(ctx)
}

// recreateConfigMap fills missing records from the current cache using a CAS.
// Affected shared groups require validation; unrelated and corrupt records stay intact.
func (c *ConfigMapReconciler) recreateConfigMap(ctx context.Context) {
	if err := c.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		return c.restoreCachedConfigMapEntries(cm.Data, "", true)
	}); err != nil {
		c.logger.Errorf("Failed to restore ConfigMap from cache: %v", err)
	}
}

// restoreModelInConfigMap uses the snapshot only to check UID ownership. Data
// comes from the live cache under mutation serialization, never the old snapshot.
func (c *ConfigMapReconciler) restoreModelInConfigMap(modelID string, cacheEntry *CacheEntry) {
	if cacheEntry == nil || c.isModelRestoreBlocked(modelID, cacheEntry.ModelUID) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.getConfigMap(ctx); err != nil {
		if errors.IsNotFound(err) {
			c.recreateConfigMap(ctx)
		} else {
			c.logger.Errorf("Failed to read ConfigMap for model restore: %v", err)
		}
		return
	}
	if err := c.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		if c.isModelRestoreBlocked(modelID, cacheEntry.ModelUID) {
			return false, nil
		}
		return c.restoreCachedConfigMapEntries(cm.Data, modelID, true)
	}); err != nil {
		c.logger.Errorf("Failed to restore model %s: %v", modelID, err)
	}
}

// ReconcileModelStatus publishes status and caches the committed model entry.
func (c *ConfigMapReconciler) ReconcileModelStatus(ctx context.Context, statusOp *ConfigMapStatusOp) error {
	if err := c.updateModelStatusInConfigMap(ctx, statusOp); err != nil {
		return err
	}
	c.cacheOrdinaryModel(statusOp.BaseModel, statusOp.ClusterBaseModel, &statusOp.ModelStatus, nil)
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

// ReconcileModelMetadata publishes metadata and caches the committed model entry.
func (c *ConfigMapReconciler) ReconcileModelMetadata(ctx context.Context, op *ConfigMapMetadataOp) error {
	if err := c.updateModelMetadataInConfigMap(ctx, op); err != nil {
		return err
	}
	c.cacheOrdinaryModel(op.BaseModel, op.ClusterBaseModel, nil, &op.ModelMetadata)
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
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	c.cacheMutex.Lock()
	if cached := c.modelCache[modelID]; cached != nil && cached.ModelUID != "" && modelUID != "" && cached.ModelUID != modelUID {
		// Shared ownership needs an explicit UID handoff. Ordinary entries,
		// including completed opt-outs, retain their existing deletion behavior.
		var child ModelEntry
		if json.Unmarshal([]byte(cached.ModelEntryJSON), &child) == nil && child.HfArtifactKey != "" {
			c.cacheMutex.Unlock()
			return fmt.Errorf("cannot delete shared model %s owned by another UID", modelID)
		}
	}
	if isModelResourceDeleting(baseModel, clusterBaseModel) {
		// A deleting CR instance must never be restored or updated again.
		c.invalidateModelUIDAndEvictCacheLocked(modelID, modelUID)
	} else {
		// A later selector or affinity update may require new work using the same UID.
		c.evictCachedModelLocked(modelID)
	}
	c.cacheMutex.Unlock()

	// Delete must bypass its own UID invalidation; retries still operate on the latest ConfigMap.
	err := c.mutateConfigMapWithModelUIDLocked(ctx, modelID, modelUID, func(cm *corev1.ConfigMap) (bool, error) {
		raw, exists := cm.Data[modelID]
		if !exists {
			return false, nil
		}
		// Another process may attach a replacement after file cleanup.
		// Recheck the shared reference on every final-removal CAS attempt.
		var child ModelEntry
		if json.Unmarshal([]byte(raw), &child) == nil {
			if child.HfArtifactKey != "" {
				return false, fmt.Errorf("shared artifact ownership changed before deleting model %s", modelID)
			}
		}
		delete(cm.Data, modelID)
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
	return c.mutateConfigMapWithModelUID(ctx, "", "", mutate)
}

func (c *ConfigMapReconciler) mutateConfigMapWithModelUID(ctx context.Context, modelID string, modelUID types.UID, mutate configMapMutation) error {
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	return c.mutateConfigMapWithModelUIDLocked(ctx, modelID, modelUID, mutate)
}

// The caller holds configMapMutationMutex, including any preceding cache eviction.
func (c *ConfigMapReconciler) mutateConfigMapWithModelUIDLocked(ctx context.Context, modelID string, modelUID types.UID, mutate configMapMutation) error {
	return retry.OnError(retry.DefaultRetry, func(err error) bool {
		return errors.IsConflict(err) || errors.IsAlreadyExists(err)
	}, func() error {
		configMap, needCreate, err := c.getOrCreateConfigMap(ctx)
		if err != nil {
			return err
		}
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		before := maps.Clone(configMap.Data)
		restored := false
		if needCreate {
			restored, err = c.restoreCachedConfigMapEntries(configMap.Data, "", false)
		}
		if err != nil {
			return err
		}
		if _, found := configMap.Data[modelID]; modelID != "" && !found {
			modelRestored, err := c.restoreCachedConfigMapEntries(configMap.Data, modelID, false)
			if err != nil {
				return err
			}
			restored = restored || modelRestored
		}
		changed, err := mutate(configMap)
		if err != nil {
			return err
		}
		if changed || restored {
			if needCreate {
				_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Create(ctx, configMap, metav1.CreateOptions{})
			} else {
				_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, configMap, metav1.UpdateOptions{})
			}
			if err != nil {
				return err
			}
		}
		c.cacheCommittedConfigMapEntries(before, configMap.Data, modelID, modelUID)
		return nil
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
	return c.mutateConfigMapWithModelUID(ctx, modelID, modelUID, func(configMap *corev1.ConfigMap) (bool, error) {
		if !allowDeleting && c.isModelMutationBlocked(modelID, modelUID) {
			c.logger.Debugf("Skipping stale ConfigMap mutation for model %s", modelID)
			return false, nil
		}
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		if !allowDeleting {
			if err := c.validateHfArtifactChildMutation(configMap.Data, modelID); err != nil {
				return false, err
			}
		}
		return mutate(configMap.Data)
	})
}

// invalidateModelUIDAndEvictCache atomically prevents further mutations from
// this CR instance and removes its cached entry. Invalidation applies to
// modelUID, not the reusable model name, so a recreated CR with a new UID
// remains valid.
// Evicting the cache prevents periodic reconciliation from restoring the old
// ConfigMap entry during deletion.
func (c *ConfigMapReconciler) invalidateModelUIDAndEvictCache(modelID string, modelUID types.UID) {
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	c.invalidateModelUIDAndEvictCacheLocked(modelID, modelUID)
}

// The caller holds configMapMutationMutex and cacheMutex.
func (c *ConfigMapReconciler) invalidateModelUIDAndEvictCacheLocked(modelID string, modelUID types.UID) {
	c.evictCachedModelLocked(modelID)
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

	readyBlocked := false
	err := c.mutateModelEntryWithRetry(ctx, key, modelUID, false, func(data map[string]string) (bool, error) {
		readyBlocked = false
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
		if op.ModelStatus == ModelStatusReady && modelEntry.HfArtifactKey != "" {
			parent, valid := hfArtifactRecoveryParent(modelEntry.HfArtifactKey, data[modelEntry.HfArtifactKey])
			if !valid || parent.Status != HfArtifactStatusReady {
				// Commit any reconstructed Failed state, but do not let this
				// stale completion bypass shared-parent validation in that CAS.
				readyBlocked = true
				return false, nil
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
	if err == nil && readyBlocked {
		return fmt.Errorf("shared artifact for model %s requires validation before Ready", key)
	}
	return err
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
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Re-fetch the latest ConfigMap to get current ResourceVersion
		latestCM, err := c.kubeClient.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.nodeName, metav1.GetOptions{})
		if err != nil {
			c.logger.Errorf("failed to get ConfigMap from Kube API server: %s", err)
			return err
		}

		before := maps.Clone(latestCM.Data)
		needUpdate, updatedConfigmap, err := updateConfigmap(latestCM)
		if err != nil {
			c.logger.Errorf("failed to compute updated ConfigMap: %s", err)
			return err
		}
		if !needUpdate {
			c.cacheCommittedConfigMapEntries(before, latestCM.Data, "", "")
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
		c.cacheCommittedConfigMapEntries(before, updatedConfigmap.Data, "", "")
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
