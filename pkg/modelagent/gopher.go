package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	omev1beta1lister "github.com/sgl-project/ome/pkg/client/listers/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/logging"
	"github.com/sgl-project/ome/pkg/ociobjectstore"
	"github.com/sgl-project/ome/pkg/principals"
	"github.com/sgl-project/ome/pkg/utils"
	"github.com/sgl-project/ome/pkg/utils/storage"
	"github.com/sgl-project/ome/pkg/xet"
)

type GopherTaskType string

const (
	Download         GopherTaskType = "Download"
	DownloadOverride GopherTaskType = "DownloadOverride"
	Delete           GopherTaskType = "Delete"
)

type GopherTask struct {
	TaskType               GopherTaskType
	BaseModel              *v1beta1.BaseModel
	ClusterBaseModel       *v1beta1.ClusterBaseModel
	TensorRTLLMShapeFilter *TensorRTLLMShapeFilter
}

type Gopher struct {
	modelConfigParser      *ModelConfigParser
	configMapReconciler    *ConfigMapReconciler
	downloadRetry          int
	concurrency            int
	multipartConcurrency   int
	modelRootDir           string
	xetConfig              *xet.Config
	kubeClient             kubernetes.Interface
	gopherChan             <-chan *GopherTask
	nodeLabelReconciler    *NodeLabelReconciler
	metrics                *Metrics
	logger                 *zap.SugaredLogger
	configMapMutex         sync.Mutex // Mutex to coordinate ConfigMap access
	baseModelLister        omev1beta1lister.BaseModelLister
	clusterBaseModelLister omev1beta1lister.ClusterBaseModelLister

	// Track active downloads for cancellation
	activeDownloads      map[string]context.CancelFunc // key: model UID
	activeDownloadsMutex sync.RWMutex
}

const (
	BigFileSizeInMB = 200
)

func NewGopher(
	modelConfigParser *ModelConfigParser,
	configMapReconciler *ConfigMapReconciler,
	xetConfig *xet.Config,
	kubeClient kubernetes.Interface,
	concurrency int,
	multipartConcurrency int,
	downloadRetry int,
	modelRootDir string,
	gopherChan <-chan *GopherTask,
	nodeLabelReconciler *NodeLabelReconciler,
	metrics *Metrics,
	logger *zap.SugaredLogger,
	baseModelLister omev1beta1lister.BaseModelLister,
	clusterBaseModelLister omev1beta1lister.ClusterBaseModelLister) (*Gopher, error) {

	if xetConfig == nil {
		return nil, fmt.Errorf("xet hugging face config cannot be nil")
	}

	return &Gopher{
		modelConfigParser:      modelConfigParser,
		configMapReconciler:    configMapReconciler,
		downloadRetry:          downloadRetry,
		concurrency:            concurrency,
		multipartConcurrency:   multipartConcurrency,
		modelRootDir:           modelRootDir,
		xetConfig:              xetConfig,
		kubeClient:             kubeClient,
		gopherChan:             gopherChan,
		nodeLabelReconciler:    nodeLabelReconciler,
		metrics:                metrics,
		logger:                 logger,
		activeDownloads:        make(map[string]context.CancelFunc),
		baseModelLister:        baseModelLister,
		clusterBaseModelLister: clusterBaseModelLister,
	}, nil
}

func (s *Gopher) Run(stopCh <-chan struct{}, numWorker int) {
	// Start the ConfigMap reconciliation service
	s.configMapReconciler.StartReconciliation()
	s.logger.Info("Started ConfigMap reconciliation service")

	// Start worker goroutines
	for i := 0; i < numWorker; i++ {
		go s.runWorker()
	}

	// Wait for stop signal
	<-stopCh

	// Stop the ConfigMap reconciliation service
	s.configMapReconciler.StopReconciliation()
	s.logger.Info("Stopped ConfigMap reconciliation service")

	s.logger.Info("Received stop signal, shutting down Gopher workers...")
}

func (s *Gopher) runWorker() {
	for {
		select {
		case task, ok := <-s.gopherChan:
			if ok {
				// Process delete tasks immediately by checking active downloads
				if task.TaskType == Delete {
					modelUID := getModelUID(task)
					s.activeDownloadsMutex.RLock()
					_, isDownloading := s.activeDownloads[modelUID]
					s.activeDownloadsMutex.RUnlock()

					if isDownloading {
						s.logger.Infof("Model %s is currently downloading, will cancel it", getModelInfoForLogging(task))
					}
				}

				err := s.processTask(task)
				if err != nil {
					s.logger.Errorf("Gopher task failed with error: %s", err.Error())
				}
			} else {
				s.logger.Info("gopher channel closed, worker exits.")
				return
			}
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// safeNodeLabelReconciliation executes the NodeLabelReconciler's ReconcileNodeLabels method with mutex protection
// to ensure thread-safe ConfigMap updates
func (s *Gopher) safeNodeLabelReconciliation(op *NodeLabelOp) error {
	ctx := context.Background()
	s.configMapMutex.Lock()
	defer s.configMapMutex.Unlock()

	// Mark the node label
	err := s.nodeLabelReconciler.ReconcileNodeLabels(op)
	if err != nil {
		return err
	}

	// Also update the ConfigMap with model status
	if op.BaseModel != nil || op.ClusterBaseModel != nil {
		// Convert ModelStateOnNode to ModelStatus
		var status ModelStatus
		switch op.ModelStateOnNode {
		case Ready:
			status = ModelStatusReady
		case Updating:
			status = ModelStatusUpdating
		case Failed:
			status = ModelStatusFailed
		case Deleted:
			// For deletion, use the DeleteModelFromConfigMap method instead
			return s.configMapReconciler.DeleteModelFromConfigMap(ctx, op.BaseModel, op.ClusterBaseModel)
		}

		// Create StatusOp for ConfigMap update
		statusOp := &ConfigMapStatusOp{
			ModelStatus:      status,
			BaseModel:        op.BaseModel,
			ClusterBaseModel: op.ClusterBaseModel,
		}

		// Update the ConfigMap with model status
		return s.configMapReconciler.ReconcileModelStatus(ctx, statusOp)
	}

	return nil
}

// safeParseAndUpdateModelConfig executes the ModelConfigParser's ParseAndUpdateModelConfig method with mutex protection
// to ensure thread-safe ConfigMap updates
func (s *Gopher) safeParseAndUpdateModelConfig(modelPath string, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel, artifact *Artifact) error {
	ctx := context.Background()
	s.configMapMutex.Lock()
	defer s.configMapMutex.Unlock()

	// First parse the configuration without updating the ConfigMap
	// This call will return model metadata
	metadata, err := s.modelConfigParser.ParseModelConfig(modelPath, baseModel, clusterBaseModel)
	if err != nil {
		return err
	}

	// add artifact info if necessary
	if artifact != nil {
		metadata = s.modelConfigParser.populateArtifactAttribute(artifact, metadata)
	}

	// If valid metadata was found, update the ConfigMap while still holding the lock
	if metadata != nil {
		op := &ConfigMapMetadataOp{
			ModelMetadata:    *metadata,
			BaseModel:        baseModel,
			ClusterBaseModel: clusterBaseModel,
		}

		// Update the ConfigMap with model configuration
		// Since we're holding the lock, we can call the ReconcileModelMetadata method directly
		return s.configMapReconciler.ReconcileModelMetadata(ctx, op)
	}

	return nil
}

func (s *Gopher) processTask(task *GopherTask) error {
	if task.BaseModel == nil && task.ClusterBaseModel == nil {
		return fmt.Errorf("gopher got empty task")
	}

	// Get model info for logging
	modelInfo := getModelInfoForLogging(task)
	modelUID := getModelUID(task)
	s.logger.Infof("Processing gopher task: %s, type: %s", modelInfo, task.TaskType)

	// Get model type, namespace, and name for metrics
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)

	var baseModelSpec v1beta1.BaseModelSpec
	if task.BaseModel != nil {
		baseModelSpec = task.BaseModel.Spec
	} else {
		baseModelSpec = task.ClusterBaseModel.Spec
	}

	// Create context - will be cancellable for downloads
	ctx := context.Background()
	var cancel context.CancelFunc

	// For Download and DownloadOverride tasks, set the node label to "Updating"
	if task.TaskType == Download || task.TaskType == DownloadOverride {
		// Atomic check-and-set: skip if another worker is already downloading this model
		s.activeDownloadsMutex.Lock()
		if _, isDownloading := s.activeDownloads[modelUID]; isDownloading {
			s.activeDownloadsMutex.Unlock()
			s.logger.Infof("Model %s is already being downloaded, skipping duplicate download", modelInfo)
			return nil
		}
		// Register the download before releasing the lock to prevent races
		ctx, cancel = context.WithCancel(context.Background())
		s.activeDownloads[modelUID] = cancel
		s.activeDownloadsMutex.Unlock()

		s.logger.Infof("Setting model %s status to Updating before download", modelInfo)
		nodeLabelOp := &NodeLabelOp{
			ModelStateOnNode: Updating,
			BaseModel:        task.BaseModel,
			ClusterBaseModel: task.ClusterBaseModel,
		}

		if err := s.safeNodeLabelReconciliation(nodeLabelOp); err != nil {
			s.logger.Errorf("Failed to set model %s status to Updating: %v", modelInfo, err)
			// Continue with download anyway
		}

		// Ensure cleanup on completion
		defer func() {
			s.activeDownloadsMutex.Lock()
			delete(s.activeDownloads, modelUID)
			s.activeDownloadsMutex.Unlock()
			cancel() // Ensure context is cancelled
		}()
	}

	storageType, err := storage.GetStorageType(*baseModelSpec.Storage.StorageUri)

	if err != nil {
		s.logger.Errorf("Failed to get target directory path for model %s: %v", modelInfo, err)

		// Record failed download in metrics
		if task.TaskType == Download || task.TaskType == DownloadOverride {
			s.metrics.RecordFailedDownload(modelType, namespace, name, "target_path_error")
		}

		s.markModelOnNodeFailed(task)
		return err
	}

	switch task.TaskType {
	case Download:
		// we might implement a "delete/cleanup and then download" logic to update a model in the future
		// use a single download function for now
		fallthrough
	case DownloadOverride:
		s.logger.Infof("Starting download for model %s", modelInfo)

		// Record time for metrics
		downloadStartTime := time.Now()
		switch storageType {
		case storage.StorageTypeOCI:
			osUri, err := getTargetDirPath(&baseModelSpec)
			destPath := getDestPath(&baseModelSpec, s.modelRootDir)
			if err != nil {
				s.logger.Errorf("Failed to get target directory path for model %s: %v", modelInfo, err)
				return err
			}
			err = utils.Retry(s.downloadRetry, 100*time.Millisecond, func() error {
				downloadErr := s.downloadModel(ctx, osUri, destPath, task)
				if downloadErr != nil {
					// Check if context was cancelled
					if ctx.Err() != nil {
						s.logger.Infof("Download cancelled for model %s: %v", modelInfo, ctx.Err())
						return ctx.Err()
					}
					s.logger.Errorf("Failed to download model %s (attempt %d/%d): %v",
						modelInfo, s.downloadRetry, s.downloadRetry, downloadErr)
				}
				return downloadErr
			})
			if err != nil {
				s.logger.Errorf("All download attempts failed for model %s: %v", modelInfo, err)

				// Record download failure in metrics
				errorType := "download_error"
				if strings.Contains(err.Error(), "MD5") {
					errorType = "md5_verification_error"
				}
				s.metrics.RecordFailedDownload(modelType, namespace, name, errorType)

				s.markModelOnNodeFailed(task)
				return err
			}
			// Parse model config and update ConfigMap
			// We can pass either BaseModel or ClusterBaseModel based on the task's model type
			var baseModel *v1beta1.BaseModel
			var clusterBaseModel *v1beta1.ClusterBaseModel

			// Check the actual model type from the task
			if task.BaseModel != nil {
				baseModel = task.BaseModel
				s.logger.Debugf("Using BaseModel %s/%s for config parsing", baseModel.Namespace, baseModel.Name)
			} else if task.ClusterBaseModel != nil {
				clusterBaseModel = task.ClusterBaseModel
				s.logger.Debugf("Using ClusterBaseModel %s for config parsing", clusterBaseModel.Name)
			} else {
				s.logger.Warnf("No model object found in task, skipping config parsing")
			}

			if err := s.safeParseAndUpdateModelConfig(destPath, baseModel, clusterBaseModel, nil); err != nil {
				s.logger.Errorf("Failed to parse and update model config: %v", err)
			}
		case storage.StorageTypeVendor:
			s.logger.Infof("Skipping download for model %s", modelInfo)
		case storage.StorageTypeHuggingFace:
			s.logger.Infof("Starting Hugging Face download for model %s", modelInfo)

			// Handle Hugging Face model download
			if err := s.processHuggingFaceModel(ctx, task, baseModelSpec, modelInfo, modelType, namespace, name); err != nil {
				// Error is already logged and metrics recorded in the method
				return err
			}
		case storage.StorageTypePVC:
			s.logger.Infof("Skipping PVC storage type for model %s (handled by BaseModel controller)", modelInfo)
			// PVC storage is handled entirely by the BaseModel controller
			// Model agent doesn't need to do anything for PVC storage
			return nil
		case storage.StorageTypeLocal:
			s.logger.Infof("Processing local storage type for model %s", modelInfo)
			// For local storage, we just need to validate the path exists and parse model config
			if err := s.processLocalStorageModel(ctx, task, baseModelSpec, modelInfo, modelType, namespace, name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown storage type %s", storageType)
		}
		// Calculate download duration
		downloadDuration := time.Since(downloadStartTime)

		// Record successful download in metrics
		s.metrics.RecordSuccessfulDownload(modelType, namespace, name)
		s.metrics.ObserveDownloadDuration(modelType, namespace, name, downloadDuration)

		if task.BaseModel != nil {
			s.logger.Infof("Successfully downloaded BaseModel %s in namespace %s", task.BaseModel.Name, task.BaseModel.Namespace)
		} else {
			s.logger.Infof("Successfully downloaded ClusterBaseModel %s", task.ClusterBaseModel.Name)
		}

		// mark the model as Ready on both node labels and ConfigMap
		nodeLabelOp := &NodeLabelOp{
			ModelStateOnNode: Ready,
			BaseModel:        task.BaseModel,
			ClusterBaseModel: task.ClusterBaseModel,
		}

		// This will update both the node label and ConfigMap status
		err = s.safeNodeLabelReconciliation(nodeLabelOp)
		if err != nil {
			s.logger.Errorf("Failed to mark model %s as Ready: %v", modelInfo, err)
			return err
		}
	case Delete:
		// First, cancel any ongoing download for this model
		s.activeDownloadsMutex.RLock()
		if cancelFunc, exists := s.activeDownloads[modelUID]; exists {
			s.logger.Infof("Cancelling ongoing download for model %s", modelInfo)
			cancelFunc() // This will cancel the download context
		}
		s.activeDownloadsMutex.RUnlock()

		// Wait a bit for download to stop
		time.Sleep(2 * time.Second)

		// Now proceed with deletion
		switch storageType {
		case storage.StorageTypeOCI:
			s.logger.Infof("Starting deletion for model %s", modelInfo)
			destPath := getDestPath(&baseModelSpec, s.modelRootDir)
			// check if it needs to skip artifact deletion
			isSkippingDeletion, _, _, _ := s.isSkippingArtifactDeletion(ctx, task, destPath, false)
			if !isSkippingDeletion {
				err = s.deleteModel(destPath, task)
				if err != nil {
					s.logger.Errorf("Failed to delete model %s: %v", modelInfo, err)
					return err
				}
				if task.BaseModel != nil {
					s.logger.Infof("Successfully deleted the BaseModel %s in namespace %s", task.BaseModel.Name, task.BaseModel.Namespace)
				} else {
					s.logger.Infof("Successfully deleted the ClusterBaseModel %s", task.ClusterBaseModel.Name)
				}
			}
		case storage.StorageTypeVendor:
			s.logger.Infof("Skipping deletion for model %s", modelInfo)
		case storage.StorageTypeHuggingFace:
			s.logger.Infof("Removing Hugging Face model %s", modelInfo)
			// Use getDestPath to get the same path used during download
			destPath := getDestPath(&baseModelSpec, s.modelRootDir)

			// check if it needs to skip artifact deletion
			isSkippingDeletion, isRemoveParent, parentName, parentDir := s.isSkippingArtifactDeletion(ctx, task, destPath, true)

			if !isSkippingDeletion {
				err = s.deleteModel(destPath, task)
				if err != nil {
					s.logger.Errorf("Failed to delete Hugging Face model %s: %v", modelInfo, err)
					return err
				}
				s.logger.Infof("Successfully deleted Hugging Face model %s", modelInfo)
			} else {
				s.logger.Infof("model %s artifact deletion will be skipped", modelInfo)
			}
			if isRemoveParent && parentName != "" && parentDir != "" {
				// check whether the parent directory still has other directory points to it using symbolic link
				isParentHasSymbolicLinkPointedTo, symbolicLinkSearchErr := utils.HasSymlinkPointingToDir(s.modelRootDir, parentDir)
				if symbolicLinkSearchErr != nil {
					s.logger.Infof("fails to search for the SymbolicLink pointing to parent Dir %s: %v. will regard the parent is still being pointed conservatively", parentDir, symbolicLinkSearchErr)
					isParentHasSymbolicLinkPointedTo = true
				}
				s.logger.Infof("parent %s:%s has other directory points to: %v", parentName, parentDir, isParentHasSymbolicLinkPointedTo)
				if !isParentHasSymbolicLinkPointedTo {
					err = s.deleteModel(parentDir, nil)
					if err != nil {
						s.logger.Errorf("fail to delete parent model artifact directory %s: %s", parentName, parentDir)
					}
					s.logger.Infof("Successfully delete parent model artifact directory %s: %s", parentName, parentDir)
				}
			} else {
				s.logger.Infof("no need to delete parent model artifact directory %s: %s", parentName, parentDir)
			}
		case storage.StorageTypeLocal:
			s.logger.Infof("Skipping deletion for local storage model %s (local files should not be deleted)", modelInfo)
			// For local storage, we should NOT delete the actual files
			// Just update the node labels and ConfigMap to reflect removal
		case storage.StorageTypePVC:
			s.logger.Infof("Skipping deletion for PVC storage model %s (handled by BaseModel controller)", modelInfo)
			// PVC storage is handled entirely by the BaseModel controller
			// Model agent doesn't delete PVC volumes
		default:
			s.logger.Warnf("Unsupported storage type %s for deletion of model %s", storageType, modelInfo)
		}

		// Mark the model as deleted in the node labels and remove from ConfigMap
		nodeLabelOp := &NodeLabelOp{
			ModelStateOnNode: Deleted,
			BaseModel:        task.BaseModel,
			ClusterBaseModel: task.ClusterBaseModel,
		}

		err = s.safeNodeLabelReconciliation(nodeLabelOp)
		if err != nil {
			s.logger.Errorf("Failed to mark model %s as deleted: %v", modelInfo, err)
			return err
		}

		// Clean up the active downloads map
		s.activeDownloadsMutex.Lock()
		delete(s.activeDownloads, modelUID)
		s.activeDownloadsMutex.Unlock()
	}

	return nil
}

// isPathReferencedByOtherModels checks if the given path is still referenced by other BaseModel or ClusterBaseModel resources
// excluding the model being deleted
func (s *Gopher) isPathReferencedByOtherModels(targetPath string, excludeBaseModel *v1beta1.BaseModel, excludeClusterBaseModel *v1beta1.ClusterBaseModel) (bool, error) {
	// Check BaseModels
	baseModels, err := s.baseModelLister.List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("failed to list BaseModels: %w", err)
	}

	for _, baseModel := range baseModels {
		// Skip the model being deleted
		if excludeBaseModel != nil && baseModel.Namespace == excludeBaseModel.Namespace && baseModel.Name == excludeBaseModel.Name {
			continue
		}

		// Check if this BaseModel references the same path
		if baseModel.Spec.Storage.Path != nil && *baseModel.Spec.Storage.Path == targetPath {
			s.logger.Infof("Path %s is still referenced by BaseModel %s/%s", targetPath, baseModel.Namespace, baseModel.Name)
			return true, nil
		}
	}

	// Check ClusterBaseModels
	clusterBaseModels, err := s.clusterBaseModelLister.List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("failed to list ClusterBaseModels: %w", err)
	}

	for _, clusterBaseModel := range clusterBaseModels {
		// Skip the model being deleted
		if excludeClusterBaseModel != nil && clusterBaseModel.Name == excludeClusterBaseModel.Name {
			continue
		}

		// Check if this ClusterBaseModel references the same path
		if clusterBaseModel.Spec.Storage.Path != nil && *clusterBaseModel.Spec.Storage.Path == targetPath {
			s.logger.Infof("Path %s is still referenced by ClusterBaseModel %s", targetPath, clusterBaseModel.Name)
			return true, nil
		}
	}

	return false, nil
}

func getModelInfoForLogging(task *GopherTask) string {
	if task.BaseModel != nil {
		return fmt.Sprintf("BaseModel %s/%s", task.BaseModel.Namespace, task.BaseModel.Name)
	} else if task.ClusterBaseModel != nil {
		return fmt.Sprintf("ClusterBaseModel %s", task.ClusterBaseModel.Name)
	}
	return "unknown model"
}

// getModelUID returns the unique identifier for a model
func getModelUID(task *GopherTask) string {
	if task.BaseModel != nil {
		return string(task.BaseModel.UID)
	} else if task.ClusterBaseModel != nil {
		return string(task.ClusterBaseModel.UID)
	}
	return ""
}

func (s *Gopher) markModelOnNodeFailed(task *GopherTask) {
	modelInfo := getModelInfoForLogging(task)
	s.logger.Infof("Marking model %s as Failed on node", modelInfo)

	nodeLabelOp := &NodeLabelOp{
		ModelStateOnNode: Failed,
		BaseModel:        task.BaseModel,
		ClusterBaseModel: task.ClusterBaseModel,
	}

	// This will update both node label and ConfigMap status
	err := s.safeNodeLabelReconciliation(nodeLabelOp)
	if err != nil {
		s.logger.Errorf("Failed to mark model %s as Failed on node: %v", modelInfo, err)
	} else {
		s.logger.Infof("Successfully marked model %s as Failed on node", modelInfo)
	}
}

// getHuggingFaceToken retrieves authentication token for Hugging Face models.
// It attempts to get the token from either a Kubernetes secret or direct parameters.
func (s *Gopher) getHuggingFaceToken(task *GopherTask, baseModelSpec v1beta1.BaseModelSpec, modelInfo string) string {
	var hfToken string
	var namespace string

	// Get namespace depending on model type
	if task.BaseModel != nil {
		namespace = task.BaseModel.Namespace
	} else if task.ClusterBaseModel != nil {
		// ClusterBaseModels look for secrets in the ome namespace by default
		namespace = "ome"
	}

	// Try to get token from storage key first (Kubernetes secret)
	if baseModelSpec.Storage.StorageKey != nil && *baseModelSpec.Storage.StorageKey != "" {
		// Get the token from the referenced Kubernetes secret
		if s.kubeClient != nil {
			s.logger.Infof("Fetching Hugging Face token from secret %s in namespace %s for model %s", *baseModelSpec.Storage.StorageKey, namespace, modelInfo)

			secret, err := s.kubeClient.CoreV1().Secrets(namespace).Get(context.Background(), *baseModelSpec.Storage.StorageKey, metav1.GetOptions{})
			if err != nil {
				s.logger.Warnf("Failed to retrieve secret %s in namespace %s for Hugging Face token: %v", *baseModelSpec.Storage.StorageKey, namespace, err)
			} else {
				// Check if a custom secret key name is specified in parameters
				secretKeyName := "token" // default key name
				if baseModelSpec.Storage.Parameters != nil {
					if customKey, exists := (*baseModelSpec.Storage.Parameters)["secretKey"]; exists && customKey != "" {
						secretKeyName = customKey
						s.logger.Infof("Using custom secret key name '%s' for model %s", secretKeyName, modelInfo)
					}
				}

				// Try to get the token using the determined key name
				if tokenBytes, exists := secret.Data[secretKeyName]; exists {
					hfToken = string(tokenBytes)
					s.logger.Infof("Successfully retrieved Hugging Face token from secret %s in namespace %s using key '%s'", *baseModelSpec.Storage.StorageKey, namespace, secretKeyName)
				} else {
					s.logger.Warnf("Secret %s in namespace %s does not contain key '%s'", *baseModelSpec.Storage.StorageKey, namespace, secretKeyName)
				}
			}
		} else {
			s.logger.Warnf("Cannot fetch token: Kubernetes client not initialized")
		}
	}

	// Fallback to parameters if token not found in secret or no secret provided
	if hfToken == "" && baseModelSpec.Storage.Parameters != nil {
		if token, exists := (*baseModelSpec.Storage.Parameters)["token"]; exists {
			hfToken = token
			s.logger.Infof("Using token from Parameters for model %s", modelInfo)
		}
	}

	return hfToken
}

func getDestPath(baseModel *v1beta1.BaseModelSpec, modelRootDir string) string {

	storagePath := *baseModel.Storage.StorageUri
	destPath := *baseModel.Storage.Path

	if len(destPath) == 0 {
		if strings.HasSuffix(modelRootDir, "/") {
			return modelRootDir + storagePath
		} else {
			return modelRootDir + "/" + storagePath
		}
	}

	return destPath
}

// getTargetDirPath determines the target directory path for a model based on its storage configuration
func getTargetDirPath(baseModel *v1beta1.BaseModelSpec) (*ociobjectstore.ObjectURI, error) {

	storagePath := *baseModel.Storage.StorageUri

	osUri, err := storage.NewObjectURI(storagePath)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(osUri.Prefix, "/") {
		osUri.Prefix = osUri.Prefix + "/"
	}

	return osUri, nil

}

// createOCIOSDataStore creates an OCIOSDataStore client based on storage parameters in the model spec
func (s *Gopher) createOCIOSDataStore(baseModelSpec v1beta1.BaseModelSpec) (*ociobjectstore.OCIOSDataStore, error) {
	// Default auth type is InstancePrincipal if not specified
	authType := principals.InstancePrincipal

	// Check if auth type is specified in the storage parameters
	if baseModelSpec.Storage.Parameters != nil {
		if authTypeStr, ok := (*baseModelSpec.Storage.Parameters)["auth"]; ok && authTypeStr != "" {
			// Convert string to AuthenticationType
			authType = principals.AuthenticationType(authTypeStr)
			s.logger.Infof("Using auth type from model parameters: %s", authType)
		}
	}

	// Create OCI Object Store config with a proper logger adapter
	osConfig, err := ociobjectstore.NewConfig(
		ociobjectstore.WithAnotherLog(logging.ForZap(s.logger.Desugar())),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ociobjectstore config: %w", err)
	}

	// Set auth type
	osConfig.AuthType = &authType

	// Check for additional parameters like region
	if baseModelSpec.Storage.Parameters != nil {
		if region, ok := (*baseModelSpec.Storage.Parameters)["region"]; ok && region != "" {
			osConfig.Region = region
			s.logger.Infof("Using region from model parameters: %s", region)
		}
	}

	// Create OCIOSDataStore
	ociOSDS, err := ociobjectstore.NewOCIOSDataStore(osConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ociobjectstore data store: %w", err)
	}

	return ociOSDS, nil
}

func (s *Gopher) downloadModel(ctx context.Context, uri *ociobjectstore.ObjectURI, destPath string, task *GopherTask) error {
	startTime := time.Now()
	defer func() {
		s.logger.Infof("Download process took %v", time.Since(startTime).Round(time.Millisecond))
	}()

	// Get model type, namespace, and name for metrics outside the defer to use within function
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)

	// Get the model spec
	var baseModelSpec v1beta1.BaseModelSpec
	if task.BaseModel != nil {
		baseModelSpec = task.BaseModel.Spec
	} else {
		baseModelSpec = task.ClusterBaseModel.Spec
	}

	// Create oci object storage data store client for this task
	ociOSDataStore, err := s.createOCIOSDataStore(baseModelSpec)
	if err != nil {
		return fmt.Errorf("failed to create object storage client: %w", err)
	}

	// Check context before making expensive operations
	select {
	case <-ctx.Done():
		return fmt.Errorf("download cancelled before listing objects: %w", ctx.Err())
	default:
	}

	s.logger.Infof("Making call to object storage with endpoint %s", ociOSDataStore.Client.Endpoint())
	objects, err := ociOSDataStore.ListObjects(*uri)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	if len(objects) == 0 {
		return fmt.Errorf("no objects found under namespace %s, bucket %s, object prefix %s", uri.Namespace, uri.BucketName, uri.Prefix)
	}

	s.logger.Infof("Done with list all %d objects in model bucket folder", len(objects))

	// Shape filtering for TensorRTLLM
	if task.TensorRTLLMShapeFilter != nil && task.TensorRTLLMShapeFilter.IsTensorrtLLMModel && task.TensorRTLLMShapeFilter.ModelType == string(constants.ServingBaseModel) {
		s.logger.Infof("TensorRTLLM Serving model detected. Start filtering model files that doesn't belong to the node shape %s in model bucket folder", task.TensorRTLLMShapeFilter.ShapeAlias)
		shapeFilteredObjects := make([]objectstorage.ObjectSummary, 0)
		for _, object := range objects {
			if object.Name != nil {
				if strings.Contains(*object.Name, fmt.Sprintf("/%s/", task.TensorRTLLMShapeFilter.ShapeAlias)) {
					shapeFilteredObjects = append(shapeFilteredObjects, object)
				}
			}
		}
		objects = shapeFilteredObjects

		if len(objects) == 0 {
			return fmt.Errorf("no suitable objects found for shape %s", task.TensorRTLLMShapeFilter.ShapeAlias)
		}
		s.logger.Infof("Found %d objects applicable for shape %s", len(objects), task.TensorRTLLMShapeFilter.ShapeAlias)
	}

	if len(objects) == 0 {
		return fmt.Errorf("no objects found under namespace %s, bucket %s, object prefix %s", uri.Namespace, uri.BucketName, uri.Prefix)
	}

	var objectUris []ociobjectstore.ObjectURI
	for _, obj := range objects {
		if obj.Name == nil {
			continue
		}
		objectUris = append(objectUris, ociobjectstore.ObjectURI{
			Namespace:  uri.Namespace,
			BucketName: uri.BucketName,
			ObjectName: *obj.Name,
			Prefix:     uri.Prefix,
		})
	}

	// Check context before starting bulk download
	select {
	case <-ctx.Done():
		return fmt.Errorf("download cancelled before starting bulk download: %w", ctx.Err())
	default:
	}

	// TODO: BulkDownload doesn't support context cancellation yet
	// This means downloads may continue even after deletion request
	// Future enhancement: modify ociobjectstore to support context
	errs := ociOSDataStore.BulkDownload(objectUris, destPath, s.concurrency,
		ociobjectstore.WithThreads(s.multipartConcurrency),
		ociobjectstore.WithChunkSize(BigFileSizeInMB),
		ociobjectstore.WithSizeThreshold(BigFileSizeInMB),
		ociobjectstore.WithOverrideEnabled(false),
		ociobjectstore.WithStripPrefix(uri.Prefix))
	if errs != nil {
		// Check if we were cancelled during download
		select {
		case <-ctx.Done():
			return fmt.Errorf("download cancelled during bulk download: %w", ctx.Err())
		default:
			return fmt.Errorf("failed to download objects: %v", errs)
		}
	}

	// Perform final verification of all downloaded files
	s.logger.Info("Performing final integrity verification of all downloaded files...")
	verificationStartTime := time.Now()
	verificationErrors := s.verifyDownloadedFiles(ociOSDataStore, objectUris, destPath, task)
	verificationDuration := time.Since(verificationStartTime)

	// Record verification duration
	s.metrics.ObserveVerificationDuration(verificationDuration)

	if len(verificationErrors) > 0 {
		s.logger.Errorf("Final verification failed for %d files", len(verificationErrors))
		errMsgs := make([]string, 0, len(verificationErrors))
		for file, err := range verificationErrors {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", file, err))
			s.logger.Errorf("Verification failed for %s: %v", file, err)
		}
		return fmt.Errorf("integrity verification failed for %d/%d files: %s", len(verificationErrors), len(objects), strings.Join(errMsgs, "; "))
	}

	// Calculate and record total bytes transferred
	var totalBytes int64
	for _, obj := range objects {
		if obj.Size != nil {
			totalBytes += *obj.Size
		}
	}
	s.metrics.RecordBytesTransferred(modelType, namespace, name, totalBytes)

	s.logger.Infof("All files downloaded and verified successfully (%d files, %d bytes, verification took %v)",
		len(objects), totalBytes, verificationDuration.Round(time.Millisecond))
	return nil
}

func (s *Gopher) verifyDownloadedFiles(ociOSDataStore *ociobjectstore.OCIOSDataStore, uris []ociobjectstore.ObjectURI, destPath string, task *GopherTask) map[string]error {
	errors := make(map[string]error)
	for _, obj := range uris {
		relativeName := filepath.Join(destPath, ociobjectstore.TrimObjectPrefix(obj.ObjectName, obj.Prefix))
		// Fallback: if relativeName is empty, use the object name directly
		if relativeName == "" {
			relativeName = obj.ObjectName
		}

		valid, err := ociOSDataStore.IsLocalCopyValid(obj, relativeName)
		if err != nil {
			errors[obj.ObjectName] = err
			continue
		}
		if !valid {
			errors[obj.ObjectName] = fmt.Errorf("MD5 or size mismatch for %s", obj.ObjectName)
		}
	}

	// Record verification result in metrics
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)
	s.metrics.RecordVerification(modelType, namespace, name, len(errors) == 0)

	return errors
}

func (s *Gopher) deleteModel(destPath string, task *GopherTask) error {
	startTime := time.Now()

	err := os.RemoveAll(destPath)

	// Log deletion time regardless of success or failure
	deleteTime := time.Since(startTime)
	s.logger.Infof("Model deletion from %s took %v", destPath, deleteTime.Round(time.Millisecond))

	// Record deletion in metrics if task is provided
	if task != nil {
		modelType, namespace, name := GetModelTypeNamespaceAndName(task)
		// We could add a dedicated deletion metric in the future
		// For now just log with context
		s.logger.Infof("Completed deletion of %s model %s/%s in %v",
			modelType, namespace, name, deleteTime.Round(time.Millisecond))
	}

	return err
}

// isReservingModelArtifact determines whether to preserve the model artifact directory during deletion.
// Behavior:
//   - Returns true if either ClusterBaseModel or BaseModel has the label models.ome/reserve-model-artifact
//     (constants.ReserveModelArtifact) set to "true" (case-insensitive).
//   - Returns false when the task is nil, labels are absent, or the label value is not "true".
//
// Precedence:
//   - If both BaseModel and ClusterBaseModel exist and at least one has the reserve label set to "true",
//     the function returns true (i.e., preserve the artifact).
func (s *Gopher) isReservingModelArtifact(task *GopherTask) bool {
	// Guard against nil task or BaseModel; reserve logic applies only to BaseModel labels
	if task == nil {
		s.logger.Infof("task is nil and will regard no reserved label")
		return false
	}
	// for clusterBaseModel
	if task.ClusterBaseModel != nil && task.ClusterBaseModel.Labels != nil {
		if val, exists := task.ClusterBaseModel.Labels[constants.ReserveModelArtifact]; exists && strings.EqualFold(val, "true") {
			s.logger.Infof("ClusterBaseModel has reserved label")
			return true
		}
	}
	// for baseModel
	if task.BaseModel != nil && task.BaseModel.Labels != nil {
		if val, exists := task.BaseModel.Labels[constants.ReserveModelArtifact]; exists && strings.EqualFold(val, "true") {
			s.logger.Infof("BaseModel has reserved label")
			return true
		}
	}

	s.logger.Infof("task is nil and will regard no reserved label")
	return false
}

// processHuggingFaceModel handles downloading models from Hugging Face Hub.
// It extracts model information from the URI, configures the download with proper authentication,
// performs the download using the hub client, and updates model configuration.
func (s *Gopher) processHuggingFaceModel(ctx context.Context, task *GopherTask, baseModelSpec v1beta1.BaseModelSpec,
	modelInfo, modelType, namespace, name string) error {
	// Parse the Hugging Face URI to get modelID and branch
	hfComponents, err := storage.ParseHuggingFaceStorageURI(*baseModelSpec.Storage.StorageUri)
	if err != nil {
		s.logger.Errorf("Failed to parse Hugging Face URI for model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "invalid_hf_uri")
		s.markModelOnNodeFailed(task)
		return err
	}

	// Create destination path
	destPath := getDestPath(&baseModelSpec, s.modelRootDir)

	// fetch sha value based on model ID from Huggingface model API
	shaStr, isShaAvailable := s.fetchSha(ctx, hfComponents.ModelID, name)
	isReuseEligible, matchedModelTypeAndModeName, parentPath := s.isEligibleForOptimization(ctx, task, baseModelSpec, modelType, namespace, isShaAvailable, shaStr, name)

	var artifact *Artifact
	if isReuseEligible {
		// create symbolic link
		err := utils.CreateSymbolicLink(destPath, parentPath)
		if err != nil {
			s.logger.Errorf("failed to create symbolic link from %s to %s for model %s: %s", destPath, parentPath, name, err)
			return err
		}
		s.logger.Infof("successfully create symbolic link from %s to %s for model: %s", destPath, parentPath, name)
		// add path to childrenPaths in configmap
		err = s.configMapReconciler.updateConfigMapWithUpdatedChildrenPaths(ctx, matchedModelTypeAndModeName, destPath)
		if err != nil {
			s.logger.Errorf("fail to update configmap to add new path to childrenPaths: %s", err)
			return err
		}
		s.logger.Infof("successfully add the new path to childrenPath for model: %s", name)

		childrenPaths := make([]string, 0)
		childrenPaths, _, _, _ = s.parseModelConfigDataEntry(ctx, s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel))
		artifact = s.modelConfigParser.buildArtifactAttribute(shaStr, matchedModelTypeAndModeName, parentPath, childrenPaths)
	} else {
		childrenPaths := make([]string, 0)
		// handle the case when download Policy is updated from ReuseIfExists to AlwaysDownload
		isSymlink, symbolicLinkErr := utils.IsSymbolicLink(destPath)
		s.logger.Infof("directory %s is symbolic link :%v and error is %v", destPath, isSymlink, symbolicLinkErr)
		if symbolicLinkErr != nil {
			s.logger.Warnf("failed to determine if %s is a symbolic link: %v", destPath, symbolicLinkErr)
		}
		currentModelTypeAndNodeName := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
		currentChildren, parentName, _, parseErr := s.parseModelConfigDataEntry(ctx, currentModelTypeAndNodeName)
		hasChildren := hasChildrenPaths(childrenPaths, parseErr)
		if isSymlink {
			if removalErr := utils.RemoveSymbolicLink(destPath); removalErr != nil {
				s.logger.Errorf("failed to remove existing symbolic link at %s: %v", destPath, removalErr)
			}
			s.logger.Infof("removed existing symbolic link at %s", destPath)
			if parentName != "" {
				s.removeChildPathFromParentConfigMapIfNecessary(ctx, hasChildren, parentName, currentModelTypeAndNodeName, destPath)
			}
		}
		childrenPaths = currentChildren

		// Get Hugging Face token from storage key or parameters
		hfToken := s.getHuggingFaceToken(task, baseModelSpec, modelInfo)

		s.logger.Infof("Downloading HuggingFace model %s (revision: %s) to %s",
			hfComponents.ModelID, hfComponents.Branch, destPath)

		// Init xet HF download config
		config := s.xetConfig.ToDownloadConfig()
		config.LocalDir = destPath
		config.RepoID = hfComponents.ModelID

		// Set revision if specified
		if hfComponents.Branch != "" {
			config.Revision = hfComponents.Branch
		}

		// If we have a token, pass it as a download option
		if hfToken != "" {
			s.logger.Infof("Using authentication token for HuggingFace model %s", modelInfo)
			config.Token = hfToken
		}

		// Create progress handler for tracking download progress
		// Uses single worker with atomic pointer to avoid fire-and-forget goroutines
		// that can cause race conditions and "context canceled" errors
		var lastBytes atomic.Uint64
		var lastTimeNano atomic.Int64
		lastTimeNano.Store(time.Now().UnixNano())
		progressThrottle := 30 * time.Second // Update ConfigMap every 30 seconds
		const progressFlushTimeout = 5 * time.Second

		// Atomic pointer to store latest progress - lock-free, instant updates
		var latestProgress atomic.Pointer[DownloadProgress]
		stopWorker := make(chan struct{})
		workerDone := make(chan struct{})

		// flushProgress is a helper to flush progress to ConfigMap
		flushProgress := func(p *DownloadProgress, timeout time.Duration) {
			if p == nil {
				return
			}
			progressOp := &ConfigMapProgressOp{
				Progress:         p,
				BaseModel:        task.BaseModel,
				ClusterBaseModel: task.ClusterBaseModel,
			}
			flushCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := s.configMapReconciler.ReconcileModelProgress(flushCtx, progressOp); err != nil {
				s.logger.Warnf("Failed to update download progress for %s: %v", modelInfo, err)
			}
		}

		// Single background worker - updates ConfigMap periodically
		// This eliminates fire-and-forget goroutines that caused race conditions
		go func() {
			defer close(workerDone)
			ticker := time.NewTicker(progressThrottle)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					flushProgress(latestProgress.Swap(nil), progressThrottle)
				case <-stopWorker:
					// Final flush before exit
					flushProgress(latestProgress.Swap(nil), progressFlushTimeout)
					return
				}
			}
		}()

		// Ensure worker stops before we set Ready status
		// This guarantees no race condition between progress updates and status updates
		defer func() {
			close(stopWorker) // Signal worker to stop
			<-workerDone      // Wait for worker to finish
			s.logger.Debugf("Progress worker stopped for %s", modelInfo)
		}()

		progressHandler := func(update xet.ProgressUpdate) {
			now := time.Now()

			// Calculate speed (bytes per second) using atomic variables for thread safety
			prevBytes := lastBytes.Load()
			prevTimeNano := lastTimeNano.Load()
			var speedBytesPerSec float64
			elapsed := float64(now.UnixNano()-prevTimeNano) / float64(time.Second)
			if elapsed > 0 && update.CompletedBytes > prevBytes {
				speedBytesPerSec = float64(update.CompletedBytes-prevBytes) / elapsed
			}
			lastBytes.Store(update.CompletedBytes)
			lastTimeNano.Store(now.UnixNano())

			// Store latest progress atomically (non-blocking, lock-free)
			// Worker will periodically flush this to ConfigMap
			latestProgress.Store(&DownloadProgress{
				Phase:            update.Phase.String(),
				TotalBytes:       update.TotalBytes,
				CompletedBytes:   update.CompletedBytes,
				TotalFiles:       update.TotalFiles,
				CompletedFiles:   update.CompletedFiles,
				SpeedBytesPerSec: speedBytesPerSec,
				LastUpdated:      now.Format(time.RFC3339),
			})
		}

		// Perform snapshot download with progress tracking
		// Note: Progress is cleared atomically with status update in ReconcileModelStatus
		// when status becomes Ready/Failed, ensuring the controller sees the final progress
		downloadPath, err := xet.SnapshotDownloadWithProgress(ctx, config, progressHandler, progressThrottle)

		if err != nil {
			// Check error type for better handling
			if strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "rate limit") {
				s.logger.Warnf("Rate limited while downloading HuggingFace model %s: %v", modelInfo, err)
				s.metrics.RecordRateLimit(modelType, namespace, name, 30*time.Second) // Estimate
				s.metrics.RecordFailedDownload(modelType, namespace, name, "rate_limit_error")
			} else {
				s.logger.Errorf("Failed to download HuggingFace model %s: %v", modelInfo, err)
				s.metrics.RecordFailedDownload(modelType, namespace, name, "hf_download_error")
			}

			s.markModelOnNodeFailed(task)
			return err
		}

		s.logger.Infof("Successfully downloaded HuggingFace model %s to %s",
			modelInfo, downloadPath)
		artifact = s.modelConfigParser.buildArtifactAttribute(shaStr, s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel), destPath, childrenPaths)
	}

	// Parse model config and update ConfigMap
	var baseModel *v1beta1.BaseModel
	var clusterBaseModel *v1beta1.ClusterBaseModel

	if task.BaseModel != nil {
		baseModel = task.BaseModel
		s.logger.Debugf("Using BaseModel %s/%s for config parsing", baseModel.Namespace, baseModel.Name)
	} else if task.ClusterBaseModel != nil {
		clusterBaseModel = task.ClusterBaseModel
		s.logger.Debugf("Using ClusterBaseModel %s for config parsing", clusterBaseModel.Name)
	}

	if err := s.safeParseAndUpdateModelConfig(destPath, baseModel, clusterBaseModel, artifact); err != nil {
		s.logger.Errorf("Failed to parse and update model config: %v", err)
	}
	return nil
}

/*
handelReuseArtifactIfNecessary determines whether to reuse an existing model artifact
based on the BaseModel's download policy and artifacts previously recorded in the
node-scoped ConfigMap.

any error thrown in the process of searching for matched parent model, will be ignored, the process will proceed
to download artifact. Will let model cr reconciliation process handle searching for matched model to avoid impact
model creation process

Returns:
  - matchedKey: the matched ConfigMap data key
  - matchedParentPath: the value of config.artifact.parentPath extracted from the matched entry
*/
func (s *Gopher) handelReuseArtifactIfNecessary(ctx context.Context, baseModelSpec v1beta1.BaseModelSpec,
	modelType string, modelName string, namespace string, shaStr string, currentModelTypeAndNodeName string) (string, string) {
	// check whether identical artifact is already existing if model specified with ReuseIfExists
	if baseModelSpec.Storage.DownloadPolicy != nil && *baseModelSpec.Storage.DownloadPolicy == v1beta1.ReuseIfExists {
		var matchedModelTypeAndModelName string
		var matchedParentPath string
		var err error
		// prioritize searching parent path in ClusterBaseModel
		// with hoping different basemodel in different namespaces could be linked to the same parent path to lower the chance of downloading artifact
		if strings.EqualFold(modelType, constants.ClusterBaseModel) || strings.EqualFold(modelType, constants.BaseModel) {
			matchedModelTypeAndModelName, matchedParentPath, err = s.configMapReconciler.getModelDataByArtifactSha(ctx, shaStr, constants.LowerCaseClusterBaseModel, currentModelTypeAndNodeName)
			if err != nil {
				s.logger.Warnf("get error when finding matched model in configmap for model : %s: %s", modelName, err)
			}
		}
		if strings.EqualFold(modelType, constants.BaseModel) && matchedModelTypeAndModelName == "" {
			// build namespaced model type
			namespacedModelType := fmt.Sprintf("%s.%s", namespace, constants.LowerCaseBaseModel)
			matchedModelTypeAndModelName, matchedParentPath, err = s.configMapReconciler.getModelDataByArtifactSha(ctx, shaStr, namespacedModelType, currentModelTypeAndNodeName)
			if err != nil {
				s.logger.Warnf("get error when finding matched model in configmap for model : %s: %s", modelName, err)
			}
		}
		return matchedModelTypeAndModelName, matchedParentPath
	}
	return "", ""
}

// processLocalStorageModel handles local filesystem models.
// For local storage:
//   - Download: validates that the path exists and parses model configuration (no actual download)
//   - Delete: no-op for files (they are preserved), only updates node labels and ConfigMap
//
// This allows users to reference pre-existing models without copying or removing them.
func (s *Gopher) processLocalStorageModel(ctx context.Context, task *GopherTask, baseModelSpec v1beta1.BaseModelSpec,
	modelInfo, modelType, namespace, name string) error {
	// Parse the local storage URI to get the path
	localComponents, err := storage.ParseLocalStorageURI(*baseModelSpec.Storage.StorageUri)
	if err != nil {
		s.logger.Errorf("Failed to parse local storage URI for model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "invalid_local_uri")
		s.markModelOnNodeFailed(task)
		return err
	}

	// Determine the actual model path
	// If Path is specified in the CRD, use it; otherwise use the path from the URI
	var modelPath string
	if baseModelSpec.Storage.Path != nil && *baseModelSpec.Storage.Path != "" {
		// Use the explicit Path from the CRD
		modelPath = *baseModelSpec.Storage.Path
		s.logger.Infof("Using explicit path from CRD for local model %s: %s", modelInfo, modelPath)
	} else {
		// Use the path from the local:// URI
		modelPath = localComponents.Path
		s.logger.Infof("Using path from URI for local model %s: %s", modelInfo, modelPath)
	}

	// Check if the path exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		s.logger.Errorf("Local model path does not exist for model %s: %s", modelInfo, modelPath)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "local_path_not_found")
		s.markModelOnNodeFailed(task)
		return fmt.Errorf("local model path does not exist: %s", modelPath)
	}

	s.logger.Infof("Local model path exists for model %s: %s", modelInfo, modelPath)

	// Parse model config and update ConfigMap
	var baseModel *v1beta1.BaseModel
	var clusterBaseModel *v1beta1.ClusterBaseModel

	if task.BaseModel != nil {
		baseModel = task.BaseModel
		s.logger.Debugf("Using BaseModel %s/%s for config parsing", baseModel.Namespace, baseModel.Name)
	} else if task.ClusterBaseModel != nil {
		clusterBaseModel = task.ClusterBaseModel
		s.logger.Debugf("Using ClusterBaseModel %s for config parsing", clusterBaseModel.Name)
	}

	if err := s.safeParseAndUpdateModelConfig(modelPath, baseModel, clusterBaseModel, nil); err != nil {
		s.logger.Errorf("Failed to parse and update model config for local model: %v", err)
		// This is not necessarily a failure - the model might still be usable
	}

	s.logger.Infof("Successfully processed local model %s at path %s", modelInfo, modelPath)
	return nil
}

// for unit test
var fetchAttributeFromHfModelMetaData = FetchAttributeFromHfModelMetaData

// fetchSha retrieves the git commit SHA associated with a Hugging Face model ID.
// It queries the Hugging Face model metadata API for the "sha" attribute and returns:
// - the SHA string if available, along with true
// - an empty string and false if the API call fails, or the attribute is missing/non-string/empty.
func (s *Gopher) fetchSha(ctx context.Context, modelId string, modelName string) (string, bool) {
	var isShaAvailable = true
	sha, err := fetchAttributeFromHfModelMetaData(ctx, modelId, Sha)
	if err != nil {
		s.logger.Errorf("Failed to retrieve sha from Hugging Face endpoint for model %s: %s", modelName, err)
		isShaAvailable = false
	}
	shaStr, ok := sha.(string)
	if !ok || shaStr == "" {
		s.logger.Warnf("Could not get a valid sha string for model %s, proceeding with download without artifact reuse.", modelName)
		isShaAvailable = false
	}
	if isShaAvailable {
		s.logger.Infof("fetched sha of model %s is %s", modelName, shaStr)
	}
	return shaStr, isShaAvailable
}

/*
isEligibleForOptimization determines whether a Hugging Face model can reuse an existing artifact.

Returns:
  - eligible: true if reuse is possible; false otherwise
  - matchedModelTypeAndModeName: ConfigMap key of the matched entry (empty if no match)
  - parentPath: artifact parent path from the matched entry (empty if no match)
*/
func (s *Gopher) isEligibleForOptimization(ctx context.Context, task *GopherTask, baseModelSpec v1beta1.BaseModelSpec,
	modelType string, namespace string, isShaAvailable bool, shaStr, modelName string) (bool, string, string) {
	if !isShaAvailable {
		return false, "", ""
	}

	currentModelTypeAndNodeName := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
	matchedModelTypeAndModeName, parentPath := s.handelReuseArtifactIfNecessary(ctx, baseModelSpec, modelType, modelName, namespace, shaStr, currentModelTypeAndNodeName)
	isEligible := matchedModelTypeAndModeName != ""
	s.logger.Infof("found matched matchedModelTypeAndModeName %s for model %s, parentPath is %s, isEligible %t", matchedModelTypeAndModeName, modelName, parentPath, isEligible)
	return isEligible, matchedModelTypeAndModeName, parentPath
}

/*
isSkippingArtifactDeletion decides whether to preserve a model artifact directory during deletion.

Consider 3 aspects:
1) If the path is still referenced by other BaseModel/ClusterBaseModel objects (excluding the current task's model),
2) If the model resource (BaseModel or ClusterBaseModel) carries the reserve label (models.ome/reserve-model-artifact=true),
3) If it needs to consider children path, inspect the node-scoped ConfigMap entry for this model for existence of children paths

Parameters:
- ctx: context for Kubernetes API operations.
- task: the model task containing BaseModel or ClusterBaseModel; used for reference exclusion and reserve label checks.
- destPath: the absolute filesystem path of the model artifact.
- needsConsiderChildrenPath: whether to consider childrenPaths relationship before deletion.

Returns:
- bool: true to skip deletion; false to proceed with deletion.
- bool: true to remove parent artifact directory
- string: parent model name
- string: parent model directory
*/
func (s *Gopher) isSkippingArtifactDeletion(ctx context.Context, task *GopherTask, destPath string, needsConsiderChildrenPath bool) (bool, bool, string, string) {
	// Double-check if the path is still referenced by other models
	isReferenced, err := s.isPathReferencedByOtherModels(destPath, task.BaseModel, task.ClusterBaseModel)
	if err != nil {
		// Cannot determine if the path is referenced; skip deletion to be safe
		s.logger.Errorf("Failed to check if path %s is referenced by other models, skip the path deletion: %v", destPath, err)
		return true, false, "", ""
	}
	if isReferenced {
		return true, false, "", ""
	}

	// check whether the model CR has reserved label
	hasReserveLabel := s.isReservingModelArtifact(task)
	if hasReserveLabel {
		return true, false, "", ""
	}
	if needsConsiderChildrenPath {
		modelTypeAndModelName := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
		childrenPaths, parentName, parentDir, parseErr := s.parseModelConfigDataEntry(ctx, modelTypeAndModelName)
		hasChildren := hasChildrenPaths(childrenPaths, parseErr)
		s.removeChildPathFromParentConfigMapIfNecessary(ctx, hasChildren, parentName, modelTypeAndModelName, destPath)
		isRemoveParent := s.isRemoveParentArtifactDirectory(ctx, hasChildren, parentName, parentDir)
		return hasChildren, isRemoveParent, parentName, parentDir
	} else {
		return hasReserveLabel, false, "", ""
	}
}

// parseModelConfigDataEntry checks if the given model type and model name has children paths.
// It retrieves the existing config map, determines the parent path and children paths,
// If an error occurs during the process, it logs the error and returns true.
func (s *Gopher) parseModelConfigDataEntry(ctx context.Context, modelTypeAndModelName string) ([]string, string, string, error) {
	// regard it is parent, check config.artifact.childrenPaths. If there are no children paths, the artifact could be deleted
	// if it does not have children, regard it is child, search for its parent. if the parent is located, remove the path from parent entry
	exists, dataEntry, err := s.configMapReconciler.getDataEntryBasedOnModelKey(ctx, modelTypeAndModelName)
	if err != nil {
		existenceErr := fmt.Errorf("cannot retrieve node configmap and cannot determine whether it has childrenPaths and will regard it has: %v", err)
		s.logger.Errorf(existenceErr.Error())
		return make([]string, 0), "", "", existenceErr
	}
	if !exists {
		nonExistence := fmt.Errorf("cannot determine whether %s has childrenPaths and will regard it has because the corresponding entry does not exist in node configmap", modelTypeAndModelName)
		s.logger.Errorf(nonExistence.Error())
		return make([]string, 0), "", "", nonExistence
	}
	parentPath, childrenPaths, err := s.configMapReconciler.getParentPathAndChildrenPaths(modelTypeAndModelName, dataEntry)
	if err != nil {
		parseErr := fmt.Errorf("cannot determine whether it has childrenPaths and will regard it has because %v", err)
		s.logger.Errorf(parseErr.Error())
		return make([]string, 0), "", "", parseErr

	}
	parentName, parentDir := parseParent(parentPath)
	return childrenPaths, parentName, parentDir, nil
}

func hasChildrenPaths(childrenPaths []string, parseError error) bool {
	if parseError != nil {
		return true
	}
	return len(childrenPaths) != 0
}

/*
removeChildPathFromParentConfigMapIfNecessary removes the child's destPath from its parent's
config.artifact.childrenPaths entry in the node-scoped ConfigMap when meets condition.

Parameters:
  - ctx: context for Kubernetes API operations.
  - hasChildren: whether the deleting model still has children paths in the ConfigMap.
  - parentName: ConfigMap key for the parent model entry.
  - modelTypeAndModelName: ConfigMap key for the current model entry (used for self-parent checks).
  - destPath: absolute filesystem path of the current model artifact to remove from the parent's childrenPaths.
*/
func (s *Gopher) removeChildPathFromParentConfigMapIfNecessary(ctx context.Context, hasChildren bool, parentName string, modelTypeAndModelName string, destPath string) {
	// if it does not have child, and its parent is not itself, need to remove the path from parent entry
	if !hasChildren && !strings.EqualFold(parentName, modelTypeAndModelName) {
		err := s.configMapReconciler.updateConfigMapWithRemovedChildPath(ctx, parentName, destPath)
		if err != nil {
			s.logger.Errorf("failed to remove model %s child path %s from parentName %s", modelTypeAndModelName, destPath, parentName)
		}
	}
}

// isRemoveParentArtifactDirectory - return whether the parent artifact directory is eligible to be deleted
// lenient way to determine whether the parent model cr is deleted or not. if more strictly, need to check the model cr existence
// after checking the existence of parent entry in the configmap
// However, due to the model key of configmap could be truncated, there is no way to retrieve the exact original parent model CR name and namespace
// based on the current design
func (s *Gopher) isRemoveParentArtifactDirectory(ctx context.Context, hasChildren bool, parentName string, parentDir string) bool {
	// If there are still children, never remove the parent artifact directory.
	if hasChildren {
		return false
	}

	// If the parent entry still exists in the node ConfigMap, don't remove.
	exists, _, err := s.configMapReconciler.getDataEntryBasedOnModelKey(ctx, parentName)
	if err != nil && strings.Contains(err.Error(), "cannot retrieve node configmap") {
		s.logger.Infof("cannot retrieve node configmap and cannot determine parent entry existence, will not remove artifact")
		return false
	}
	s.logger.Infof("parent entry %s:%s exists on node configmap: %v", parentName, parentDir, exists)
	return !exists
}
