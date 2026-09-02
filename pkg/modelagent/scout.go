package modelagent

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/kmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omev1beta1informers "sigs.k8s.io/ome/pkg/client/informers/externalversions"
	omev1beta1 "sigs.k8s.io/ome/pkg/client/informers/externalversions/ome/v1beta1"
	omev1beta1lister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

type Scout struct {
	ctx                    context.Context
	baseModelLister        omev1beta1lister.BaseModelLister
	baseModelSynced        cache.InformerSynced
	clusterBaseModelLister omev1beta1lister.ClusterBaseModelLister
	clusterBaseModelSynced cache.InformerSynced
	inferenceServiceLister omev1beta1lister.InferenceServiceLister
	inferenceServiceSynced cache.InformerSynced
	informerFactory        omev1beta1informers.SharedInformerFactory
	gopherChan             chan<- *GopherTask
	nodeName               string
	nodeInfo               *v1.Node
	nodeShapeAlias         string
	kubeClient             *kubernetes.Clientset
	logger                 *zap.SugaredLogger
	demandPriorityEnabled  bool
	demandMu               sync.RWMutex
	endpointDemands        map[string]string
	demandCounts           map[string]int
}

type TensorRTLLMShapeFilter struct {
	IsTensorrtLLMModel bool
	ShapeAlias         string
	ModelType          string
}

type downloadOverrideChangeCandidate struct {
	name     string
	old, new interface{}
}

type downloadOverrideInputs struct {
	Storage              *v1beta1.StorageSpec
	IsTensorRTLLMModel   bool
	TensorRTLLMModelType string
}

type modelDemandReference struct {
	kind      string
	namespace string
	name      string
}

func NewScout(ctx context.Context, nodeName string,
	baseModelInformer omev1beta1.BaseModelInformer,
	clusterBaseModelInformer omev1beta1.ClusterBaseModelInformer,
	informerFactory omev1beta1informers.SharedInformerFactory,
	gopherChan chan<- *GopherTask,
	kubeClient *kubernetes.Clientset,
	logger *zap.SugaredLogger) (*Scout, error) {
	return NewScoutWithDemand(ctx, nodeName, baseModelInformer, clusterBaseModelInformer,
		nil, false, informerFactory, gopherChan, kubeClient, logger)
}

func NewScoutWithDemand(ctx context.Context, nodeName string,
	baseModelInformer omev1beta1.BaseModelInformer,
	clusterBaseModelInformer omev1beta1.ClusterBaseModelInformer,
	inferenceServiceInformer omev1beta1.InferenceServiceInformer,
	demandPriorityEnabled bool,
	informerFactory omev1beta1informers.SharedInformerFactory,
	gopherChan chan<- *GopherTask,
	kubeClient *kubernetes.Clientset,
	logger *zap.SugaredLogger) (*Scout, error) {

	logger.Infof("Initializing Scout for node: %s", nodeName)

	// Fetch the complete node info
	nodeInfo, err := kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get node info for node %s: %w", nodeName, err)
	}
	// Try the newer label first, then fallback to the deprecated beta label
	instanceType, ok := nodeInfo.Labels[constants.NodeInstanceShapeLabel]
	if !ok {
		instanceType = nodeInfo.Labels[constants.DeprecatedNodeInstanceShapeLabel]
	}
	nodeShapeAlias, err := utils.GetInstanceTypeShortName(instanceType)
	if err != nil {
		// A shape alias is only needed for shape-specific TensorRT-LLM artifacts.
		// CPU nodes do not have an entry in the GPU shape map, but can still
		// download normal models.
		logger.Infof("Node %s has no GPU shape alias; continuing without TensorRT-LLM shape filtering: %s", nodeName, err)
		nodeShapeAlias = ""
	}

	scout := &Scout{
		ctx:                    ctx,
		nodeShapeAlias:         nodeShapeAlias,
		nodeInfo:               nodeInfo,
		baseModelLister:        baseModelInformer.Lister(),
		baseModelSynced:        baseModelInformer.Informer().HasSynced,
		clusterBaseModelLister: clusterBaseModelInformer.Lister(),
		clusterBaseModelSynced: clusterBaseModelInformer.Informer().HasSynced,
		informerFactory:        informerFactory,
		gopherChan:             gopherChan,
		nodeName:               nodeName,
		kubeClient:             kubeClient,
		logger:                 logger,
		demandPriorityEnabled:  demandPriorityEnabled && inferenceServiceInformer != nil,
		endpointDemands:        make(map[string]string),
		demandCounts:           make(map[string]int),
	}
	if scout.demandPriorityEnabled {
		scout.inferenceServiceLister = inferenceServiceInformer.Lister()
		scout.inferenceServiceSynced = inferenceServiceInformer.Informer().HasSynced
	}

	logger.Info("Setting up informer error handlers")
	informers := map[string]cache.SharedInformer{
		"baseModelInformer":        baseModelInformer.Informer(),
		"clusterBaseModelInformer": clusterBaseModelInformer.Informer(),
	}
	if scout.demandPriorityEnabled {
		informers["inferenceServiceInformer"] = inferenceServiceInformer.Informer()
	}

	for name, informer := range informers {
		err := informer.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
			// Pipe to the default handler first, which just logs the error
			cache.DefaultWatchErrorHandler(ctx, r, err)

			// Only fatal on authentication/authorization errors
			if errors.IsUnauthorized(err) || errors.IsForbidden(err) {
				logger.Fatalf("Unable to sync cache for informer %s: %s. Requesting scout to exit.", name, err.Error())
			}

			// Log all watch errors for better diagnostics
			logger.Warnf("Watch error in informer %s: %v (Type: %T)", name, err, err)
		})

		if err != nil {
			return nil, fmt.Errorf("unable to set error handler for informer %s: %s", name, err)
		}
	}

	logger.Info("Setting up event handlers")

	if _, err := baseModelInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    scout.downloadBaseModel,
		UpdateFunc: scout.updateBaseModel,
		DeleteFunc: scout.deleteBaseModel,
	}); err != nil {
		return nil, err
	}

	if _, err := clusterBaseModelInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    scout.downloadClusterBaseModel,
		UpdateFunc: scout.updateClusterBaseModel,
		DeleteFunc: scout.deleteClusterBaseModel,
	}); err != nil {
		return nil, err
	}

	if scout.demandPriorityEnabled {
		if _, err := inferenceServiceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    scout.addOrUpdateInferenceService,
			UpdateFunc: func(_, newObj interface{}) { scout.addOrUpdateInferenceService(newObj) },
			DeleteFunc: scout.deleteInferenceService,
		}); err != nil {
			return nil, err
		}
	}

	return scout, nil
}

func (w *Scout) Run(stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()

	w.logger.Info("Starting scout")
	w.logger.Info("Starting informer cache")
	go w.informerFactory.Start(stopCh)

	w.logger.Info("Waiting for informer caches to sync")
	synced := []cache.InformerSynced{
		w.clusterBaseModelSynced,
		w.baseModelSynced,
	}
	if w.demandPriorityEnabled {
		synced = append(synced, w.inferenceServiceSynced)
	}

	// Add retry logic with exponential backoff for cache sync
	// This handles transient API server connectivity issues during node startup
	maxRetries := 5
	retryDelay := 2 * time.Second
	maxRetryDelay := 15 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if shutdown was requested before attempting sync
		select {
		case <-stopCh:
			return fmt.Errorf("shutdown requested before cache sync attempt (pod is being terminated)")
		default:
		}

		w.logger.Infof("Attempting to sync caches (attempt %d/%d)", attempt, maxRetries)

		// Use a separate channel for this sync attempt to distinguish between
		// timeout and shutdown signal
		syncStopCh := make(chan struct{})
		syncResult := make(chan bool, 1)

		go func() {
			ok := cache.WaitForCacheSync(syncStopCh, synced...)
			syncResult <- ok
		}()

		// Wait for sync result with timeout
		syncTimeout := 30 * time.Second
		select {
		case ok := <-syncResult:
			if ok {
				w.logger.Info("Successfully synced informer caches")
				// Cache sync succeeded, proceed with normal operation
				goto syncComplete
			}
			// Cache sync returned false, log and retry
			w.logger.Warnf("Cache sync returned false (attempt %d/%d) - informers may not be synced yet", attempt, maxRetries)
		case <-time.After(syncTimeout):
			w.logger.Warnf("Cache sync timeout after %v (attempt %d/%d)", syncTimeout, attempt, maxRetries)
			close(syncStopCh) // Stop the sync goroutine
		case <-stopCh:
			close(syncStopCh) // Stop the sync goroutine
			return fmt.Errorf("shutdown requested during cache sync (pod is being terminated)")
		}

		if attempt == maxRetries {
			// Log detailed information about what didn't sync
			w.logger.Errorf("Failed to sync caches after %d attempts", maxRetries)
			w.logger.Errorf("ClusterBaseModel synced: %v", w.clusterBaseModelSynced())
			w.logger.Errorf("BaseModel synced: %v", w.baseModelSynced())
			return fmt.Errorf("failed to wait for caches to sync after %d attempts", maxRetries)
		}

		// Calculate next retry delay with exponential backoff and cap
		nextDelay := retryDelay
		if nextDelay > maxRetryDelay {
			nextDelay = maxRetryDelay
		}

		// Sleep before retry, but allow immediate shutdown if requested
		// We use a timer instead of time.After to avoid blocking shutdown for too long
		timer := time.NewTimer(nextDelay)
		select {
		case <-timer.C:
			retryDelay *= 2 // Exponential backoff for next iteration
		case <-stopCh:
			timer.Stop()
			return fmt.Errorf("shutdown requested while waiting to retry cache sync (pod is being terminated)")
		}
	}
syncComplete:

	// After caches are synced, check for any pending deletions (resources with DeletionTimestamp)
	// This ensures we catch any deletion requests that occurred while the agent was down
	w.reconcilePendingDeletions()
	if w.demandPriorityEnabled {
		w.refreshAllEndpointDemands()
	}

	<-stopCh
	close(w.gopherChan)
	w.logger.Info("Shutting down scout")

	return nil
}

func (w *Scout) downloadBaseModel(obj interface{}) {
	baseModel, ok := obj.(*v1beta1.BaseModel)
	if !ok {
		w.logger.Errorf("Failed to convert %v to BaseModel", obj)
		return
	}

	w.logger.Infof("Processing BaseModel: %s in namespace %s", baseModel.Name, baseModel.Namespace)
	if !baseModel.ObjectMeta.DeletionTimestamp.IsZero() {
		w.logger.Infof("ignoring because of deleting of BaseModel '%s'", baseModel.Name)
		return
	}

	if w.shouldDownloadModel(baseModel.Spec.Storage) {
		// Refresh the node info
		var err error
		w.nodeInfo, err = w.kubeClient.CoreV1().Nodes().Get(w.ctx, w.nodeName, metav1.GetOptions{})
		if err != nil {
			w.logger.Errorf("Error getting the node info: %s, skipping download", err.Error())
			return
		}

		w.logger.Infof("Downloading BaseModel: %s in namespace %s", baseModel.Name, baseModel.Namespace)

		IsTensorrtLLMModel := baseModel.Spec.ModelFormat.Name == constants.TensorRTLLM

		modelType := string(constants.ServingBaseModel)
		if modelTypeFromMetadata, ok := baseModel.Spec.AdditionalMetadata["type"]; ok {
			modelType = modelTypeFromMetadata
		}

		gopherTask := &GopherTask{
			TaskType:  Download,
			BaseModel: baseModel,
			ServingDemand: w.isModelDemanded(
				modelDemandKey(constants.BaseModel, baseModel.Namespace, baseModel.Name)),
			TensorRTLLMShapeFilter: &TensorRTLLMShapeFilter{
				IsTensorrtLLMModel: IsTensorrtLLMModel,
				ShapeAlias:         w.nodeShapeAlias,
				ModelType:          modelType,
			},
		}

		w.gopherChan <- gopherTask
	}
}

func (w *Scout) downloadClusterBaseModel(obj interface{}) {
	clusterBaseModel, ok := obj.(*v1beta1.ClusterBaseModel)
	if !ok {
		w.logger.Errorf("Failed to convert %v to clusterBaseModel", obj)
		return
	}

	w.logger.Infof("Processing ClusterBaseModel: %s", clusterBaseModel.Name)
	if !clusterBaseModel.ObjectMeta.DeletionTimestamp.IsZero() {
		w.logger.Infof("ignoring because of deleting ClusterBaseModel '%s'", clusterBaseModel.Name)
		return
	}

	if w.shouldDownloadModel(clusterBaseModel.Spec.Storage) {
		// Refresh the node info
		var err error
		w.nodeInfo, err = w.kubeClient.CoreV1().Nodes().Get(w.ctx, w.nodeName, metav1.GetOptions{})
		if err != nil {
			w.logger.Errorf("Error getting the node info: %s, skipping download", err.Error())
			return
		}

		w.logger.Infof("Downloading ClusterBaseModel: %s", clusterBaseModel.Name)

		IsTensorrtLLMModel := clusterBaseModel.Spec.ModelFormat.Name == constants.TensorRTLLM

		modelType := string(constants.ServingBaseModel)
		if modelTypeFromMetadata, ok := clusterBaseModel.Spec.AdditionalMetadata["type"]; ok {
			modelType = modelTypeFromMetadata
		}

		gopherTask := &GopherTask{
			TaskType:         Download,
			ClusterBaseModel: clusterBaseModel,
			ServingDemand: w.isModelDemanded(
				modelDemandKey(constants.ClusterBaseModel, "", clusterBaseModel.Name)),
			TensorRTLLMShapeFilter: &TensorRTLLMShapeFilter{
				IsTensorrtLLMModel: IsTensorrtLLMModel,
				ShapeAlias:         w.nodeShapeAlias,
				ModelType:          modelType,
			},
		}

		w.gopherChan <- gopherTask
	}
}

func (w *Scout) updateBaseModel(old, new interface{}) {
	oldBaseModel, ok := old.(*v1beta1.BaseModel)
	if !ok {
		w.logger.Errorf("Failed to convert %v to ClusterBaseModel", old)
		return
	}
	newBaseModel := new.(*v1beta1.BaseModel)

	if w.shouldDownloadModel(oldBaseModel.Spec.Storage) &&
		!w.shouldDownloadModel(newBaseModel.Spec.Storage) {
		// shape config changed, delete it from the current node
		w.logger.Infof("Target shapes excluded BaseModel update: %s in namespace %s, deleting", newBaseModel.GetName(), newBaseModel.GetNamespace())
		w.deleteBaseModel(new)
		return
	}

	if !newBaseModel.ObjectMeta.DeletionTimestamp.IsZero() {
		w.logger.Infof("Resource has deletion timestamp: BaseModel '%s', processing delete", newBaseModel.Name)
		w.deleteBaseModel(newBaseModel)
		return
	}

	policyChanged := w.isToDownloadOverrideDueToDownloadPolicyBasedOnBM(oldBaseModel, newBaseModel)

	// Exclude DownloadPolicy from Spec diff — policy changes are detected separately above.
	ignoreDownloadPolicy := cmpopts.IgnoreFields(v1beta1.StorageSpec{}, "DownloadPolicy")

	hasChanges, err := hasDownloadOverrideChanges([]downloadOverrideChangeCandidate{
		{"Labels", oldBaseModel.Labels, newBaseModel.Labels},
		{"Annotations", oldBaseModel.Annotations, newBaseModel.Annotations},
		{"DownloadOverrideInputs", downloadOverrideInputsFromSpec(oldBaseModel.Spec), downloadOverrideInputsFromSpec(newBaseModel.Spec)},
	}, ignoreDownloadPolicy)
	if err != nil {
		w.logger.Errorf("Failed to diff BaseModel %s in namespace %s: %v",
			newBaseModel.Name, newBaseModel.Namespace, err)
		return
	}

	if (policyChanged || hasChanges) && w.shouldDownloadModel(newBaseModel.Spec.Storage) {
		w.logger.Infof("BaseModel %s needs refresh in namespace %s", newBaseModel.GetName(), newBaseModel.GetNamespace())
		w.generateDownloadOverrideTaskBasedOnBaseModel(newBaseModel)
	}
}

func (w *Scout) updateClusterBaseModel(old, new interface{}) {
	oldClusterBaseModel, ok := old.(*v1beta1.ClusterBaseModel)
	if !ok {
		w.logger.Errorf("Failed to convert %v to ClusterBaseModel", old)
		return
	}

	newClusterBaseModel, ok := new.(*v1beta1.ClusterBaseModel)
	if !ok {
		w.logger.Errorf("Failed to convert %v to ClusterBaseModel", new)
		return
	}

	if w.shouldDownloadModel(oldClusterBaseModel.Spec.Storage) &&
		!w.shouldDownloadModel(newClusterBaseModel.Spec.Storage) {
		// shape config changed, delete it from the current node
		w.logger.Infof("Target shapes excluded ClusterBaseModel %s, deleting", newClusterBaseModel.GetName())
		w.deleteClusterBaseModel(new)
		return
	}

	if !newClusterBaseModel.ObjectMeta.DeletionTimestamp.IsZero() {
		w.logger.Infof("Resource has deletion timestamp: ClusterBaseModel '%s', processing delete", newClusterBaseModel.Name)
		w.deleteClusterBaseModel(newClusterBaseModel)
		return
	}

	policyChanged := w.isToDownloadOverrideDueToDownloadPolicyBasedOnCBM(oldClusterBaseModel, newClusterBaseModel)

	// Exclude DownloadPolicy from Spec diff — policy changes are detected separately above.
	ignoreDownloadPolicy := cmpopts.IgnoreFields(v1beta1.StorageSpec{}, "DownloadPolicy")

	hasChanges, err := hasDownloadOverrideChanges([]downloadOverrideChangeCandidate{
		{"Labels", oldClusterBaseModel.Labels, newClusterBaseModel.Labels},
		{"Annotations", oldClusterBaseModel.Annotations, newClusterBaseModel.Annotations},
		{"DownloadOverrideInputs", downloadOverrideInputsFromSpec(oldClusterBaseModel.Spec), downloadOverrideInputsFromSpec(newClusterBaseModel.Spec)},
	}, ignoreDownloadPolicy)
	if err != nil {
		w.logger.Errorf("Failed to diff ClusterBaseModel %s: %v", newClusterBaseModel.Name, err)
		return
	}

	if (policyChanged || hasChanges) && w.shouldDownloadModel(newClusterBaseModel.Spec.Storage) {
		w.logger.Infof("ClusterBaseModel %s need refresh", newClusterBaseModel.GetName())
		w.generateDownloadOverrideTaskBasedOnClusterBaseModel(newClusterBaseModel)
	}
}

func hasDownloadOverrideChanges(candidates []downloadOverrideChangeCandidate, opts ...cmp.Option) (bool, error) {
	for _, candidate := range candidates {
		result, err := kmp.SafeDiff(candidate.old, candidate.new, opts...)
		if err != nil {
			return false, fmt.Errorf("failed to diff %s: %w", candidate.name, err)
		}
		if result != "" {
			return true, nil
		}
	}
	return false, nil
}

func downloadOverrideInputsFromSpec(spec v1beta1.BaseModelSpec) downloadOverrideInputs {
	isTensorRTLLMModel := spec.ModelFormat.Name == constants.TensorRTLLM
	modelType := ""
	if isTensorRTLLMModel {
		modelType = string(constants.ServingBaseModel)
		if modelTypeFromMetadata, ok := spec.AdditionalMetadata["type"]; ok {
			modelType = modelTypeFromMetadata
		}
	}
	return downloadOverrideInputs{
		Storage:              spec.Storage,
		IsTensorRTLLMModel:   isTensorRTLLMModel,
		TensorRTLLMModelType: modelType,
	}
}

func (w *Scout) deleteBaseModel(obj interface{}) {
	baseModel, ok := obj.(*v1beta1.BaseModel)
	if !ok {
		w.logger.Errorf("Failed to convert %v to BaseModel", obj)
		return
	}

	w.logger.Infof("Deleting BaseModel: %s in namespace %s", baseModel.Name, baseModel.Namespace)

	gopherTask := &GopherTask{
		TaskType:  Delete,
		BaseModel: baseModel,
	}

	w.gopherChan <- gopherTask
}

func (w *Scout) deleteClusterBaseModel(obj interface{}) {
	clusterBaseModel, ok := obj.(*v1beta1.ClusterBaseModel)
	if !ok {
		w.logger.Errorf("Failed to convert %v to ClusterBaseModel", obj)
		return
	}

	w.logger.Infof("Deleting ClusterBaseModel: %s", clusterBaseModel.Name)

	gopherTask := &GopherTask{
		TaskType:         Delete,
		ClusterBaseModel: clusterBaseModel,
	}
	w.gopherChan <- gopherTask
}

func (w *Scout) addOrUpdateInferenceService(obj interface{}) {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		w.logger.Errorf("Failed to convert %v to InferenceService", obj)
		return
	}
	w.demandMu.Lock()
	demand := w.setInferenceServiceDemandLocked(isvc)
	w.demandMu.Unlock()
	if demand != nil {
		w.enqueueDemandedModel(demand.kind, demand.namespace, demand.name)
	}
}

// setInferenceServiceDemandLocked updates the endpoint demand indexes from one
// InferenceService. The caller must hold demandMu for writing.
func (w *Scout) setInferenceServiceDemandLocked(isvc *v1beta1.InferenceService) *modelDemandReference {
	endpointKey := isvc.Namespace + "/" + isvc.Name
	if !isvc.DeletionTimestamp.IsZero() || isvc.Spec.Model == nil ||
		strings.TrimSpace(isvc.Spec.Model.Name) == "" {
		w.setEndpointDemandLocked(endpointKey, "")
		return nil
	}

	kind, name, supported := inferenceServiceModelReference(isvc.Spec.Model)
	if !supported {
		w.logger.Debugf("InferenceService %s does not reference a supported OME model", endpointKey)
		w.setEndpointDemandLocked(endpointKey, "")
		return nil
	}
	modelKey := modelDemandKey(kind, isvc.Namespace, name)
	if !w.setEndpointDemandLocked(endpointKey, modelKey) {
		return nil
	}
	return &modelDemandReference{kind: kind, namespace: isvc.Namespace, name: name}
}

func (w *Scout) deleteInferenceService(obj interface{}) {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		if tombstone, tombstoneOK := obj.(cache.DeletedFinalStateUnknown); tombstoneOK {
			isvc, ok = tombstone.Obj.(*v1beta1.InferenceService)
		}
	}
	if !ok {
		w.logger.Errorf("Failed to convert deleted %v to InferenceService", obj)
		return
	}
	w.setEndpointDemand(isvc.Namespace+"/"+isvc.Name, "")
}

func (w *Scout) refreshAllEndpointDemands() {
	// Hold demandMu while taking and replaying the lister snapshot. This orders
	// informer updates after the snapshot publication so a stale snapshot can
	// never overwrite a newer endpoint event.
	w.demandMu.Lock()
	isvcs, err := w.inferenceServiceLister.List(labels.Everything())
	if err != nil {
		w.demandMu.Unlock()
		w.logger.Errorf("Unable to rebuild endpoint model demand: %v", err)
		return
	}
	w.endpointDemands = make(map[string]string)
	w.demandCounts = make(map[string]int)
	demands := make([]modelDemandReference, 0, len(isvcs))
	for _, isvc := range isvcs {
		if demand := w.setInferenceServiceDemandLocked(isvc); demand != nil {
			demands = append(demands, *demand)
		}
	}
	w.demandMu.Unlock()

	// Queue operations can perform lister and Kubernetes client work, so keep
	// them outside demandMu.
	for _, demand := range demands {
		w.enqueueDemandedModel(demand.kind, demand.namespace, demand.name)
	}
}

func (w *Scout) setEndpointDemand(endpointKey, modelKey string) bool {
	w.demandMu.Lock()
	defer w.demandMu.Unlock()
	return w.setEndpointDemandLocked(endpointKey, modelKey)
}

// setEndpointDemandLocked mutates the demand indexes. The caller must hold
// demandMu for writing.
func (w *Scout) setEndpointDemandLocked(endpointKey, modelKey string) bool {
	previous := w.endpointDemands[endpointKey]
	if previous == modelKey {
		return false
	}
	if previous != "" {
		w.demandCounts[previous]--
		if w.demandCounts[previous] <= 0 {
			delete(w.demandCounts, previous)
		}
	}
	if modelKey == "" {
		delete(w.endpointDemands, endpointKey)
		return false
	}
	w.endpointDemands[endpointKey] = modelKey
	w.demandCounts[modelKey]++
	return true
}

func (w *Scout) isModelDemanded(modelKey string) bool {
	w.demandMu.RLock()
	defer w.demandMu.RUnlock()
	return w.demandCounts[modelKey] > 0
}

func (w *Scout) enqueueDemandedModel(kind, namespace, name string) {
	if strings.EqualFold(kind, constants.BaseModel) {
		model, err := w.baseModelLister.BaseModels(namespace).Get(name)
		if err != nil {
			w.logger.Debugf("Demanded BaseModel %s/%s is not available yet: %v", namespace, name, err)
			return
		}
		if w.shouldDownloadModel(model.Spec.Storage) {
			w.downloadBaseModel(model)
		}
		return
	}
	model, err := w.clusterBaseModelLister.Get(name)
	if err != nil {
		w.logger.Debugf("Demanded ClusterBaseModel %s is not available yet: %v", name, err)
		return
	}
	if w.shouldDownloadModel(model.Spec.Storage) {
		w.downloadClusterBaseModel(model)
	}
}

func modelDemandKey(kind, namespace, name string) string {
	if strings.EqualFold(strings.TrimSpace(kind), constants.BaseModel) {
		return "base/" + strings.TrimSpace(namespace) + "/" + strings.TrimSpace(name)
	}
	return "cluster/" + strings.TrimSpace(name)
}

func inferenceServiceModelReference(model *v1beta1.ModelRef) (string, string, bool) {
	if model == nil || strings.TrimSpace(model.Name) == "" {
		return "", "", false
	}
	if model.APIGroup != nil {
		apiGroup := strings.TrimSpace(*model.APIGroup)
		if apiGroup != "" && !strings.EqualFold(apiGroup, constants.OMEAPIGroupName) {
			return "", "", false
		}
	}

	kind := constants.ClusterBaseModel
	if model.Kind != nil && strings.TrimSpace(*model.Kind) != "" {
		kind = strings.TrimSpace(*model.Kind)
	}
	switch {
	case strings.EqualFold(kind, constants.BaseModel):
		return constants.BaseModel, strings.TrimSpace(model.Name), true
	case strings.EqualFold(kind, constants.ClusterBaseModel):
		return constants.ClusterBaseModel, strings.TrimSpace(model.Name), true
	default:
		return "", "", false
	}
}

// reconcilePendingDeletions checks for any resources with deletion timestamps
// and processes them to ensure no deletions are missed if the model agent was down
// when the deletion request was made
func (w *Scout) reconcilePendingDeletions() {
	w.logger.Info("Checking for pending deletions on startup...")

	// Check BaseModels with deletionTimestamp
	baseModels, err := w.baseModelLister.List(labels.Everything())
	if err != nil {
		w.logger.Errorf("Failed to list BaseModels during reconciliation: %v", err)
	} else {
		for _, baseModel := range baseModels {
			if !baseModel.ObjectMeta.DeletionTimestamp.IsZero() {
				w.logger.Infof("Found BaseModel with deletion timestamp during startup: %s in namespace %s",
					baseModel.Name, baseModel.Namespace)
				w.deleteBaseModel(baseModel)
			}
		}
	}

	// Check ClusterBaseModels with deletionTimestamp
	clusterBaseModels, err := w.clusterBaseModelLister.List(labels.Everything())
	if err != nil {
		w.logger.Errorf("Failed to list ClusterBaseModels during reconciliation: %v", err)
	} else {
		for _, clusterBaseModel := range clusterBaseModels {
			if !clusterBaseModel.ObjectMeta.DeletionTimestamp.IsZero() {
				w.logger.Infof("Found ClusterBaseModel with deletion timestamp during startup: %s",
					clusterBaseModel.Name)
				w.deleteClusterBaseModel(clusterBaseModel)
			}
		}
	}

	w.logger.Info("Finished checking for pending deletions")
}

// shouldDownloadModelCommon encapsulates the shared logic for determining
// whether a model should be downloaded on this node. The defaultDecision
// controls the final return value when no other conditions cause an early return.
func (w *Scout) shouldDownloadModelCommon(storageSpec *v1beta1.StorageSpec, defaultDecision bool) bool {
	if storageSpec == nil {
		// If storage is nil, default to true (backward compatibility)
		return true
	}

	// Check if it's PVC storage type - if so, skip it entirely
	if storageSpec.StorageUri != nil {
		storageType, err := storage.GetStorageType(*storageSpec.StorageUri)
		if err == nil && storageType == storage.StorageTypePVC {
			w.logger.Debugf("Skipping PVC storage type in shouldDownloadModel (handled by BaseModel controller)")
			return false
		}
	}

	// Check NodeSelector if specified
	if len(storageSpec.NodeSelector) > 0 {
		for key, value := range storageSpec.NodeSelector {
			nodeValue, exists := w.nodeInfo.Labels[key]
			if !exists || nodeValue != value {
				return false
			}
		}
	}

	// Check NodeAffinity if specified
	if storageSpec.NodeAffinity != nil && storageSpec.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		nodeSelectorTerms := storageSpec.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		if len(nodeSelectorTerms) > 0 {
			matches := false
			for _, term := range nodeSelectorTerms {
				if w.nodeMatchesSelectorTerm(term) {
					matches = true
					break
				}
			}
			if !matches {
				return false
			}
		}
	}

	// Return the caller-provided default when no other condition rejected it
	return defaultDecision
}

// shouldDownloadModel checks if a model should be downloaded to this node based on node selector and node affinity
func (w *Scout) shouldDownloadModel(storageSpec *v1beta1.StorageSpec) bool {
	return w.shouldDownloadModelCommon(storageSpec, true)
}

func (w *Scout) nodeMatchesSelectorTerm(term v1.NodeSelectorTerm) bool {
	// Check match expressions
	for _, expr := range term.MatchExpressions {
		if !w.nodeMatchesExpression(expr) {
			return false
		}
	}

	// Check match fields
	for _, field := range term.MatchFields {
		if !w.nodeMatchesExpression(field) {
			return false
		}
	}

	return true
}

func (w *Scout) nodeMatchesExpression(expr v1.NodeSelectorRequirement) bool {
	// Get the field value based on whether it's a label or field selector
	var values []string
	var exists bool

	// For label selectors, get the label values
	labelValue, labelExists := w.nodeInfo.Labels[expr.Key]
	if labelExists {
		values = []string{labelValue}
		exists = true
	}

	// If not found in labels, try fields (only for special fields)
	if !exists {
		switch expr.Key {
		case "metadata.name":
			values = []string{w.nodeInfo.Name}
			exists = true
			// Add other field cases as needed
		}
	}

	if !exists {
		return expr.Operator == v1.NodeSelectorOpDoesNotExist
	}

	switch expr.Operator {
	case v1.NodeSelectorOpIn:
		for _, v := range values {
			for _, requiredValue := range expr.Values {
				if v == requiredValue {
					return true
				}
			}
		}
		return false
	case v1.NodeSelectorOpNotIn:
		for _, v := range values {
			for _, requiredValue := range expr.Values {
				if v == requiredValue {
					return false
				}
			}
		}
		return true
	case v1.NodeSelectorOpExists:
		return true
	case v1.NodeSelectorOpDoesNotExist:
		return false
	case v1.NodeSelectorOpGt:
		if len(values) == 0 || len(expr.Values) == 0 {
			return false
		}
		// Try to convert to integers for numeric comparison
		nodeVal, nodeErr := strconv.Atoi(values[0])
		requiredVal, reqErr := strconv.Atoi(expr.Values[0])
		if nodeErr == nil && reqErr == nil {
			// If both values can be parsed as integers, do numeric comparison
			return nodeVal > requiredVal
		}
		// Fall back to string comparison if not numeric
		return values[0] > expr.Values[0]
	case v1.NodeSelectorOpLt:
		if len(values) == 0 || len(expr.Values) == 0 {
			return false
		}
		// Try to convert to integers for numeric comparison
		nodeVal, nodeErr := strconv.Atoi(values[0])
		requiredVal, reqErr := strconv.Atoi(expr.Values[0])
		if nodeErr == nil && reqErr == nil {
			// If both values can be parsed as integers, do numeric comparison
			return nodeVal < requiredVal
		}
		// Fall back to string comparison if not numeric
		return values[0] < expr.Values[0]
	}

	return false
}

func downloadPolicyOrDefault(storage *v1beta1.StorageSpec) v1beta1.DownloadPolicy {
	if storage == nil || storage.DownloadPolicy == nil {
		return v1beta1.AlwaysDownload // default
	}
	return *storage.DownloadPolicy
}

// isToDownloadOverrideDueToDownloadPolicyBasedOnCBM returns true when the download policy changes between
// the old and new ClusterBaseModel and the new storage type is HuggingFace. If the storage type
// cannot be determined from the new model's StorageUri, the method logs the error and returns false.
func (w *Scout) isToDownloadOverrideDueToDownloadPolicyBasedOnCBM(oldClusterBaseModel *v1beta1.ClusterBaseModel, newClusterBaseModel *v1beta1.ClusterBaseModel) bool {
	oldPolicy := downloadPolicyOrDefault(oldClusterBaseModel.Spec.Storage)
	newPolicy := downloadPolicyOrDefault(newClusterBaseModel.Spec.Storage)

	storageType, err := storage.GetStorageType(*newClusterBaseModel.Spec.Storage.StorageUri)
	if err != nil {
		w.logger.Errorf("Failed to get target directory path for model %s: %v", newClusterBaseModel.Name, err)
		return false
	}
	isToDownloadOverride := oldPolicy != newPolicy && storageType == storage.StorageTypeHuggingFace

	if isToDownloadOverride {
		w.logger.Infof("ClusterBaseModel: %s: download policy is changed from %s to %s", newClusterBaseModel.Name, oldPolicy, newPolicy)
	}
	return isToDownloadOverride
}

// isToDownloadOverrideDueToDownloadPolicyBasedOnBM returns true when the download policy changes between
// the old and new BaseModel and the new storage type is HuggingFace. If the storage type
// cannot be determined from the new model's StorageUri, the method logs the error and returns false.
func (w *Scout) isToDownloadOverrideDueToDownloadPolicyBasedOnBM(oldBaseModel *v1beta1.BaseModel, newBaseModel *v1beta1.BaseModel) bool {
	oldPolicy := downloadPolicyOrDefault(oldBaseModel.Spec.Storage)
	newPolicy := downloadPolicyOrDefault(newBaseModel.Spec.Storage)

	storageType, err := storage.GetStorageType(*newBaseModel.Spec.Storage.StorageUri)
	if err != nil {
		w.logger.Errorf("Failed to get target directory path for model %s: %v", newBaseModel.Name, err)
		return false
	}
	isToDownloadOverride := oldPolicy != newPolicy && storageType == storage.StorageTypeHuggingFace

	if isToDownloadOverride {
		w.logger.Infof("BaseModel: %s: download policy is changed from %s to %s", newBaseModel.Name, oldPolicy, newPolicy)
	}
	return isToDownloadOverride
}

/*
generateDownloadOverrideTaskBasedOnClusterBaseModel constructs and enqueues a DownloadOverride GopherTask
for the provided ClusterBaseModel.
The task is sent to w.gopherChan for processing by Gopher workers.
*/
func (w *Scout) generateDownloadOverrideTaskBasedOnClusterBaseModel(clusterBaseModel *v1beta1.ClusterBaseModel) {
	IsTensorrtLLMModel := clusterBaseModel.Spec.ModelFormat.Name == constants.TensorRTLLM

	modelType := string(constants.ServingBaseModel)
	if modelTypeFromMetadata, ok := clusterBaseModel.Spec.AdditionalMetadata["type"]; ok {
		modelType = modelTypeFromMetadata
	}

	gopherTask := &GopherTask{
		TaskType:         DownloadOverride,
		ClusterBaseModel: clusterBaseModel,
		ServingDemand: w.isModelDemanded(
			modelDemandKey(constants.ClusterBaseModel, "", clusterBaseModel.Name)),
		TensorRTLLMShapeFilter: &TensorRTLLMShapeFilter{
			IsTensorrtLLMModel: IsTensorrtLLMModel,
			ShapeAlias:         w.nodeShapeAlias,
			ModelType:          modelType,
		},
	}

	w.logger.Infof("generate DownloadOverride task %v", clusterBaseModel.Spec.DisplayName)
	w.gopherChan <- gopherTask
}

func (w *Scout) generateDownloadOverrideTaskBasedOnBaseModel(baseModel *v1beta1.BaseModel) {
	IsTensorrtLLMModel := baseModel.Spec.ModelFormat.Name == constants.TensorRTLLM

	modelType := string(constants.ServingBaseModel)
	if modelTypeFromMetadata, ok := baseModel.Spec.AdditionalMetadata["type"]; ok {
		modelType = modelTypeFromMetadata
	}
	gopherTask := &GopherTask{
		TaskType:  DownloadOverride,
		BaseModel: baseModel,
		ServingDemand: w.isModelDemanded(
			modelDemandKey(constants.BaseModel, baseModel.Namespace, baseModel.Name)),
		TensorRTLLMShapeFilter: &TensorRTLLMShapeFilter{
			IsTensorrtLLMModel: IsTensorrtLLMModel,
			ShapeAlias:         w.nodeShapeAlias,
			ModelType:          modelType,
		},
	}
	w.logger.Infof("generate DownloadOverride task %v", baseModel.Spec.DisplayName)
	w.gopherChan <- gopherTask
}
