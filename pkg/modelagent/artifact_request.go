package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

var errArtifactStatusLiveValidation = errors.New("artifact status live validation failed")

// A Ready label alone may belong to an earlier model, node, or restore cycle.
func modelRehydrationAcknowledged(task *GopherTask, node *corev1.Node, cm *corev1.ConfigMap) bool {
	meta := taskModelMeta(task)
	request := artifactRehydrationID(task)
	if request == "" || cm == nil || node == nil || node.UID == "" ||
		cm.Annotations[constants.ModelArtifactNodeUIDAnnotation] != string(node.UID) {
		return false
	}
	var entry ModelEntry
	if json.Unmarshal([]byte(cm.Data[getModelID(task.BaseModel, task.ClusterBaseModel)]), &entry) != nil ||
		entry.Status != ModelStatusReady || entry.ModelUID != meta.UID || entry.ArtifactRehydrationID != request {
		return false
	}
	requestLabel, err := constants.ArtifactReadyLabelKey(meta.UID)
	if err != nil || node.Labels[requestLabel] != request {
		return false
	}
	modelLabel, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
	return err == nil && node.Labels[modelLabel] == string(Ready)
}

func (w *Scout) reconcileArtifactRequests(ctx context.Context) error {
	models, err := w.baseModelLister.List(labels.Everything())
	if err != nil {
		return err
	}
	clusterModels, err := w.clusterBaseModelLister.List(labels.Everything())
	if err != nil {
		return err
	}
	var tasks []*GopherTask
	for _, model := range models {
		tasks = append(tasks, &GopherTask{TaskType: Download, BaseModel: model})
	}
	for _, model := range clusterModels {
		tasks = append(tasks, &GopherTask{TaskType: Download, ClusterBaseModel: model})
	}
	var node *corev1.Node
	var cm *corev1.ConfigMap
	for _, task := range tasks {
		meta := taskModelMeta(task)
		if !meta.DeletionTimestamp.IsZero() || (!modelEvictionRequested(meta) && artifactRehydrationID(task) == "") {
			continue
		}
		if node == nil {
			node, err = w.kubeClient.CoreV1().Nodes().Get(ctx, w.nodeName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			cm, err = w.kubeClient.CoreV1().ConfigMaps(w.configMapNamespace).Get(ctx, w.nodeName, metav1.GetOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		// Use this pass's fresh node, without mutating the watch handler's copy.
		eligibility := Scout{nodeInfo: node, logger: w.logger}
		spec := taskModelSpec(task)
		if !eligibility.shouldDownloadModel(spec.Storage) {
			continue
		}
		if modelEvictionRequested(meta) {
			task.TaskType = Evict
		} else if modelRehydrationAcknowledged(task, node, cm) {
			continue
		} else {
			task.ArtifactRequestReplay = true
		}
		modelType := spec.AdditionalMetadata["type"]
		if modelType == "" {
			modelType = string(constants.ServingBaseModel)
		}
		task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: spec.ModelFormat.Name == constants.TensorRTLLM, ShapeAlias: w.nodeShapeAlias, ModelType: modelType}
		select {
		case w.gopherChan <- task:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *Gopher) artifactRequestAlreadyReady(ctx context.Context, task *GopherTask) (bool, error) {
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.Path == nil || *spec.Storage.Path == "" || spec.Storage.StorageUri == nil {
		return false, fmt.Errorf("artifact acknowledgement requires an owned path")
	}
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil || !modelRehydrationAcknowledged(task, node, cm) {
		return false, err
	}
	info, err := os.Stat(getDestPath(&spec, s.modelRootDir))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil && info.IsDir(), err
}

func artifactRehydrationID(task *GopherTask) string {
	if task == nil || taskModelMeta(task) == nil {
		return ""
	}
	return taskModelMeta(task).Annotations[constants.ModelArtifactRehydrationIDAnnotation]
}

// Match the ordinary writer's bounded scope without opting unsupported legacy
// sources or paths into request-bound status publication.
func (s *Gopher) isBoundedDirectArtifactTask(task *GopherTask) bool {
	if task == nil || taskModelMeta(task) == nil {
		return false
	}
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.StorageUri == nil {
		return false
	}
	source, err := storage.GetStorageType(*spec.Storage.StorageUri)
	if err != nil || (source != storage.StorageTypeOCI && source != storage.StorageTypeHuggingFace) {
		return false
	}
	_, err = s.directArtifactPath(task)
	return err == nil
}

func (s *Gopher) artifactRequestNeedsValidation(ctx context.Context, task *GopherTask) (bool, error) {
	request := artifactRehydrationID(task)
	if request == "" {
		return false, nil
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	raw := cm.Data[getModelID(task.BaseModel, task.ClusterBaseModel)]
	if raw == "" {
		return true, nil
	}
	var entry ModelEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return false, err
	}
	return entry.ModelUID != taskModelMeta(task).UID || entry.ArtifactRehydrationID != request, nil
}

func (s *Gopher) checkCurrentArtifactRequest(ctx context.Context, task *GopherTask) error {
	if artifactRehydrationID(task) == "" {
		return ctx.Err()
	}
	skip, _, err := s.shouldSkipArtifactTask(ctx, task)
	if err != nil {
		return err
	}
	if skip {
		return fmt.Errorf("artifact request changed during operation")
	}
	return ctx.Err()
}

// Finish admitted direct cleanup before routing to either direct or shared
// download. A policy change must not strand the old deletion receipt.
func (s *Gopher) settleDirectArtifactEviction(ctx context.Context, task *GopherTask) (bool, error) {
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	raw := cm.Data[getModelID(task.BaseModel, task.ClusterBaseModel)]
	if raw == "" {
		return false, nil
	}
	var entry ModelEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return false, err
	}
	if entry.ArtifactPendingEviction == nil {
		return false, nil
	}
	unlock, acquired, err := s.acquireDirectArtifactPath(task)
	if err != nil || !acquired {
		return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(gopherTaskModelKey(task), err))
	}
	defer unlock()
	return false, s.resumeDirectArtifactEviction(ctx, task)
}

// A task acknowledges only its captured request and source. A fresh API read
// prevents a late completion from publishing readiness for a replacement CR
// or for a new request that the informer has not observed yet.
func (s *Gopher) shouldSkipArtifactTask(ctx context.Context, task *GopherTask) (bool, bool, error) {
	if s.modelClient == nil {
		return false, false, fmt.Errorf("rehydration requires a live model client")
	}
	latest := *task
	var err error
	if task.BaseModel != nil {
		latest.BaseModel, err = s.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(ctx, task.BaseModel.Name, metav1.GetOptions{})
	} else {
		latest.ClusterBaseModel, err = s.modelClient.OmeV1beta1().ClusterBaseModels().Get(ctx, task.ClusterBaseModel.Name, metav1.GetOptions{})
	}
	if apierrors.IsNotFound(err) {
		return true, false, nil
	}
	if err != nil {
		return false, false, err
	}
	meta := taskModelMeta(&latest)
	if meta.UID != taskModelMeta(task).UID || artifactRehydrationID(&latest) != artifactRehydrationID(task) ||
		!reflect.DeepEqual(downloadOverrideInputsFromSpec(taskModelSpec(task)), downloadOverrideInputsFromSpec(taskModelSpec(&latest))) ||
		!reflect.DeepEqual(downloadAnnotations(meta.Annotations), downloadAnnotations(taskModelMeta(task).Annotations)) {
		return true, false, nil
	}
	if !meta.DeletionTimestamp.IsZero() {
		return true, true, nil
	}
	if modelEvictionRequested(meta) {
		blocked, err := s.shouldBlockDownloadForEviction(ctx, task)
		if err != nil || blocked {
			return blocked, false, err
		}
	}
	spec := taskModelSpec(&latest)
	if spec.Storage == nil || spec.Storage.StorageUri == nil || *spec.Storage.StorageUri == "" ||
		spec.Storage.Path == nil || *spec.Storage.Path == "" {
		return false, false, fmt.Errorf("rehydration requires an explicit storage URI and path")
	}
	if len(spec.Storage.NodeSelector) == 0 && spec.Storage.NodeAffinity == nil {
		return false, false, nil
	}
	if s.kubeClient == nil || s.configMapReconciler == nil {
		return false, false, fmt.Errorf("rehydration requires current node eligibility")
	}
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return false, false, err
	}
	scout := Scout{nodeInfo: node, logger: s.logger}
	return !scout.shouldDownloadModel(spec.Storage), false, nil
}
