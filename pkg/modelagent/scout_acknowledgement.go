package modelagent

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
)

// Informer resync is disabled. Periodically validate missing reports/labels so
// API write failures and node/ConfigMap replacement recover without CR edits.
func (w *Scout) runArtifactAcknowledgementRecovery(stopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()
	go func() {
		select {
		case <-stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	interval := w.acknowledgementInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := w.reconcileArtifactAcknowledgements(ctx); err != nil && ctx.Err() == nil {
			w.logger.Warnf("Cannot reconcile artifact acknowledgements: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Scout) reconcileArtifactAcknowledgements(ctx context.Context) error {
	node, err := w.kubeClient.CoreV1().Nodes().Get(ctx, w.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.UID == "" {
		return fmt.Errorf("node identity is unavailable for artifact acknowledgement recovery")
	}
	cm, err := w.kubeClient.CoreV1().ConfigMaps(w.configMapNamespace).Get(ctx, w.nodeName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	data := map[string]string{}
	if err == nil {
		data = cm.Data
	}
	instanceType := node.Labels[constants.NodeInstanceShapeLabel]
	if instanceType == "" {
		instanceType = node.Labels[constants.DeprecatedNodeInstanceShapeLabel]
	}
	shape, _ := utils.GetInstanceTypeShortName(instanceType)
	// Never mutate the informer handlers' nodeInfo from this periodic worker.
	placement := Scout{nodeInfo: node, logger: w.logger}
	enqueue := func(task *GopherTask) error {
		meta := taskModelMeta(task)
		request := meta.Annotations[constants.ModelArtifactRehydrationIDAnnotation]
		if request == "" || meta.UID == "" || meta.DeletionTimestamp != nil || artifactEvictionRequested(task) || !placement.shouldDownloadModel(taskModelSpec(task).Storage) {
			return nil
		}
		current, err := artifactAcknowledgementCurrent(task, node, data)
		if err != nil || current {
			return err
		}
		// Use the same full-validation operation as explicit input refreshes.
		// A newer recovery task cannot swallow an explicit override's validation.
		task.TaskType = DownloadOverride
		task.artifactAcknowledgementRecovery = true
		task.TensorRTLLMShapeFilter = artifactTaskShapeFilter(taskModelSpec(task), shape)
		select {
		case w.gopherChan <- task:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	models, err := w.baseModelLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, model := range models {
		if err := enqueue(&GopherTask{BaseModel: model}); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.logger.Warnf("Cannot recover artifact acknowledgement for BaseModel %s/%s: %v", model.Namespace, model.Name, err)
		}
	}
	clusters, err := w.clusterBaseModelLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, model := range clusters {
		if err := enqueue(&GopherTask{ClusterBaseModel: model}); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.logger.Warnf("Cannot recover artifact acknowledgement for ClusterBaseModel %s: %v", model.Name, err)
		}
	}
	return nil
}

func artifactAcknowledgementCurrent(task *GopherTask, node *corev1.Node, data map[string]string) (bool, error) {
	meta := taskModelMeta(task)
	request := meta.Annotations[constants.ModelArtifactRehydrationIDAnnotation]
	if meta.UID == "" || request == "" || node.UID == "" {
		return false, nil
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	if _, exists := data[key]; !exists {
		return false, nil
	}
	entry, err := existingModelEntry(data, key)
	if err != nil {
		return false, err
	}
	label, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
	if err != nil {
		return false, err
	}
	return entry.Status == ModelStatusReady && entry.ModelUID == meta.UID && entry.ArtifactRehydrationID == request && entry.NodeUID == node.UID &&
		node.Labels[label] == string(Ready) && node.Labels[constants.GetModelArtifactRequestLabel(meta.UID)] == request, nil
}

// Recheck queued periodic work against fresh evidence. This never acknowledges
// or publishes anything; missing evidence still takes the full validation path.
func (s *Gopher) artifactRecoveryAlreadyAcknowledged(ctx context.Context, task *GopherTask) (bool, error) {
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		return false, err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return artifactAcknowledgementCurrent(task, node, cm.Data)
}
