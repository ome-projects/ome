package modelagent

import (
	"context"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/constants"
)

// The existing reconciliation loop also repairs missed request reports after
// restart, failed publication, or sibling repair. It never manufactures proof:
// missing work returns through the normal source validation/download path.
func (s *Gopher) recoverArtifactReports(ctx context.Context) error {
	if s.omeClient == nil || s.artifactNodeUID == "" {
		return nil
	}
	models, err := liveArtifactModels(ctx, s.omeClient)
	if err != nil {
		return err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	for _, task := range models {
		task.TaskType = Download
		// Satisfied reports need only this pass's live snapshots, with no
		// admission or per-model API lookup.
		if !s.artifactReportNeedsRecovery(task, cm, node) {
			continue
		}
		if err := s.validateArtifactDownload(ctx, task); err != nil {
			continue
		}
		task.ArtifactReportRecovery, task.NormalPriorityOnly = true, true
		s.enqueueTask(task)
	}
	return ctx.Err()
}

func (s *Gopher) artifactReportRecoveryNeeded(ctx context.Context, task *GopherTask) (bool, error) {
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		return false, err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return s.artifactReportNeedsRecovery(task, cm, node), nil
}

func (s *Gopher) artifactReportNeedsRecovery(task *GopherTask, cm *corev1.ConfigMap, node *corev1.Node) bool {
	request := artifactRehydrationID(task)
	if request == "" || len(validation.IsValidLabelValue(request)) != 0 || getModelUID(task) == "" ||
		artifactEvictionRequested(task) || isModelResourceDeleting(task.BaseModel, task.ClusterBaseModel) ||
		node.UID != s.artifactNodeUID || node.DeletionTimestamp != nil {
		return false
	}
	spec := taskModelSpec(task)
	// Periodic tasks come from live objects, without Scout's task-local GPU
	// shape filter. Apply the same Shared eligibility exclusion from the spec.
	modelType, hasModelType := spec.AdditionalMetadata["type"]
	if spec.ModelFormat.Name == constants.TensorRTLLM && (!hasModelType || modelType == string(constants.ServingBaseModel)) {
		return false
	}
	if !(&Scout{nodeInfo: node, logger: s.logger}).shouldDownloadModel(spec.Storage) {
		return false
	}
	_, eligible, err := newHfArtifactTaskInputForOCI(task, spec.Storage, s.modelRootDir)
	if err != nil || !eligible && !isDirectHfReuseEligible(task, spec.Storage) {
		return false
	}
	// Current reuse policy is not ownership of an existing Direct directory.
	// Leave such paths untouched even after loss of their ConfigMap entry.
	path := getDestPath(&spec, s.modelRootDir)
	info, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) || err == nil && (info.Mode()&os.ModeSymlink == 0 || !isSharedHfArtifactSymlink(path)) {
		return false
	}
	if cm == nil {
		return true
	}
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
	label, labelErr := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
	return err != nil || labelErr != nil || entry.Status != ModelStatusReady ||
		entry.ModelUID != types.UID(getModelUID(task)) || entry.NodeUID != s.artifactNodeUID ||
		entry.ArtifactRehydrationID != request || entry.HfArtifactKey == "" || entry.HfArtifactPendingDeletion != nil ||
		node.Labels[label] != string(Ready) || node.Labels[constants.GetModelArtifactRequestLabel(types.UID(getModelUID(task)))] != request
}
