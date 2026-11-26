package modelagent

import (
	"context"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The existing reconciliation loop also repairs missed request reports after
// restart, failed publication, or sibling repair. It never manufactures proof:
// missing work returns through the normal source validation/download path.
func (s *Gopher) recoverArtifactReports(ctx context.Context) error {
	if s.omeClient == nil {
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
		if isDirectOCIRestoreEligible(task) {
			filter, err := ociRestoreShapeFilter(task, node)
			if err != nil {
				s.logger.Debugw("Skipping artifact report recovery after shape filtering failed", "model", getModelInfoForLogging(task), "error", err)
				continue
			}
			task.TensorRTLLMShapeFilter = filter
		}
		// Satisfied reports need only this pass's live snapshots, with no
		// admission or per-model API lookup.
		if !s.artifactReportNeedsRecovery(task, cm, node) {
			continue
		}
		if err := s.validateArtifactDownloadOnNode(ctx, task, true, node); err != nil {
			if ctx.Err() == nil {
				s.logger.Debugw("Skipping artifact report recovery after validation failed", "model", getModelInfoForLogging(task), "error", err)
			}
			continue
		}
		task.ArtifactReportRecovery, task.NormalPriorityOnly = true, true
		s.enqueueTask(task)
	}
	return ctx.Err()
}

func (s *Gopher) artifactReportRecoveryNeeded(ctx context.Context, task *GopherTask) (bool, error) {
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if err := s.validateArtifactDownloadOnNode(ctx, task, true, node); err != nil {
		return false, err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	return s.artifactReportNeedsRecovery(task, cm, node), nil
}

func (s *Gopher) artifactReportNeedsRecovery(task *GopherTask, cm *corev1.ConfigMap, node *corev1.Node) bool {
	request := artifactRehydrationID(task)
	if request == "" || getModelUID(task) == "" ||
		artifactEvictionRequested(task) || isModelResourceDeleting(task.BaseModel, task.ClusterBaseModel) ||
		node.DeletionTimestamp != nil {
		return false
	}
	spec := taskModelSpec(task)
	if !(&Scout{nodeInfo: node, logger: s.logger}).shouldDownloadModel(spec.Storage) {
		return false
	}
	_, eligible, err := newHfArtifactTaskInputForOCI(task, spec.Storage, s.modelRootDir)
	direct := isDirectHfRestoreEligible(task, spec.Storage) || isDirectOCIRestoreEligible(task)
	if err != nil || !eligible && !direct {
		return false
	}
	// Direct sources use the configured directory, including after loss of their
	// report. Existing directories are never adopted as Shared parents.
	path := getDestPath(&spec, s.modelRootDir)
	info, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		return false
	}
	sharedSymlink := err == nil && info.Mode()&os.ModeSymlink != 0
	if sharedSymlink {
		if !isSharedHfArtifactSymlink(path) {
			return false
		}
	} else if direct {
		if _, err := s.directEvictionPath(path); err != nil {
			return false
		}
	} else if err == nil {
		return false
	}
	if cm == nil {
		return true
	}
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
	// A reported failure completes this attempt; recovery repairs missing
	// reports, not failed downloads. Older identities do not block new work.
	if err == nil && entry.Status == ModelStatusFailed && entry.ArtifactRehydrationFailed &&
		entry.ModelUID == types.UID(getModelUID(task)) &&
		entry.ArtifactRehydrationID == request {
		return false
	}
	label, labelErr := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
	return err != nil || labelErr != nil || entry.Status != ModelStatusReady ||
		entry.ModelUID != types.UID(getModelUID(task)) ||
		entry.ArtifactRehydrationID != request || (!direct || sharedSymlink) && entry.HfArtifactKey == "" || entry.HfArtifactPendingDeletion != nil || entry.DirectArtifactPendingDeletion != nil ||
		node.Labels[label] != string(Ready)
}
