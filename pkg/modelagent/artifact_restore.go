package modelagent

import (
	"context"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/constants"
)

func artifactRehydrationID(task *GopherTask) string {
	if task.BaseModel != nil {
		return task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation]
	}
	if task.ClusterBaseModel != nil {
		return task.ClusterBaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation]
	}
	return ""
}

// Residency controls enqueue reuse-capable work; source and explicit override
// metadata retain their existing refresh semantics.
func artifactDownloadAnnotations(annotations map[string]string) map[string]string {
	result := maps.Clone(annotations)
	delete(result, constants.ModelArtifactRehydrationIDAnnotation)
	delete(result, artifactResidencyAnnotation)
	if len(result) == 0 {
		return nil
	}
	return result
}

// One restoration attempt owns this guard, including cleanup, CAS retries, and final
// publication. Validation proof is never inferred from a startup Ready marker.
type artifactRestoreGuard struct {
	gopher    *Gopher
	task      *GopherTask
	validated bool
}
type artifactRestoreGuardKey struct{}

func (s *Gopher) artifactRestoreContext(ctx context.Context, task *GopherTask) context.Context {
	if task.TaskType != Download && task.TaskType != DownloadOverride || artifactRehydrationID(task) == "" || ctx.Value(artifactRestoreGuardKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, artifactRestoreGuardKey{}, &artifactRestoreGuard{gopher: s, task: task})
}

func validateArtifactRestore(ctx context.Context) error {
	if guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard); guard != nil {
		return guard.gopher.validateArtifactDownload(ctx, guard.task)
	}
	return nil
}

// Call only after validating bytes, retaining file locks through the report
// CAS and paired Node labels.
func reportArtifactRestore(ctx context.Context, key string, uid types.UID, validateEntry func(*corev1.ConfigMap, ModelEntry) error) error {
	guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard)
	if guard == nil {
		return nil
	}
	s, task := guard.gopher, guard.task
	if !guard.validated || key != getModelID(task.BaseModel, task.ClusterBaseModel) || uid != types.UID(getModelUID(task)) {
		return fmt.Errorf("artifact restoration requires current byte validation")
	}
	err := s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
		if err := s.validateArtifactDownload(ctx, task); err != nil {
			return false, err
		}
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if err := validateEntry(cm, entry); err != nil {
			return false, err
		}
		if entry.HfArtifactPendingDeletion != nil {
			return false, fmt.Errorf("artifact restoration cleanup is unfinished")
		}
		entry.Status, entry.Progress = ModelStatusReady, nil
		entry.ModelUID, entry.NodeUID = uid, s.artifactNodeUID
		entry.ArtifactRehydrationID = artifactRehydrationID(task)
		return writeModelEntry(cm.Data, key, entry)
	})
	if err != nil {
		return err
	}
	op := &NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel,
		ModelStateOnNode: Ready, artifactRequestID: artifactRehydrationID(task), nodeUID: s.artifactNodeUID}
	op.validateReady = func() error { return s.validateArtifactRestoreReport(ctx, task) }
	return s.nodeLabelReconciler.ReconcileNodeLabels(op)
}

// A retry may observe a committed report after a lost API response. It still
// rechecks live authority and the exact report before publishing paired labels.
func (s *Gopher) validateArtifactRestoreReport(ctx context.Context, task *GopherTask) error {
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		return err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
	if err != nil {
		return err
	}
	if entry.Status != ModelStatusReady || entry.ModelUID != types.UID(getModelUID(task)) || entry.NodeUID != s.artifactNodeUID || entry.ArtifactRehydrationID != artifactRehydrationID(task) || entry.HfArtifactKey == "" || entry.HfArtifactPendingDeletion != nil {
		return fmt.Errorf("artifact restoration report is absent or stale")
	}
	return nil
}
