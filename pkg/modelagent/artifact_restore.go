package modelagent

import (
	"context"
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
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

// This node's prior report authorizes leaving Shared mode. It is historical
// evidence only, never proof that newly downloaded ordinary bytes are ready.
// Retain it on the logical task because Updating and detachment replace the report.
type completedArtifactRequest struct{ snapshot *GopherTask }
type completedArtifactRequestKey struct{}

// Placement can change while bytes stay the same. Every attempt still checks
// live placement eligibility before this historical evidence can be used.
func (proof *completedArtifactRequest) matches(task *GopherTask) bool {
	if proof == nil || proof.snapshot == nil {
		return false
	}
	old := proof.snapshot
	return getModelID(old.BaseModel, old.ClusterBaseModel) == getModelID(task.BaseModel, task.ClusterBaseModel) &&
		getModelUID(old) == getModelUID(task) && artifactRehydrationID(old) == artifactRehydrationID(task) &&
		cmp.Equal(downloadOverrideInputsFromSpec(taskModelSpec(old)), downloadOverrideInputsFromSpec(taskModelSpec(task)),
			cmpopts.IgnoreFields(v1beta1.StorageSpec{}, "NodeAffinity", "NodeSelector"))
}

func historicalArtifactRequest(ctx context.Context, task *GopherTask) bool {
	if task.completedArtifactRequest.matches(task) {
		return true
	}
	proof, _ := ctx.Value(completedArtifactRequestKey{}).(*completedArtifactRequest)
	return proof.matches(task)
}

func activeArtifactRestore(ctx context.Context, task *GopherTask) bool {
	return artifactRehydrationID(task) != "" && !historicalArtifactRequest(ctx, task)
}

func (s *Gopher) classifyCompletedArtifactRequest(ctx context.Context, task *GopherTask) {
	if artifactRehydrationID(task) == "" || task.completedArtifactRequest.matches(task) || s.configMapReconciler == nil {
		return
	}
	_, eligible, err := newHfArtifactTaskInputForOCI(task, taskModelSpec(task).Storage, s.modelRootDir)
	if err != nil || eligible || isDirectHfReuseEligible(task, taskModelSpec(task).Storage) {
		return
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return
	}
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
	if err != nil || entry.Status != ModelStatusReady || entry.ModelUID != types.UID(getModelUID(task)) || entry.ArtifactRehydrationID != artifactRehydrationID(task) || entry.HfArtifactPendingDeletion != nil {
		return
	}
	snapshot := &GopherTask{BaseModel: task.BaseModel.DeepCopy(), ClusterBaseModel: task.ClusterBaseModel.DeepCopy()}
	task.completedArtifactRequest = &completedArtifactRequest{snapshot: snapshot}
}

// One restoration attempt owns this guard through cleanup and publication.
// Readiness follows source validation or Shared attachment after startup checks.
type artifactRestoreGuard struct {
	gopher    *Gopher
	task      *GopherTask
	validated bool // Source checks passed or a trusted Ready Shared parent was attached.
	reporting bool // Publication failures must remain eligible for report recovery.
	// Valid only while the initial-download handler retains both file locks.
	freshSharedDownload bool
}
type artifactRestoreGuardKey struct{}

func (s *Gopher) artifactRestoreContext(ctx context.Context, task *GopherTask) context.Context {
	if historicalArtifactRequest(ctx, task) {
		proof := task.completedArtifactRequest
		if proof == nil {
			proof, _ = ctx.Value(completedArtifactRequestKey{}).(*completedArtifactRequest)
		}
		ctx = context.WithValue(ctx, artifactRestoreGuardKey{}, (*artifactRestoreGuard)(nil))
		return context.WithValue(ctx, completedArtifactRequestKey{}, proof)
	}
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

// Retain file locks through the readiness checks, report CAS, and the Node Ready label.
func reportArtifactRestore(ctx context.Context, key string, uid types.UID, validateEntry func(*corev1.ConfigMap, ModelEntry) error) error {
	guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard)
	if guard == nil {
		return nil
	}
	s, task := guard.gopher, guard.task
	if key != getModelID(task.BaseModel, task.ClusterBaseModel) || uid != types.UID(getModelUID(task)) {
		return fmt.Errorf("artifact restoration belongs to another model")
	}
	guard.reporting = true
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
		if !guard.validated {
			return false, fmt.Errorf("artifact restoration requires current readiness checks")
		}
		if entry.HfArtifactPendingDeletion != nil {
			return false, fmt.Errorf("artifact restoration cleanup is unfinished")
		}
		entry.Status, entry.Progress = ModelStatusReady, nil
		entry.ArtifactRehydrationFailed = false
		entry.ModelUID = uid
		entry.ArtifactRehydrationID = artifactRehydrationID(task)
		return writeModelEntry(cm.Data, key, entry)
	})
	if err != nil {
		return err
	}
	op := &NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel,
		ModelStateOnNode: Ready, requireNodeCAS: true}
	op.validateReady = func() error { return s.validateArtifactRestoreReport(ctx, task) }
	return s.nodeLabelReconciler.ReconcileNodeLabels(op)
}

// A retry may observe a committed report after a lost API response. It still
// rechecks live authority and the exact report before publishing the Ready label.
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
	if entry.Status != ModelStatusReady || entry.ModelUID != types.UID(getModelUID(task)) || entry.ArtifactRehydrationID != artifactRehydrationID(task) || entry.HfArtifactKey == "" || entry.HfArtifactPendingDeletion != nil {
		return fmt.Errorf("artifact restoration report is absent or stale")
	}
	return nil
}
