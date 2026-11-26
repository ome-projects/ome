package modelagent

import (
	"context"
	"fmt"
	"os"
	"reflect"

	"k8s.io/apimachinery/pkg/types"
)

func validateDirectRestoreEntry(uid types.UID, entry ModelEntry) error {
	if entry.ModelUID != "" && entry.ModelUID != uid || entry.HfArtifactKey != "" || entry.HfArtifactPendingDeletion != nil || entry.DirectArtifactPendingDeletion != nil {
		return fmt.Errorf("Direct restoration ownership or cleanup changed")
	}
	return nil
}

// Check Direct ownership before inspection and prepare writes only after
// content validation proves repair is needed.
func (s *Gopher) prepareDirectRestore(ctx context.Context, task *GopherTask, path string, repair bool) error {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return err
	}
	if err := validateDirectRestoreEntry(types.UID(getModelUID(task)), entry); err != nil {
		return err
	}
	if err := validateArtifactRestore(ctx); err != nil {
		return err
	}
	if !repair {
		if guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard); guard != nil {
			_, err := os.Lstat(path)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			guard.freshDirectDownload = os.IsNotExist(err)
		}
		return nil
	}
	if err := s.checkDirectEvictionUsers(ctx, key, path); err != nil {
		return err
	}
	if err := s.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStateOnNode: Updating}); err != nil {
		return err
	}
	if err := s.configMapReconciler.checkDirectCleanupPath(ctx, path); err != nil {
		return err
	}
	if err := s.checkDirectEvictionUsers(ctx, key, path); err != nil {
		return err
	}
	if guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard); guard != nil {
		guard.freshDirectDownload = false
	}
	if err := validateArtifactRestore(ctx); err != nil {
		return err
	}
	return os.MkdirAll(path, 0o755)
}

// An absent destination can be restored while copies with identical storage
// wait for the same path lock. Recorded use or cleanup still blocks replacement.
func (s *Gopher) isUnattachedDirectCopy(model, owner *GopherTask, data map[string]string) bool {
	if getModelUID(model) == "" || isModelResourceDeleting(model.BaseModel, model.ClusterBaseModel) || artifactEvictionRequested(model) {
		return false
	}
	spec, ownerSpec := taskModelSpec(model), taskModelSpec(owner)
	if !reflect.DeepEqual(ownerSpec.Storage, spec.Storage) {
		return false
	}
	key := getModelID(model.BaseModel, model.ClusterBaseModel)
	if _, exists := data[key]; !exists {
		return true
	}
	entry, err := existingModelEntry(data, key)
	return err == nil && entry.Status == ModelStatusUpdating && entry.Config == nil && entry.Progress == nil &&
		entry.HfArtifactKey == "" && entry.HfArtifactPendingDeletion == nil && entry.HfArtifactPendingDownload == nil &&
		entry.DirectArtifactPendingDeletion == nil && (entry.ModelUID == "" || entry.ModelUID == types.UID(getModelUID(model)))
}
