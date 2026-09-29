package modelagent

import (
	"context"
	"fmt"
	"os"

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
	if !repair {
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
		return validateArtifactRestore(ctx)
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
	if err := validateArtifactRestore(ctx); err != nil {
		return err
	}
	return os.MkdirAll(path, 0o755)
}
