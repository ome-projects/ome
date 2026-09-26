package modelagent

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// A replacement can already be deleting when an agent restarts, without ever
// passing through Download. Transfer only obsolete ordinary bookkeeping, not
// file-cleanup authority, before final removal invalidates this CR's UID.
func (s *Gopher) handoffOrdinaryArtifactDeletion(ctx context.Context, task *GopherTask) error {
	c := s.configMapReconciler
	meta := taskModelMeta(task)
	if c == nil || meta == nil || meta.UID == "" {
		return nil
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	c.cacheMutex.RLock()
	cached := c.modelCache[key]
	mismatch := cached != nil && cached.ModelUID != "" && cached.ModelUID != meta.UID
	c.cacheMutex.RUnlock()
	cm, err := c.getConfigMap(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil && cm.Data[key] != "" {
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return err
		}
		mismatch = mismatch || entry.ModelUID != "" && entry.ModelUID != meta.UID
	}
	if !mismatch {
		return nil
	}
	return c.handoffOrdinaryModelOwner(ctx, key, meta.UID, func() error {
		owned, err := s.artifactDeletionOwner(ctx, task, true)
		if err != nil {
			return err
		}
		if !owned {
			return fmt.Errorf("ordinary deletion handoff Model UID changed or disappeared")
		}
		return nil
	})
}
