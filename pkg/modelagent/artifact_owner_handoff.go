package modelagent

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// handoffOrdinaryModelOwner handles an offline delete/recreate without an old
// delete event. Cleanup and shared ownership must finish through their own paths.
// Live proof is repeated on every CAS retry; the old cache is fenced only after
// the replacement owner is committed.
func (c *ConfigMapReconciler) handoffOrdinaryModelOwner(ctx context.Context, key string, uid types.UID, verifyCurrent func() error) error {
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	var oldUID types.UID
	var replacement ModelEntry
	err := c.mutateConfigMapWithModelUIDLocked(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
		oldUID = ""
		if cm.Data[key] == "" {
			return false, nil
		}
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		c.cacheMutex.RLock()
		invalidated := c.isModelUIDInvalidatedLocked(key, uid)
		var cached CacheEntry
		if current := c.modelCache[key]; current != nil {
			cached = *current
		}
		c.cacheMutex.RUnlock()
		if invalidated || uid == "" {
			return false, fmt.Errorf("cannot hand off model %s to a stale or empty UID", key)
		}
		cachedReplacement := cached.ModelUID != "" && cached.ModelUID != uid
		if entry.DirectArtifactPendingEviction != nil || entry.ModelUID == uid && entry.Status == ModelStatusEvicted {
			return false, fmt.Errorf("model %s has pending or completed local eviction", key)
		}
		if entry.ModelUID == "" || entry.ModelUID == uid && !cachedReplacement {
			return false, nil
		}
		if err := ordinaryModelOwnerCanBeReplaced(entry, key); err != nil {
			return false, err
		}
		if cachedReplacement {
			snapshot := ordinaryCachedModelEntry(&cached)
			if cached.ModelEntryJSON != "" {
				if err := json.Unmarshal([]byte(cached.ModelEntryJSON), &snapshot); err != nil {
					return false, err
				}
			}
			if err := ordinaryModelOwnerCanBeReplaced(snapshot, key); err != nil {
				return false, err
			}
		}
		// Also reject one-sided parent references that are absent from the child.
		for parentKey, raw := range cm.Data {
			if !isHfArtifactConfigMapKey(parentKey) {
				continue
			}
			parent, err := decodeHfArtifactEntry(parentKey, raw)
			if err != nil {
				return false, err
			}
			if _, referenced := parent.Children[key]; referenced {
				return false, fmt.Errorf("model %s still has a shared parent reference", key)
			}
		}
		if verifyCurrent == nil {
			return false, fmt.Errorf("model UID handoff requires live verification")
		}
		if err := verifyCurrent(); err != nil {
			return false, err
		}
		if entry.ModelUID == uid {
			// The previous CAS committed but its response was lost.
			oldUID, replacement = cached.ModelUID, entry
			return false, nil
		}
		oldUID = entry.ModelUID
		replacement = ModelEntry{Name: entry.Name, ModelUID: uid, Status: ModelStatusUpdating}
		return writeModelEntry(cm.Data, key, replacement)
	})
	if err != nil || oldUID == "" {
		return err
	}
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	c.invalidateModelUIDAndEvictCacheLocked(key, oldUID)
	c.modelCache[key] = &CacheEntry{ModelName: replacement.Name, ModelUID: uid, ModelStatus: replacement.Status}
	delete(c.evictedModels, key)
	return nil
}

func ordinaryModelOwnerCanBeReplaced(entry ModelEntry, key string) error {
	if entry.HfArtifactPendingDeletion != nil || entry.HfArtifactKey != "" || entry.DirectArtifactPendingEviction != nil {
		return fmt.Errorf("model %s still has cleanup or shared ownership", key)
	}
	if entry.Config != nil {
		artifact := entry.Config.Artifact
		if len(artifact.ChildrenPaths) != 0 || len(artifact.ParentPath) > 1 || len(artifact.ParentPath) == 1 && artifact.ParentPath[key] == "" {
			return fmt.Errorf("model %s still has legacy shared ownership", key)
		}
	}
	return nil
}
