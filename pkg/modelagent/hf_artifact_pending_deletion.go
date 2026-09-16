package modelagent

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// HfArtifactPendingDeletion is a cleanup receipt, not a live parent reference.
// It is written in the same ConfigMap CAS that removes both live references.
type HfArtifactPendingDeletion struct {
	Identity       HfArtifactIdentity `json:"identity"`
	ParentPath     string             `json:"parentPath"`
	ChildPath      string             `json:"childPath"`
	ModelUID       types.UID          `json:"modelUID"`
	ParentLockID   string             `json:"parentLockId,omitempty"`
	ParentWasReady bool               `json:"parentWasReady,omitempty"`
}

func (pending HfArtifactPendingDeletion) parentForChild(key string) HfArtifactEntry {
	return HfArtifactEntry{
		Key: hfArtifactConfigMapKey(pending.Identity), Identity: pending.Identity,
		LocalPath: pending.ParentPath, Status: HfArtifactStatusUpdating,
		LockID: pending.ParentLockID, Children: map[string]string{key: pending.ChildPath},
	}
}

func (r *HfArtifactRepository) pendingDeletion(ctx context.Context, key string) (*HfArtifactPendingDeletion, error) {
	cm, err := r.configMaps.getConfigMap(ctx)
	if err != nil {
		return nil, err
	}
	if _, exists := cm.Data[key]; !exists {
		return nil, nil
	}
	child, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return nil, err
	}
	return child.HfArtifactPendingDeletion, nil
}

// claimPendingDeletion transfers only the exact orphaned receipt to a proven
// current UID. Both OS locks are held. A lister-verified UID can replace an old
// cache owner, but a different parent owner or receipt cannot be stolen.
func (r *HfArtifactRepository) claimPendingDeletion(ctx context.Context, key string, uid types.UID, pending *HfArtifactPendingDeletion, verifyCurrent func() (bool, error)) error {
	if uid == "" || verifyCurrent == nil {
		return fmt.Errorf("current lister UID proof is required for pending shared deletion of %s", key)
	}
	expected := *pending
	next := expected
	next.ModelUID = uid
	err := r.configMaps.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		r.configMaps.cacheMutex.RLock()
		invalidated := r.configMaps.isModelUIDInvalidatedLocked(key, uid)
		r.configMaps.cacheMutex.RUnlock()
		current, err := verifyCurrent()
		if err != nil {
			return false, err
		}
		if !current || invalidated {
			return false, fmt.Errorf("cannot prove current UID for pending shared deletion of %s", key)
		}
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if child.HfArtifactKey != "" || child.HfArtifactPendingDeletion == nil || *child.HfArtifactPendingDeletion != expected {
			return false, fmt.Errorf("pending shared deletion for %s changed", key)
		}
		expectedParent := expected.parentForChild(key)
		if _, exists := cm.Data[expectedParent.Key]; exists {
			parent, err := requireStoredHfArtifactEntry(cm.Data, expectedParent)
			if err != nil {
				return false, err
			}
			if _, referenced := parent.Children[key]; referenced {
				return false, fmt.Errorf("parent still references deleting child %s", key)
			}
			if !isValidHfArtifactStatus(parent.Status) || parent.Status == HfArtifactStatusUpdating && (expected.ParentLockID == "" || parent.LockID != expected.ParentLockID) {
				return false, fmt.Errorf("shared artifact %s has another owner or invalid status", parent.Key)
			}
		}
		child.HfArtifactPendingDeletion = &next
		return writeModelEntry(cm.Data, key, child)
	})
	if err == nil {
		*pending = next
		return r.adoptPendingDeletionUID(ctx, key, next, verifyCurrent)
	}
	return err
}

// The receipt CAS must commit before changing the process-local UID fence.
// Re-read it under the mutation mutex so failed/lost responses, concurrent
// status writes, and a changed lister UID cannot adopt an uncommitted owner.
func (r *HfArtifactRepository) adoptPendingDeletionUID(ctx context.Context, key string, expected HfArtifactPendingDeletion, verifyCurrent func() (bool, error)) error {
	c := r.configMaps
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	cm, err := c.getConfigMap(ctx)
	if err != nil {
		return err
	}
	child, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return err
	}
	if child.HfArtifactKey != "" || child.HfArtifactPendingDeletion == nil || *child.HfArtifactPendingDeletion != expected {
		return fmt.Errorf("pending shared deletion for %s changed before UID handoff", key)
	}
	current, err := verifyCurrent()
	if err != nil {
		return err
	}
	if !current || expected.ModelUID == "" {
		return fmt.Errorf("current UID for %s changed before handoff", key)
	}
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	if c.isModelUIDInvalidatedLocked(key, expected.ModelUID) {
		return fmt.Errorf("current UID for %s was invalidated before handoff", key)
	}
	if old := c.modelCache[key]; old != nil && old.ModelUID != "" && old.ModelUID != expected.ModelUID {
		if c.invalidatedModelUIDs[key] == nil {
			c.invalidatedModelUIDs[key] = make(map[types.UID]struct{})
		}
		c.invalidatedModelUIDs[key][old.ModelUID] = struct{}{}
	}
	c.modelCache[key] = &CacheEntry{ModelName: child.Name, ModelUID: expected.ModelUID,
		ModelStatus: child.Status, ModelEntryJSON: cm.Data[key]}
	delete(c.evictedModels, key)
	return nil
}

// deletionParent resumes this receipt's owner or atomically records a new
// deletion owner after startup recovery released the abandoned ConfigMap lock.
// The caller must hold both OS locks; a receipt never bypasses an active owner.
func (r *HfArtifactRepository) deletionParent(ctx context.Context, key string, pending *HfArtifactPendingDeletion) (HfArtifactEntry, bool, error) {
	var parent HfArtifactEntry
	found := false
	lockID := uuid.NewString()
	expected := *pending
	err := r.configMaps.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		parent, found = HfArtifactEntry{}, false
		if r.isChildMutationBlocked(key, expected.ModelUID) {
			return false, fmt.Errorf("pending shared deletion for %s has a stale UID", key)
		}
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if child.HfArtifactKey != "" || child.HfArtifactPendingDeletion == nil || *child.HfArtifactPendingDeletion != expected {
			return false, fmt.Errorf("pending shared deletion for %s changed", key)
		}
		expectedParent := expected.parentForChild(key)
		if _, exists := cm.Data[expectedParent.Key]; !exists {
			return false, nil
		}
		parent, err = requireStoredHfArtifactEntry(cm.Data, expectedParent)
		if err != nil {
			return false, err
		}
		found = true
		if _, exists := parent.Children[key]; exists {
			return false, fmt.Errorf("parent still references deleting child %s", key)
		}
		if parent.Status == HfArtifactStatusUpdating {
			if expected.ParentLockID == "" || parent.LockID != expected.ParentLockID {
				return false, fmt.Errorf("shared artifact %s has another owner", parent.Key)
			}
			return false, nil
		}
		if !isValidHfArtifactStatus(parent.Status) {
			return false, fmt.Errorf("shared artifact %s has invalid status", parent.Key)
		}
		if len(parent.Children) != 0 || expected.ParentLockID == "" {
			return false, nil
		}
		parent.Status = HfArtifactStatusUpdating
		parent.LockID = lockID
		parent.LastCompletedLockID = ""
		child.HfArtifactPendingDeletion.ParentLockID = lockID
		if _, err := writeHfArtifactEntry(cm.Data, parent); err != nil {
			return false, err
		}
		_, err = writeModelEntry(cm.Data, key, child)
		return true, err
	})
	if err == nil && found && parent.Status == HfArtifactStatusUpdating {
		pending.ParentLockID = parent.LockID
	}
	return parent, found, err
}

func (r *HfArtifactRepository) finishPendingDeletion(ctx context.Context, key string, expected HfArtifactPendingDeletion) error {
	return r.configMaps.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		if r.isChildMutationBlocked(key, expected.ModelUID) {
			return false, fmt.Errorf("pending shared deletion for %s has a stale UID", key)
		}
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if child.HfArtifactPendingDeletion == nil {
			return false, nil
		}
		if child.HfArtifactKey != "" || *child.HfArtifactPendingDeletion != expected {
			return false, fmt.Errorf("pending shared deletion for %s changed", key)
		}
		child.HfArtifactPendingDeletion = nil
		return writeModelEntry(cm.Data, key, child)
	})
}

func (h *hfArtifactTaskHandler) resumePendingDeletion(ctx context.Context, input hfArtifactTaskInput, pending *HfArtifactPendingDeletion) (hfArtifactTaskResult, error) {
	parent := pending.parentForChild(input.ChildModelKey)
	if pending.ChildPath != input.ChildModelPath {
		return newHfArtifactRetryResult(parent.Key, fmt.Errorf("pending shared deletion child path changed")), nil
	}
	if err := input.validateFilesystemPaths(parent); err != nil {
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	if pending.ModelUID != input.ChildModelUID || h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
		var verifyCurrent func() (bool, error)
		if h.isCurrentChildUID != nil {
			verifyCurrent = func() (bool, error) { return h.isCurrentChildUID(input) }
		}
		if err := h.repository.claimPendingDeletion(ctx, input.ChildModelKey, input.ChildModelUID, pending, verifyCurrent); err != nil {
			return newHfArtifactRetryResult(parent.Key, err), nil
		}
	}
	parent, found, err := h.repository.deletionParent(ctx, input.ChildModelKey, pending)
	if err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if h.repository.isChildMutationBlocked(input.ChildModelKey, pending.ModelUID) {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	// A different model may have attached this path while this receipt was
	// queued. The current parent snapshot and child lock fence that ownership.
	referenced, err := hfArtifactParentReferencesPath(parent, input)
	if err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if !referenced && h.hasOtherPathUsers != nil {
		referenced, err = h.hasOtherPathUsers(input)
		if err != nil {
			return newHfArtifactRetryResult(input.Parent.Key, err), nil
		}
	}
	if !referenced {
		if err := h.files.RemoveChildSymlink(input.ChildModelPath, pending.ParentPath); err != nil {
			return newHfArtifactRetryResult(input.Parent.Key, err), nil
		}
	}
	if found && len(parent.Children) == 0 && pending.ParentLockID != "" {
		if referenced {
			if err := h.releaseParentDeletionLock(ctx, parent, pending.ParentWasReady); err != nil {
				return newHfArtifactRetryResult(parent.Key, err), nil
			}
		} else {
			result, err := h.deleteLockedParent(ctx, parent, input.ModelStoreRoot, pending.ParentWasReady)
			if err != nil || result.Outcome != hfArtifactTaskDone {
				return result, err
			}
		}
	}
	if err := h.repository.finishPendingDeletion(ctx, input.ChildModelKey, *pending); err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
}

func hfArtifactParentReferencesPath(parent HfArtifactEntry, input hfArtifactTaskInput) (bool, error) {
	root, err := canonicalHfArtifactStoreRoot(input.ModelStoreRoot)
	if err != nil {
		return false, err
	}
	childPath, err := hfArtifactPathInRoot(input.ChildModelPath, input.ModelStoreRoot, root)
	if err != nil {
		return false, err
	}
	for _, path := range parent.Children {
		path, err := hfArtifactPathInRoot(path, input.ModelStoreRoot, root)
		if err != nil {
			return false, err
		}
		if path == childPath {
			return true, nil
		}
	}
	return false, nil
}

func (h *hfArtifactTaskHandler) resumeDeletionBeforeDownload(ctx context.Context, input hfArtifactTaskInput) (hfArtifactTaskResult, bool) {
	pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
	if err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), true
	}
	if pending == nil {
		return hfArtifactTaskResult{}, false
	}
	result, err := h.resumePendingDeletion(ctx, input, pending)
	if err != nil || result.Outcome == hfArtifactTaskDone {
		result = newHfArtifactRetryResult(input.Parent.Key, err)
	}
	return result, true
}
