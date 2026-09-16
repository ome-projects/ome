package modelagent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// MarkChildrenFailedForRepair atomically saves prior statuses and fails all
// consistent referenced children before their shared files can be modified.
// A retry preserves the original snapshot, including across failed lock owners.
func (r *HfArtifactRepository) MarkChildrenFailedForRepair(ctx context.Context, expected HfArtifactEntry) (HfArtifactEntry, error) {
	if err := validateHfArtifactIdentityAndPath(expected); err != nil {
		return HfArtifactEntry{}, err
	}
	var result HfArtifactEntry
	err := r.configMaps.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		stored, err := requireStoredHfArtifactEntry(cm.Data, expected)
		if err != nil {
			return false, err
		}
		if err := requireHfArtifactRepairLock(stored, expected); err != nil {
			return false, err
		}
		if stored.ChildStatusesBeforeRepair == nil {
			stored.ChildStatusesBeforeRepair = make(map[string]ModelStatus)
		}
		changed := false
		for key := range stored.Children {
			child, matches, err := hfArtifactRepairChild(cm.Data, key, stored, expected)
			if err != nil {
				return false, err
			}
			if !matches || child.Status == ModelStatusDeleted {
				continue
			}
			// A different operation may have changed the child after a failed
			// repair. Preserve that new status, not an older repair's snapshot.
			if _, saved := stored.ChildStatusesBeforeRepair[key]; !saved || child.Status != ModelStatusFailed {
				stored.ChildStatusesBeforeRepair[key] = child.Status
			}
			child.Status = ModelStatusFailed
			childChanged, err := writeModelEntry(cm.Data, key, child)
			if err != nil {
				return false, err
			}
			changed = changed || childChanged
		}
		result = stored
		parentChanged, err := writeHfArtifactEntry(cm.Data, stored)
		return changed || parentChanged, err
	})
	return result, err
}

// ChildStatusesToRestore supplies the node-label callback with the same current
// relationship checks used by MarkReady. MarkReady checks again in its CAS.
func (r *HfArtifactRepository) ChildStatusesToRestore(ctx context.Context, expected HfArtifactEntry) (map[string]ModelStatus, error) {
	if err := validateHfArtifactIdentityAndPath(expected); err != nil {
		return nil, err
	}
	cm, err := r.configMaps.getConfigMap(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := requireStoredHfArtifactEntry(cm.Data, expected)
	if err != nil {
		return nil, err
	}
	if stored.Status == HfArtifactStatusReady && expected.LockID != "" && stored.LastCompletedLockID == expected.LockID {
		return nil, nil
	}
	if err := requireHfArtifactRepairLock(stored, expected); err != nil {
		return nil, err
	}
	return hfArtifactChildStatusesToRestore(cm.Data, stored, expected)
}

func requireHfArtifactRepairLock(stored, expected HfArtifactEntry) error {
	if stored.Status != HfArtifactStatusUpdating || expected.LockID == "" || stored.LockID != expected.LockID {
		return fmt.Errorf("Hugging Face artifact %s repair lock ID does not match current owner", stored.Key)
	}
	return nil
}

// hfArtifactChildStatusesToRestore uses the current ConfigMap, not cached child
// state. An unrelated status change or a replaced relationship is left alone.
func hfArtifactChildStatusesToRestore(data map[string]string, stored, expected HfArtifactEntry) (map[string]ModelStatus, error) {
	statuses := make(map[string]ModelStatus)
	for key, status := range stored.ChildStatusesBeforeRepair {
		child, matches, err := hfArtifactRepairChild(data, key, stored, expected)
		if err != nil {
			return nil, err
		}
		if matches && child.Status == ModelStatusFailed {
			statuses[key] = status
		}
	}
	return statuses, nil
}

// hfArtifactRepairChild requires both reference directions and the same child
// path seen by the lock owner before changing a child's status.
func hfArtifactRepairChild(data map[string]string, key string, stored, expected HfArtifactEntry) (ModelEntry, bool, error) {
	path := stored.Children[key]
	expectedPath := expected.Children[key]
	if strings.TrimSpace(path) == "" || strings.TrimSpace(expectedPath) == "" ||
		filepath.Clean(path) != filepath.Clean(expectedPath) {
		return ModelEntry{}, false, nil
	}
	if _, found := data[key]; !found {
		return ModelEntry{}, false, nil
	}
	child, err := existingModelEntry(data, key)
	if err != nil {
		return ModelEntry{}, false, fmt.Errorf("cannot read child for Hugging Face repair: %w", err)
	}
	return child, child.HfArtifactKey == stored.Key, nil
}
