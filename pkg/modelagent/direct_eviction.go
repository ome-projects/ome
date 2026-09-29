package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/ome/pkg/constants"
)

// DirectArtifactPendingDeletion owns only unfinished cleanup. Completion
// removes it atomically with Evicted; it is never retained as layout history.
type DirectArtifactPendingDeletion struct {
	ModelUID types.UID `json:"modelUID"`
	Path     string    `json:"path"`
}

func (s *Gopher) directEvictionPath(path string) (string, error) {
	root, err := canonicalHfArtifactStoreRoot(s.modelRootDir)
	if err != nil {
		return "", err
	}
	path, err = hfArtifactPathInRoot(path, s.modelRootDir, root)
	if err != nil {
		return "", err
	}
	if path == root {
		return "", fmt.Errorf("cannot evict the model store root")
	}
	relative, _ := filepath.Rel(root, path)
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == constants.ModelArtifactsDirectory || part == hfArtifactLockDirectory {
			return "", fmt.Errorf("Direct eviction path uses a reserved artifact directory")
		}
	}
	if err := validateHfArtifactPathAncestors(root, path, true); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Gopher) processDirectEviction(ctx context.Context, guard *artifactCleanupGuard, entry ModelEntry) (bool, error) {
	task := guard.task
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), types.UID(getModelUID(task))
	if entry.ModelUID != "" && entry.ModelUID != uid {
		return false, fmt.Errorf("Direct artifact belongs to another model UID")
	}
	spec := taskModelSpec(task)
	expected := DirectArtifactPendingDeletion{ModelUID: uid, Path: getDestPath(&spec, s.modelRootDir)}
	if entry.DirectArtifactPendingDeletion != nil {
		expected = *entry.DirectArtifactPendingDeletion
		if expected.ModelUID != uid || entry.NodeUID != s.artifactNodeUID {
			return false, fmt.Errorf("Direct cleanup model or Node UID changed")
		}
	}
	// Ordinary OCI downloads can be Ready even if metadata parsing failed,
	// so a Ready entry is ownership evidence without Config.
	if entry.Name == "" || entry.DirectArtifactPendingDeletion == nil && entry.Config == nil && entry.Status != ModelStatusReady {
		return false, fmt.Errorf("Direct eviction requires recorded model artifact ownership")
	}
	path, err := s.directEvictionPath(expected.Path)
	if err != nil {
		return false, err
	}
	expected.Path = path
	if entry.DirectArtifactPendingDeletion == nil && entry.Config != nil {
		for _, ownedPath := range entry.Config.Artifact.ParentPath {
			owned, err := s.directEvictionPath(ownedPath)
			if err != nil || owned != path {
				return false, fmt.Errorf("Direct artifact ownership path does not match destination")
			}
		}
	}
	guard.directReceipt = &expected
	acquired, err := s.tryLockDirectModelPath(ctx, path)
	if err != nil {
		return false, err
	}
	if !acquired {
		err := s.requeueTaskOnWait(task, gopherTaskWait)
		return err == nil, err
	}
	// The lock may have become available after another process completed.
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return false, err
	}
	current, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return false, err
	}
	if current.Status == ModelStatusEvicted && current.ModelUID == uid && current.DirectArtifactPendingDeletion == nil {
		return false, guard.withdrawReady(ctx)
	}
	if err := s.checkDirectEvictionUsers(ctx, key, path); err != nil {
		return false, err
	}
	if err := guard.withdrawReady(ctx); err != nil {
		return false, err
	}
	if err := s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if child.ModelUID != "" && child.ModelUID != uid || child.HfArtifactKey != "" || child.HfArtifactPendingDeletion != nil || child.Status == ModelStatusEvicted {
			return false, fmt.Errorf("Direct artifact ownership changed")
		}
		if child.DirectArtifactPendingDeletion != nil && (*child.DirectArtifactPendingDeletion != expected || child.NodeUID != s.artifactNodeUID) {
			return false, fmt.Errorf("Direct cleanup receipt changed")
		}
		child.DirectArtifactPendingDeletion = &expected
		child.ModelUID, child.NodeUID, child.Status, child.Progress = uid, s.artifactNodeUID, ModelStatusUpdating, nil
		return writeModelEntry(cm.Data, key, child)
	}); err != nil {
		return false, err
	}
	return false, s.finishDirectArtifactCleanup(ctx, guard, expected)
}

// Resume before ordinary Delete or source dispatch. The receipt's old path is
// authoritative even if the current CR points at another path or Local source.
func (s *Gopher) resumeDirectArtifactCleanup(ctx context.Context, task *GopherTask) (handled, waiting bool, err error) {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	cm, err := s.configMapReconciler.getConfigMapForRecovery(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, false, err
	}
	missing := cm == nil
	if cm != nil {
		_, found := cm.Data[key]
		missing = !found
	}
	if missing {
		// Final removal can fail after installing the normal deletion fences.
		// Such proof is deliberately excluded from restorable cache entries;
		// consume only its exact completed UID for Delete, without file work.
		if task.TaskType == Delete {
			c := s.configMapReconciler
			c.cacheMutex.RLock()
			_, evicted := c.evictedModels[key]
			var completed ModelEntry
			decoded := false
			if cached := c.modelCache[key]; evicted && cached != nil {
				decoded = json.Unmarshal([]byte(cached.ModelEntryJSON), &completed) == nil
			}
			c.cacheMutex.RUnlock()
			if decoded && completed.Status == ModelStatusEvicted && completed.ModelUID != "" && completed.ModelUID == types.UID(getModelUID(task)) && completed.HfArtifactKey == "" && completed.HfArtifactPendingDeletion == nil && completed.DirectArtifactPendingDeletion == nil {
				guard := &artifactCleanupGuard{gopher: s, task: task}
				ctx = context.WithValue(ctx, artifactCleanupGuardKey{}, guard)
				if err := guard.validate(ctx); err != nil {
					return true, false, err
				}
				return true, false, s.safeNodeLabelReconciliation(ctx, &NodeLabelOp{ModelStateOnNode: Deleted, BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
			}
		}
		// Missing server state cannot turn cached completed cleanup back into
		// ordinary deletion authority. Reuse serialized recovery, which fills
		// only absent entries and never overwrites newer ownership.
		cached, _ := s.configMapReconciler.cachedConfigMapEntries()
		entry, cacheErr := existingModelEntry(cached, key)
		if cacheErr != nil || entry.DirectArtifactPendingDeletion == nil && entry.Status != ModelStatusEvicted {
			return false, false, nil
		}
		if err := s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
			return s.configMapReconciler.restoreCachedConfigMapEntries(cm.Data, key, false)
		}); err != nil {
			return true, false, err
		}
		cm, err = s.configMapReconciler.getConfigMapForRecovery(ctx)
		if err != nil {
			return true, false, err
		}
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return false, false, err
	}
	pending := entry.DirectArtifactPendingDeletion
	completed := task.TaskType == Delete && entry.Status == ModelStatusEvicted && entry.HfArtifactKey == "" && entry.HfArtifactPendingDeletion == nil
	if pending == nil && !completed {
		return false, false, nil
	}
	if task.TaskType != Delete && !hasEvictableArtifactSource(task) {
		return true, false, fmt.Errorf("pending Direct cleanup requires a Hugging Face or OCI download source")
	}
	guard := &artifactCleanupGuard{gopher: s, task: task, directReceipt: pending}
	ctx = context.WithValue(ctx, artifactCleanupGuardKey{}, guard)
	if entry.ModelUID != types.UID(getModelUID(task)) || pending != nil && entry.NodeUID != s.artifactNodeUID {
		return true, false, fmt.Errorf("Direct cleanup model or Node UID changed")
	}
	if err := guard.validate(ctx); err != nil {
		return true, false, err
	}
	if pending != nil {
		if pending.ModelUID != entry.ModelUID {
			return true, false, fmt.Errorf("Direct cleanup receipt belongs to another model UID")
		}
		path, err := s.directEvictionPath(pending.Path)
		if err != nil {
			return true, false, err
		}
		if path != pending.Path {
			return true, false, fmt.Errorf("Direct cleanup receipt path is not canonical")
		}
		acquired, err := s.tryLockDirectModelPath(ctx, path)
		if err != nil {
			return true, false, err
		}
		if !acquired {
			err := s.requeueTaskOnWait(task, gopherTaskWait)
			return true, err == nil, err
		}
		if err := guard.withdrawReady(ctx); err != nil {
			return true, false, err
		}
		if err := s.finishDirectArtifactCleanup(ctx, guard, *pending); err != nil {
			return true, false, err
		}
	}
	if task.TaskType == Delete {
		err = s.safeNodeLabelReconciliation(ctx, &NodeLabelOp{ModelStateOnNode: Deleted, BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
	}
	return true, false, err
}

func (s *Gopher) finishDirectArtifactCleanup(ctx context.Context, guard *artifactCleanupGuard, expected DirectArtifactPendingDeletion) error {
	task := guard.task
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	// Local/PVC/vendor Delete keeps its existing non-destructive semantics,
	// even when the object used to be a Direct model at this same path.
	preserve := task.TaskType == Delete && (s.isReservingModelArtifact(task) || !hasEvictableArtifactSource(task))
	if !preserve {
		if err := s.checkDirectEvictionUsers(ctx, key, expected.Path); err != nil {
			return err
		}
		if _, err := s.directEvictionPath(expected.Path); err != nil {
			return err
		}
		// A receipt replacement or UID handoff must not pass the final destructive check.
		cm, err := s.configMapReconciler.getConfigMap(ctx)
		if err != nil {
			return err
		}
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return err
		}
		if entry.DirectArtifactPendingDeletion == nil || *entry.DirectArtifactPendingDeletion != expected || entry.ModelUID != expected.ModelUID || entry.NodeUID != s.artifactNodeUID || entry.HfArtifactKey != "" || entry.HfArtifactPendingDeletion != nil {
			return fmt.Errorf("Direct cleanup receipt changed")
		}
		if err := guard.validate(ctx); err != nil {
			return err
		}
		if err := s.deleteModel(expected.Path, task); err != nil {
			return err
		}
	}
	return s.completeDirectArtifactCleanup(ctx, task, expected)
}

// Called with the compatible file lock held by Direct and Shared writers.
// Lookup failures fail closed, but ordinary paths do not inherit eviction's
// managed-layout restrictions. Pending receipts also survive ConfigMap loss.
func (c *ConfigMapReconciler) checkDirectCleanupPath(ctx context.Context, path string) error {
	cm, err := c.getConfigMap(ctx)
	var data map[string]string
	if apierrors.IsNotFound(err) {
		data, _ = c.cachedConfigMapEntries()
	} else if err != nil {
		return err
	} else {
		data = cm.Data
		cached, _ := c.cachedConfigMapEntries()
		for key, raw := range cached {
			if _, present := data[key]; !present {
				if data == nil {
					data = make(map[string]string)
				}
				data[key] = raw
			}
		}
	}
	path, err = canonicalDirectModelPath(path)
	if err != nil {
		return err
	}
	paths := []string{path}
	// Writers can follow a leaf alias although deletion locks retain the link
	// itself. Keep both spellings, including a dangling link to pending bytes.
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		paths = append(paths, resolved)
	}
	if linked, err := readChildSymlinkTarget(path); err == nil {
		resolved, err := canonicalDirectModelPath(linked)
		if err != nil {
			return err
		}
		paths = append(paths, resolved)
	}
	for key := range data {
		if isHfArtifactConfigMapKey(key) {
			continue
		}
		entry, err := existingModelEntry(data, key)
		if err != nil {
			return err
		}
		pending := entry.DirectArtifactPendingDeletion
		if pending == nil {
			continue
		}
		guard, _ := ctx.Value(artifactCleanupGuardKey{}).(*artifactCleanupGuard)
		if guard != nil && guard.directReceipt != nil && *guard.directReceipt == *pending && key == getModelID(guard.task.BaseModel, guard.task.ClusterBaseModel) && entry.ModelUID == pending.ModelUID && entry.NodeUID == guard.gopher.artifactNodeUID {
			continue
		}
		owned, err := canonicalDirectModelPath(pending.Path)
		if err != nil {
			return err
		}
		for _, candidate := range paths {
			if hfArtifactInputPathWithin(owned, candidate) || hfArtifactInputPathWithin(candidate, owned) {
				return fmt.Errorf("path %s has unfinished Direct artifact cleanup", path)
			}
		}
	}
	return nil
}

func (s *Gopher) checkDirectEvictionUsers(ctx context.Context, key, path string) error {
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return err
	}
	if entry.Config != nil && len(entry.Config.Artifact.ChildrenPaths) != 0 {
		return fmt.Errorf("Direct artifact still records child paths")
	}
	for parentKey, raw := range cm.Data {
		if !isHfArtifactConfigMapKey(parentKey) {
			continue
		}
		parent, err := decodeHfArtifactEntry(parentKey, raw)
		if err != nil {
			return err
		}
		if _, linked := parent.Children[key]; linked {
			return fmt.Errorf("Direct model still has a Shared parent relationship")
		}
	}
	used, err := s.artifactPathUsers(ctx, key, path, s.modelRootDir, path, nil, false)
	if err != nil {
		return err
	}
	if !used {
		used, err = (hfArtifactFiles{}).HasChildren(path, s.modelRootDir)
	}
	if err != nil {
		return err
	}
	if used {
		return fmt.Errorf("Direct artifact path %s is still referenced", path)
	}
	return nil
}

func (s *Gopher) completeDirectArtifactCleanup(ctx context.Context, task *GopherTask, expected DirectArtifactPendingDeletion) error {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	return s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if child.DirectArtifactPendingDeletion == nil || *child.DirectArtifactPendingDeletion != expected || child.ModelUID != expected.ModelUID || child.NodeUID != s.artifactNodeUID || child.HfArtifactKey != "" || child.HfArtifactPendingDeletion != nil {
			return false, fmt.Errorf("Direct cleanup receipt changed")
		}
		child.DirectArtifactPendingDeletion = nil
		child.Status, child.Progress = ModelStatusEvicted, nil
		child.ArtifactRehydrationID = ""
		if child.Config != nil {
			child.Config.Artifact = Artifact{}
		}
		return writeModelEntry(cm.Data, key, child)
	})
}
