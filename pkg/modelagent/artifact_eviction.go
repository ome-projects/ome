package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

func (w *Scout) enqueueArtifactEviction(task *GopherTask) bool {
	if !artifactEvictionRequested(task) {
		return false
	}
	task.TaskType = Evict
	w.gopherChan <- task
	return true
}

// evictDirectArtifact never consults workloads: it validates only the local
// artifact's identity, ownership, and write serialization. Shared eviction is
// handled separately and must not fall through to ordinary file deletion.
func (s *Gopher) evictDirectArtifact(ctx context.Context, task *GopherTask) error {
	if task.SharedArtifact || task.TaskType != Delete && !s.isBoundedDirectArtifactTask(task) {
		return fmt.Errorf("eviction requires a directly owned HF or OCI artifact")
	}
	if task.TaskType != Delete {
		skip, _, err := s.shouldSkipArtifactTask(ctx, task)
		if err != nil || skip {
			return err
		}
		if s.configMapReconciler != nil {
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			if err != nil {
				return err
			}
			entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
			if err == nil && entry.ModelUID == taskModelMeta(task).UID && entry.Status == ModelStatusEvicted && entry.DirectArtifactPendingEviction == nil {
				// Completed eviction owns no remaining cleanup. Another Model may
				// already have reused this path; duplicate intent is a no-op.
				return nil
			}
		}
	}
	path, err := s.directArtifactPath(task)
	if err != nil {
		return err
	}
	return s.cleanupDirectArtifact(ctx, task, path, nil)
}

// Eviction and restoration settle the same durable direct cleanup under the
// existing writer locks. A restoration receipt authorizes only its recorded
// path; otherwise the eviction request (or captured Delete receipt) does.
func (s *Gopher) cleanupDirectArtifact(ctx context.Context, task *GopherTask, path string, restoration *DirectArtifactPendingEviction) error {
	release, acquired, err := s.acquireDirectArtifactPathLock(path)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("artifact path %s has an active writer", path)
	}
	defer release()
	// Status publication uses the same in-process lock as ordinary writers.
	s.configMapMutex.Lock()
	defer s.configMapMutex.Unlock()
	validate := func() error { return s.validateArtifactCleanupRequest(ctx, task) }
	state := Deleted
	if restoration != nil {
		validate = func() error { return s.validateArtifactDownload(ctx, task) }
		state = Updating
	}
	validateCleanup := func() error {
		if restoration == nil {
			return s.validateDirectEviction(ctx, task, path)
		}
		if err := validate(); err != nil {
			return err
		}
		cm, err := s.configMapReconciler.getConfigMap(ctx)
		if err != nil {
			return err
		}
		if _, err := directRestorationEntry(cm.Data, task, *restoration); err != nil {
			return err
		}
		return s.validateDirectArtifactCleanup(ctx, task, path)
	}
	if err := validateCleanup(); err != nil {
		return err
	}
	if restoration == nil {
		if err := s.persistDirectEviction(ctx, task, path, false); err != nil {
			return err
		}
	}
	if s.nodeLabelReconciler == nil {
		return fmt.Errorf("eviction requires node readiness reconciliation")
	}
	op := &NodeLabelOp{ModelStateOnNode: state, BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, validateCurrent: validate}
	if err := s.nodeLabelReconciler.WithdrawReadiness(ctx, op); err != nil {
		return err
	}
	if err := validateCleanup(); err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove direct artifact %s: %w", path, err)
	}
	if restoration != nil {
		return s.finishDirectRestorationReceipt(ctx, task, *restoration)
	}
	if err := s.persistDirectEviction(ctx, task, path, true); err != nil {
		return err
	}
	if task.TaskType == Delete {
		return s.removeCompletedDirectEviction(ctx, task)
	}
	return nil
}

// Delete tasks for CR deletion or placement loss settle pending cleanup using
// its persisted path, never a newly edited spec path. Completed eviction needs
// only acknowledgement removal fenced by the UID and completed state.
func (s *Gopher) resumeDirectEvictionOnDelete(ctx context.Context, task *GopherTask) (bool, error) {
	if s.configMapReconciler == nil {
		return false, nil
	}
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		// Recover a locally cached receipt if the ConfigMap disappeared.
		if err = s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(*corev1.ConfigMap) (bool, error) { return false, nil }); err != nil {
			return true, err
		}
		cm, err = s.configMapReconciler.getConfigMap(ctx)
	}
	if apierrors.IsNotFound(err) {
		return task.directEvictionDelete != nil, nil
	}
	if err != nil {
		return true, err
	}
	entry, err := existingModelEntry(cm.Data, key)
	if captured := task.directEvictionDelete; captured != nil {
		// A retry retains only its original cleanup authority. Restoration or
		// another owner must never send it back through ordinary Delete.
		if cm.Data[key] == "" {
			return true, nil
		}
		if err != nil {
			return true, err
		}
		if entry.ModelUID != captured.ModelUID || entry.ArtifactRehydrationID != captured.ArtifactRehydrationID ||
			entry.HfArtifactKey != "" || entry.HfArtifactPendingDeletion != nil {
			return true, nil
		}
		if receipt := entry.DirectArtifactPendingEviction; receipt != nil {
			if captured.DirectArtifactPendingEviction == nil || *receipt != *captured.DirectArtifactPendingEviction {
				return true, nil
			}
		} else if !completedArtifactEviction(entry) {
			return true, nil
		}
	}
	if err == nil && (entry.HfArtifactPendingDeletion != nil || entry.HfArtifactKey != "") {
		return false, nil
	}
	if err != nil || entry.DirectArtifactPendingEviction == nil && entry.Status != ModelStatusEvicted {
		return false, nil
	}
	if task.directEvictionDelete == nil {
		task.directEvictionDelete = &entry
	}
	path := entry.DirectArtifactPath
	if receipt := entry.DirectArtifactPendingEviction; receipt != nil {
		if receipt.ModelUID != uid {
			return true, fmt.Errorf("pending eviction belongs to another Model UID")
		}
		path = receipt.Path
	}
	if uid == "" || entry.ModelUID != uid {
		return true, fmt.Errorf("eviction cleanup belongs to another Model UID")
	}
	if entry.DirectArtifactPendingEviction == nil {
		return true, s.removeCompletedDirectEviction(ctx, task)
	}
	path, err = ownedArtifactPath(s.modelRootDir, path)
	if err != nil {
		return true, err
	}
	return true, s.cleanupDirectArtifact(ctx, task, path, nil)
}

// Completed cleanup may race with same-UID restoration at another destination.
// Recheck the acknowledgement on every CAS, then retire its recovery cache.
func (s *Gopher) removeCompletedDirectEviction(ctx context.Context, task *GopherTask) error {
	c := s.configMapReconciler
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	err := c.mutateConfigMapWithModelUIDLocked(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
		if err := s.validateArtifactCleanupRequest(ctx, task); err != nil {
			return false, err
		}
		if cm.Data[key] == "" {
			return false, nil
		}
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if entry.ModelUID != uid || !completedArtifactEviction(entry) {
			return false, fmt.Errorf("completed eviction changed before acknowledgement removal")
		}
		if captured := task.directEvictionDelete; captured != nil && entry.ArtifactRehydrationID != captured.ArtifactRehydrationID {
			return false, fmt.Errorf("completed eviction request changed before acknowledgement removal")
		}
		delete(cm.Data, key)
		return true, nil
	})
	if err != nil {
		return err
	}
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	if isModelResourceDeleting(task.BaseModel, task.ClusterBaseModel) {
		c.invalidateModelUIDAndEvictCacheLocked(key, uid)
	} else {
		c.evictCachedModelLocked(key)
	}
	return nil
}

func (s *Gopher) validateArtifactCleanupRequest(ctx context.Context, task *GopherTask) error {
	if task.TaskType != Delete {
		return s.validateArtifactDownload(ctx, task)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.modelClient == nil {
		return fmt.Errorf("artifact cleanup requires a live model client")
	}
	var latest metav1.Object
	var err error
	if task.BaseModel != nil {
		latest, err = s.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(ctx, task.BaseModel.Name, metav1.GetOptions{})
	} else {
		latest, err = s.modelClient.OmeV1beta1().ClusterBaseModels().Get(ctx, task.ClusterBaseModel.Name, metav1.GetOptions{})
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// Placement loss also queues Delete while the same Model remains live.
	if latest.GetUID() != taskModelMeta(task).UID {
		return fmt.Errorf("artifact cleanup requires the matching Model UID")
	}
	return nil
}

func (s *Gopher) validateDirectEviction(ctx context.Context, task *GopherTask, path string) error {
	if err := s.validateArtifactCleanupRequest(ctx, task); err != nil {
		return err
	}
	if task.TaskType != Delete {
		current, err := s.directArtifactPath(task)
		if err != nil || path != current {
			return fmt.Errorf("eviction path changed: %s (%v)", path, err)
		}
	}
	return s.validateDirectArtifactCleanup(ctx, task, path)
}

// The caller owns the path-family lock and validates its current request. A
// restoration receipt authorizes this recorded path, not the new source path.
func (s *Gopher) validateDirectArtifactCleanup(ctx context.Context, task *GopherTask, path string) error {
	if info, err := os.Lstat(path); err == nil && !info.IsDir() {
		return fmt.Errorf("eviction requires a real directory: %s", path)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	// Removing a containing directory must not unlink stable lock inodes or
	// a nested shared store, even if no currently listed Model uses them.
	if err := filepath.WalkDir(path, func(current string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) && current == path {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Name() == hfArtifactLockDirectory || entry.Name() == constants.ModelArtifactsDirectory {
			return fmt.Errorf("eviction would remove reserved artifact directory %s", current)
		}
		return nil
	}); err != nil {
		return err
	}
	if s.configMapReconciler == nil || s.kubeClient == nil {
		return fmt.Errorf("eviction requires persisted ownership and node readiness")
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	if _, err := directEvictionEntry(cm.Data, task, path); err != nil {
		return err
	}
	// Other Models may share or contain this directory, including ordinary OCI
	// same-path reuse. A live list failure is not proof of exclusive ownership.
	used, err := s.liveArtifactReferenceMatches(ctx, task, cm.Data, func(reference string) (bool, error) {
		if reference == "" {
			return false, nil
		}
		return artifactReferenceUsesPath(reference, path)
	})
	if err != nil {
		return err
	}
	if used {
		return fmt.Errorf("artifact directory is also referenced by another model")
	}
	return nil
}

func directEvictionEntry(data map[string]string, task *GopherTask, path string) (ModelEntry, error) {
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID
	entry, err := existingModelEntry(data, key)
	if err != nil {
		return entry, err
	}
	if entry.ModelUID == "" || entry.ModelUID != uid {
		return entry, fmt.Errorf("eviction requires persisted ownership by UID %s", uid)
	}
	if captured := task.directEvictionDelete; captured != nil && entry.ArtifactRehydrationID != captured.ArtifactRehydrationID {
		return entry, fmt.Errorf("pending eviction request changed before Delete cleanup")
	}
	// Delete resumes the captured receipt's UID and path. A Ready owner with
	// the same UID may have restored this path before Delete acquired its lock.
	if task.TaskType == Delete && entry.DirectArtifactPendingEviction == nil {
		return entry, fmt.Errorf("pending eviction changed before Delete cleanup")
	}
	if receipt := entry.DirectArtifactPendingEviction; receipt != nil && (receipt.ModelUID != uid || receipt.Path != path) {
		return entry, fmt.Errorf("pending eviction belongs to another owner or path")
	}
	ownedPath := entry.DirectArtifactPath
	if ownedPath == "" && entry.DirectArtifactPendingEviction != nil {
		ownedPath = entry.DirectArtifactPendingEviction.Path
	}
	if ownedPath == "" && entry.Config != nil {
		ownedPath = entry.Config.Artifact.ParentPath[key]
	}
	if ownedPath != path {
		return entry, fmt.Errorf("eviction requires persisted ownership of path %s", path)
	}
	probe := entry
	probe.DirectArtifactPendingEviction = nil
	if err := ordinaryModelOwnerCanBeReplaced(probe, key); err != nil {
		return entry, err
	}
	if entry.Config != nil && len(entry.Config.Artifact.ParentPath) != 0 && filepath.Clean(entry.Config.Artifact.ParentPath[key]) != path {
		return entry, fmt.Errorf("persisted artifact path does not match eviction path")
	}
	for otherKey, raw := range data {
		if isHfArtifactConfigMapKey(otherKey) {
			parent, err := decodeHfArtifactEntry(otherKey, raw)
			if err != nil {
				return entry, err
			}
			if _, exists := parent.Children[key]; exists {
				return entry, fmt.Errorf("eviction target still has shared ownership")
			}
			continue
		}
		if otherKey == key {
			continue
		}
		other, err := existingModelEntry(data, otherKey)
		if err != nil {
			return entry, err
		}
		for _, reference := range persistedModelArtifactReferences(other, false) {
			if reference == "" {
				continue
			}
			used, err := artifactReferenceUsesPath(reference, path)
			if err != nil {
				return entry, fmt.Errorf("resolve artifact reference for %s: %w", otherKey, err)
			}
			if used {
				return entry, fmt.Errorf("another model references eviction path: %s", otherKey)
			}
		}
	}
	return entry, nil
}

func (s *Gopher) persistDirectEviction(ctx context.Context, task *GopherTask, path string, complete bool) error {
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID
	return s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
		if err := s.validateArtifactCleanupRequest(ctx, task); err != nil {
			return false, err
		}
		entry, err := directEvictionEntry(cm.Data, task, path)
		if err != nil {
			return false, err
		}
		entry.Progress = nil
		if complete {
			entry.Status = ModelStatusEvicted
			entry.DirectArtifactPath = ""
			entry.Config = nil
			entry.DirectArtifactPendingEviction = nil
		} else {
			entry.Status = ModelStatusEvicting
			entry.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: uid, Path: path}
		}
		return writeModelEntry(cm.Data, key, entry)
	})
}

func directRestorationEntry(data map[string]string, task *GopherTask, receipt DirectArtifactPendingEviction) (ModelEntry, error) {
	entry, err := directEvictionEntry(data, task, receipt.Path)
	if err == nil && (entry.DirectArtifactPendingEviction == nil || *entry.DirectArtifactPendingEviction != receipt) {
		err = fmt.Errorf("restoration cleanup receipt changed")
	}
	return entry, err
}

func (s *Gopher) finishDirectRestorationReceipt(ctx context.Context, task *GopherTask, receipt DirectArtifactPendingEviction) error {
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID
	return s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
		if err := s.validateArtifactDownload(ctx, task); err != nil {
			return false, err
		}
		entry, err := directRestorationEntry(cm.Data, task, receipt)
		if err != nil {
			return false, err
		}
		entry.DirectArtifactPendingEviction, entry.Config, entry.Progress = nil, nil, nil
		entry.Status, entry.DirectArtifactPath = ModelStatusUpdating, ""
		return writeModelEntry(cm.Data, key, entry)
	})
}
