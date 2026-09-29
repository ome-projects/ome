package modelagent

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// HfArtifactPendingDownload preserves the original destination until attachment
// succeeds. It is an intent, not a Ready reference or permission to delete files.
type HfArtifactPendingDownload struct {
	Identity   HfArtifactIdentity `json:"identity"`
	ParentPath string             `json:"parentPath"`
	ChildPath  string             `json:"childPath"`
	ModelUID   types.UID          `json:"modelUID"`
}

func pendingHfArtifactDownload(input hfArtifactTaskInput) HfArtifactPendingDownload {
	return HfArtifactPendingDownload{Identity: input.Parent.Identity, ParentPath: input.Parent.LocalPath,
		ChildPath: input.ChildModelPath, ModelUID: input.ChildModelUID}
}

func (pending HfArtifactPendingDownload) deletionReceipt() HfArtifactPendingDeletion {
	return HfArtifactPendingDeletion{Identity: pending.Identity, ParentPath: pending.ParentPath,
		ChildPath: pending.ChildPath, ModelUID: pending.ModelUID}
}

// rememberDownload runs under both filesystem locks, before any parent or child
// writes. A failed/lost response prevents file work; retry confirms the same intent.
func (r *HfArtifactRepository) rememberDownload(ctx context.Context, input hfArtifactTaskInput) error {
	expected := pendingHfArtifactDownload(input)
	return r.configMaps.mutateConfigMapWithModelUID(ctx, input.ChildModelKey, input.ChildModelUID, func(cm *corev1.ConfigMap) (bool, error) {
		if r.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
			return false, fmt.Errorf("cannot record shared download for stale child %s", input.ChildModelKey)
		}
		child, err := existingModelEntry(cm.Data, input.ChildModelKey)
		if err != nil {
			return false, err
		}
		if child.DirectArtifactPendingDeletion != nil {
			return false, fmt.Errorf("model %s has unfinished Direct artifact cleanup", input.ChildModelKey)
		}
		if child.HfArtifactPendingDeletion != nil {
			return false, fmt.Errorf("model %s has unfinished shared artifact deletion", input.ChildModelKey)
		}
		if child.HfArtifactKey != "" {
			// Established references already preserve the original identity/path.
			_, _, err := parentForChildEntry(cm.Data, input.ChildModelKey, child)
			return false, err
		}
		if child.HfArtifactPendingDownload != nil && *child.HfArtifactPendingDownload != expected {
			return false, fmt.Errorf("model %s has another pending shared download", input.ChildModelKey)
		}
		child.HfArtifactPendingDownload = &expected
		return writeModelEntry(cm.Data, input.ChildModelKey, child)
	})
}

func (r *HfArtifactRepository) pendingDownload(ctx context.Context, key string) (*HfArtifactPendingDownload, error) {
	cm, err := r.configMaps.getConfigMapForHfArtifactCleanup(ctx)
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
	if child.HfArtifactPendingDownload != nil && (child.HfArtifactKey != "" || child.HfArtifactPendingDeletion != nil) {
		return nil, fmt.Errorf("model %s has conflicting shared artifact ownership", key)
	}
	return child.HfArtifactPendingDownload, nil
}

// beginPendingDownloadDeletion converts only the exact intent read by the
// caller. Both filesystem locks must be held. The normal deletion path later
// claims a durable parent LockID and verifies UID handoff before touching files.
// UID handoffs must prove current ownership before even creating a receipt.
func (r *HfArtifactRepository) beginPendingDownloadDeletion(ctx context.Context, key string, uid types.UID, expected HfArtifactPendingDownload, verifyCurrent func() (bool, error)) error {
	return r.configMaps.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		if uid != expected.ModelUID || r.isChildMutationBlocked(key, uid) {
			if uid == "" || verifyCurrent == nil {
				return false, fmt.Errorf("current lister UID proof is required for pending shared download cleanup of %s", key)
			}
			r.configMaps.cacheMutex.RLock()
			invalidated := r.configMaps.isModelUIDInvalidatedLocked(key, uid)
			r.configMaps.cacheMutex.RUnlock()
			current, err := verifyCurrent()
			if err != nil {
				return false, err
			}
			if !current || invalidated {
				return false, fmt.Errorf("cannot prove current UID for pending shared download cleanup of %s", key)
			}
		}
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		receipt := expected.deletionReceipt()
		if child.HfArtifactPendingDownload == nil && child.HfArtifactKey == "" &&
			child.HfArtifactPendingDeletion != nil && *child.HfArtifactPendingDeletion == receipt {
			return false, nil
		}
		if child.HfArtifactKey != "" || child.HfArtifactPendingDeletion != nil ||
			child.HfArtifactPendingDownload == nil || *child.HfArtifactPendingDownload != expected {
			return false, fmt.Errorf("pending shared download for %s changed", key)
		}
		child.HfArtifactPendingDownload = nil
		child.HfArtifactPendingDeletion = &receipt
		return writeModelEntry(cm.Data, key, child)
	})
}

// A sibling between download and attachment still needs the parent. Intent
// writes take the same parent filesystem lock as deletion, so this read is fenced.
func (r *HfArtifactRepository) hasPendingDownloads(ctx context.Context, parent HfArtifactEntry) (bool, error) {
	cm, err := r.configMaps.getConfigMap(ctx)
	if err != nil {
		return false, err
	}
	for key := range cm.Data {
		if isHfArtifactConfigMapKey(key) {
			continue
		}
		child, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if pending := child.HfArtifactPendingDownload; pending != nil &&
			pending.Identity == parent.Identity && pending.ParentPath == parent.LocalPath {
			return true, nil
		}
	}
	return false, nil
}

// reconcileHfArtifactCleanup completes pending cleanup before a caller handles
// live references or downloads. A matching retry keeps its download intent;
// changed targets and deletes convert it into the existing deletion workflow.
// A nil next means the caller is deleting or leaving shared storage, not that
// revision resolution is still pending. Delete uses handled to avoid falling
// through to ordinary file removal after shared cleanup has completed.
func (s *Gopher) reconcileHfArtifactCleanup(ctx context.Context, task *GopherTask, next *hfArtifactTaskInput, allowCleanup bool) (handled, waiting bool, err error) {
	if s.configMapReconciler == nil || s.isOrdinaryArtifactTask(task) {
		return s.resumeHfArtifactChildDeletion(ctx, task, allowCleanup)
	}
	h := s.sharedHfArtifactHandler()
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	pending, err := h.repository.pendingDownload(ctx, key)
	if ctx.Err() != nil {
		return true, false, ctx.Err()
	}
	if apierrors.IsNotFound(err) {
		return s.resumeHfArtifactChildDeletion(ctx, task, allowCleanup)
	}
	if err != nil {
		return true, true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(key, err))
	}
	// The proposed parent path is derived from this child; reuse may resolve to
	// another directory. rememberDownload still checks the actual parent path
	// after the repository lookup under the operation locks.
	if pending == nil || next != nil && pending.Identity == next.Parent.Identity &&
		pending.ChildPath == next.ChildModelPath && pending.ModelUID == next.ChildModelUID {
		return s.resumeHfArtifactChildDeletion(ctx, task, allowCleanup)
	}
	if !allowCleanup {
		s.demoteToNormalPriority(task)
		return true, true, nil
	}
	receipt := pending.deletionReceipt()
	input := s.hfArtifactInputForChild(task, receipt.parentForChild(key))
	unlock, acquired, err := h.tryArtifactOperation(ctx, input)
	if err != nil || !acquired {
		return true, true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(input.Parent.Key, err))
	}
	var verifyCurrent func() (bool, error)
	if h.isCurrentChildUID != nil {
		verifyCurrent = func() (bool, error) { return h.isCurrentChildUID(input) }
	}
	err = h.repository.beginPendingDownloadDeletion(ctx, key, input.ChildModelUID, *pending, verifyCurrent)
	unlock()
	if err != nil {
		return true, true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(input.Parent.Key, err))
	}
	return s.resumeHfArtifactChildDeletion(ctx, task, allowCleanup)
}
