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
func (r *HfArtifactRepository) beginPendingDownloadDeletion(ctx context.Context, key string, expected HfArtifactPendingDownload) error {
	return r.configMaps.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
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

// cleanupHfArtifactPendingDownload uses the saved destination, never the Delete
// task's possibly changed CR. A matching retry keeps its intent and continues
// downloading. Changed sources and deletes enter the existing receipt workflow.
func (s *Gopher) cleanupHfArtifactPendingDownload(ctx context.Context, task *GopherTask, next *hfArtifactTaskInput, allowCleanup bool) (bool, error) {
	if s.configMapReconciler == nil || s.isOrdinaryArtifactTask(task) {
		return false, nil
	}
	h := s.sharedHfArtifactHandler()
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	pending, err := h.repository.pendingDownload(ctx, key)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(key, err))
	}
	// The proposed parent path is derived from this child; reuse may resolve to
	// another directory. rememberDownload still checks the actual parent path
	// after the repository lookup under the operation locks.
	if pending == nil || next != nil && pending.Identity == next.Parent.Identity &&
		pending.ChildPath == next.ChildModelPath && pending.ModelUID == next.ChildModelUID {
		return false, nil
	}
	if !allowCleanup {
		s.demoteToNormalPriority(task)
		return true, nil
	}
	receipt := pending.deletionReceipt()
	input := s.hfArtifactInputForChild(task, receipt.parentForChild(key))
	unlock, acquired, err := h.tryArtifactOperation(input)
	if err != nil || !acquired {
		return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(input.Parent.Key, err))
	}
	err = h.repository.beginPendingDownloadDeletion(ctx, key, *pending)
	unlock()
	if err != nil {
		return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(input.Parent.Key, err))
	}
	_, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
	return waiting, err
}
