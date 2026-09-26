package modelagent

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Shared cleanup always follows persisted ownership, including the old paths
// retained by a receipt. Task routing is only a serialization hint.
func (s *Gopher) processSharedArtifactEviction(ctx context.Context, task *GopherTask) (bool, bool, error) {
	if s.configMapReconciler == nil {
		return false, false, nil
	}
	retry := func(cause error) (bool, bool, error) {
		if err := ctx.Err(); err != nil {
			return true, false, err
		}
		// A withdrawn or superseded request must release the delete barrier.
		// Its durable receipt remains available to the next operation.
		if skip, err := s.shouldSkipArtifactTask(ctx, task); err == nil && skip {
			return true, false, nil
		}
		err := s.requeueHfArtifactTask(task, newHfArtifactRetryResult(gopherTaskModelKey(task), cause))
		return true, err == nil, err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return retry(err)
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	if cm.Data[key] == "" {
		return false, false, nil
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return retry(err)
	}
	if completedArtifactEviction(entry) && entry.ModelUID == taskModelMeta(task).UID {
		return true, false, nil
	}
	parent, found, err := parentForChildEntry(cm.Data, key, entry)
	if err != nil {
		return retry(err)
	}
	if !found {
		return false, false, nil
	}
	if skip, err := s.shouldSkipArtifactTask(ctx, task); err != nil || skip {
		if err != nil {
			return retry(err)
		}
		return true, false, nil
	}
	input, err := s.hfArtifactInputForChild(task, parent)
	if err != nil {
		return retry(err)
	}
	input.RetainDeletionReceipt = true
	s.guardSharedEvictionCleanup(task, &input)
	result, err := s.releaseHfArtifactChild(ctx, input, false)
	if err != nil {
		return retry(err)
	}
	if result.Outcome != hfArtifactTaskDone {
		return retry(result.RetryReason)
	}
	return true, false, nil
}

// The existing handler calls this before releasing its cleanup locks. Receipt
// removal and Evicted acknowledgement are one exact-receipt CAS.
func (s *Gopher) finishSharedEviction(ctx context.Context, task *GopherTask, input hfArtifactTaskInput, pending HfArtifactPendingDeletion) error {
	if err := s.validateSharedEvictionCleanup(ctx, task, input); err != nil {
		return err
	}
	if _, err := os.Lstat(input.ChildModelPath); !os.IsNotExist(err) {
		return fmt.Errorf("shared eviction child path still exists: %v", err)
	}
	return s.configMapReconciler.mutateConfigMapWithModelUID(ctx, input.ChildModelKey, taskModelMeta(task).UID, func(cm *corev1.ConfigMap) (bool, error) {
		if err := s.validateArtifactDownload(ctx, task); err != nil {
			return false, err
		}
		entry, err := sharedEvictionEntry(cm.Data, task, input)
		if err != nil {
			return false, err
		}
		if entry.HfArtifactPendingDeletion == nil || *entry.HfArtifactPendingDeletion != pending {
			return false, fmt.Errorf("shared eviction receipt changed before acknowledgement")
		}
		entry.HfArtifactPendingDeletion = nil
		entry.Status, entry.Config, entry.Progress = ModelStatusEvicted, nil, nil
		entry.DirectArtifactPath = ""
		return writeModelEntry(cm.Data, input.ChildModelKey, entry)
	})
}

func sharedEvictionEntry(data map[string]string, task *GopherTask, input hfArtifactTaskInput) (ModelEntry, error) {
	entry, err := existingModelEntry(data, input.ChildModelKey)
	if err != nil {
		return entry, err
	}
	uid := taskModelMeta(task).UID
	if uid == "" || entry.ModelUID != uid || entry.DirectArtifactPendingEviction != nil {
		return entry, fmt.Errorf("shared eviction requires matching persisted Model UID")
	}
	if pending := entry.HfArtifactPendingDeletion; pending != nil {
		if pending.ModelUID != uid || pending.ChildPath != input.ChildModelPath ||
			pending.ParentPath != input.Parent.LocalPath || pending.Identity != input.Parent.Identity || entry.HfArtifactKey != "" {
			return entry, fmt.Errorf("shared eviction receipt ownership changed")
		}
		return entry, nil
	}
	parent, found, err := parentForChildEntry(data, input.ChildModelKey, entry)
	if err != nil {
		return entry, err
	}
	if !found || parent.Key != input.Parent.Key || parent.LocalPath != input.Parent.LocalPath ||
		parent.Children[input.ChildModelKey] != input.ChildModelPath {
		return entry, fmt.Errorf("shared eviction ownership changed")
	}
	return entry, nil
}

func (s *Gopher) validateSharedEvictionCleanup(ctx context.Context, task *GopherTask, input hfArtifactTaskInput) error {
	if err := s.validateArtifactCleanupRequest(ctx, task); err != nil {
		return err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	if _, err := sharedEvictionEntry(cm.Data, task, input); err != nil {
		return err
	}
	used, err := s.sharedEvictionPathReferenced(ctx, task, input.ChildModelPath, true)
	if err != nil {
		return err
	}
	if used {
		return fmt.Errorf("shared child path remains referenced")
	}
	return nil
}

// Called after phase-entry validation while both operation locks are held,
// once per phase including retries.
func (s *Gopher) prepareSharedEviction(ctx context.Context, task *GopherTask, input hfArtifactTaskInput) error {
	key, uid := input.ChildModelKey, input.ChildModelUID
	err := s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
		if err := s.validateArtifactCleanupRequest(ctx, task); err != nil {
			return false, err
		}
		entry, err := sharedEvictionEntry(cm.Data, task, input)
		if err != nil {
			return false, err
		}
		entry.Status, entry.Progress = ModelStatusEvicting, nil
		return writeModelEntry(cm.Data, key, entry)
	})
	if err != nil {
		return err
	}
	if s.nodeLabelReconciler == nil {
		return fmt.Errorf("eviction requires node readiness reconciliation")
	}
	op := &NodeLabelOp{ModelStateOnNode: Deleted, BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel}
	op.validateCurrent = func() error { return s.validateArtifactCleanupRequest(ctx, task) }
	return s.nodeLabelReconciler.WithdrawReadiness(ctx, op)
}

func (s *Gopher) guardSharedEvictionCleanup(task *GopherTask, input *hfArtifactTaskInput) {
	input.validateDeletion = func(ctx context.Context) error { return s.validateSharedEvictionCleanup(ctx, task, *input) }
	input.prepareDeletion = func(ctx context.Context) error { return s.prepareSharedEviction(ctx, task, *input) }
	input.finishDeletion = func(ctx context.Context, pending HfArtifactPendingDeletion) error {
		return s.finishSharedEviction(ctx, task, *input, pending)
	}
	input.pathReferenced = func(ctx context.Context, path string) (bool, error) {
		if err := s.validateArtifactCleanupRequest(ctx, task); err != nil {
			return false, err
		}
		cm, err := s.configMapReconciler.getConfigMap(ctx)
		if err != nil {
			return false, err
		}
		if _, err := sharedEvictionEntry(cm.Data, task, *input); err != nil {
			return false, err
		}
		return s.sharedEvictionPathReferenced(ctx, task, path, path == input.ChildModelPath)
	}
}
