package modelagent

import (
	"context"
	"fmt"
)

// handleDelete removes this child's references and symlink. Only the final
// recorded child acquires the lock needed to delete parent files.
func (h *hfArtifactTaskHandler) handleDelete(ctx context.Context, input hfArtifactTaskInput) (hfArtifactTaskResult, error) {
	unlock, acquired := h.tryParentOperation(input.Parent.Key)
	if !acquired {
		return newHfArtifactRetryResult(input.Parent.Key, nil), nil
	}
	defer unlock()
	if err := h.retryPendingParentFailure(ctx, input.Parent.Key); err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	parent, found, err := h.repository.GetParentForChild(ctx, input.ChildModelKey)
	if err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if !found {
		return h.removeUnreferencedChildSymlink(ctx, input)
	}
	if parent.Key != input.Parent.Key {
		// The caller must rebuild its input before acting on a different parent.
		// We hold only the expected parent's operation lock, not this new one.
		return newHfArtifactRetryResult(parent.Key, fmt.Errorf("child model %s parent reference changed", input.ChildModelKey)), nil
	}
	if err := input.validateStoredChildPath(parent); err != nil {
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	if parent.Status == HfArtifactStatusUpdating {
		if !h.files.ParentReadyMarkerMatchesLock(parent) {
			return newHfArtifactRetryResult(parent.Key, nil), nil
		}
		if err := h.markParentReady(ctx, parent); err != nil {
			return newHfArtifactRetryResult(parent.Key, err), nil
		}
		parent.Status = HfArtifactStatusReady
		parent.LockID = ""
	}
	// Validate the scan root before removing references: that removal may grant
	// this task the parent deletion lock, which it must be able to finish.
	root, err := input.modelStoreRoot()
	if err != nil {
		return newHfArtifactRetryResult(parent.Key, err), nil
	}

	// Remove references first so the repository can reject a stale UID or path
	// before this task touches the child symlink.
	removal, err := h.repository.RemoveModelReference(
		ctx, parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath,
	)
	if err != nil {
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	if !removal.ReferenceRemoved {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	if err := h.files.RemoveChildSymlink(input.ChildModelPath, parent.LocalPath); err != nil {
		if removal.LastReferenceRemoved {
			return h.markLockedParentFailed(ctx, removal.Artifact, err)
		}
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	if !removal.LastReferenceRemoved {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	return h.deleteLockedParent(ctx, removal.Artifact, root, parent.Status == HfArtifactStatusReady)
}

// removeUnreferencedChildSymlink removes only a matching local link. Without a
// recorded relationship, this task cannot claim ownership of parent deletion.
func (h *hfArtifactTaskHandler) removeUnreferencedChildSymlink(ctx context.Context, input hfArtifactTaskInput) (hfArtifactTaskResult, error) {
	parent, found, err := h.repository.Get(ctx, input.Parent.Identity)
	if err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if !found {
		parent = input.Parent
	} else {
		// A one-sided record requires reconciliation, not removal of the local link.
		if _, referencesChild := parent.Children[input.ChildModelKey]; referencesChild {
			return newHfArtifactRetryResult(parent.Key, fmt.Errorf("child model %s and shared Hugging Face parent %s do not contain matching references", input.ChildModelKey, parent.Key)), nil
		}
		if parent.Status == HfArtifactStatusUpdating {
			return newHfArtifactRetryResult(parent.Key, nil), nil
		}
	}
	if err := h.files.RemoveChildSymlink(input.ChildModelPath, parent.LocalPath); err != nil {
		return hfArtifactTaskResult{}, err
	}
	return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
}

// deleteLockedParent requires the deletion LockID returned by the last
// reference removal and a previously validated modelStoreRoot.
func (h *hfArtifactTaskHandler) deleteLockedParent(
	ctx context.Context,
	parent HfArtifactEntry,
	modelStoreRoot string,
	wasReady bool,
) (hfArtifactTaskResult, error) {
	hasChildren, err := h.files.HasChildren(parent.LocalPath, modelStoreRoot)
	if err != nil {
		return h.markLockedParentFailed(ctx, parent, fmt.Errorf("scan child symlinks for shared Hugging Face parent %s: %w", parent.Key, err))
	}
	if hasChildren {
		// Preserve files for an unrecorded child and end deletion ownership.
		// Reconstructing the missing references is separate recovery work.
		if err := h.releaseParentDeletionLock(ctx, parent, wasReady); err != nil {
			return newHfArtifactRetryResult(parent.Key, err), nil
		}
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	if err := h.files.RemoveParentDirectory(parent.LocalPath); err != nil {
		return h.markLockedParentFailed(ctx, parent, err)
	}
	deleted, err := h.repository.DeleteIfUnreferenced(ctx, parent)
	if err != nil {
		return h.markLockedParentFailed(ctx, parent, err)
	}
	if !deleted {
		return h.markLockedParentFailed(ctx, parent, fmt.Errorf("shared Hugging Face parent %s changed during deletion", parent.Key))
	}
	return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
}

func (h *hfArtifactTaskHandler) releaseParentDeletionLock(ctx context.Context, parent HfArtifactEntry, wasReady bool) error {
	if wasReady && h.files.ParentReadyMarkerExists(parent) {
		return h.repository.MarkReady(ctx, parent)
	}
	return h.repository.MarkFailed(ctx, parent)
}
