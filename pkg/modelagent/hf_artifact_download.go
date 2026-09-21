package modelagent

import (
	"context"
	"errors"
	"fmt"
)

// handleDownload checks the child, reads the parent, then either downloads an
// initial copy or reuses an existing one. Shared-parent repair is separate.
func (h *hfArtifactTaskHandler) handleDownload(
	ctx context.Context,
	input hfArtifactTaskInput,
	download hfArtifactDownloadFunc,
) (hfArtifactTaskResult, error) {
	unlock, acquired, err := h.tryArtifactOperation(input)
	if err != nil || !acquired {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	defer unlock()
	if err := h.retryPendingParentFailure(ctx, input.Parent.Key); err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	if err := h.validateChildReference(ctx, input); err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}

	parent, found, err := h.repository.Get(ctx, input.Parent.Identity)
	if err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if !found {
		parent = input.Parent
	}
	if err := input.validateFilesystemPaths(parent); err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if h.childPathConflictsWithParent(input.ChildModelPath, parent.LocalPath) {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskUseDefaultDownload}, nil
	}

	if !found || parent.Status == HfArtifactStatusFailed {
		return h.downloadParentAndAttachChild(ctx, input, parent, download)
	}
	return h.reuseParent(ctx, input, parent)
}

// downloadParentAndAttachChild handles missing parents and failed initial
// downloads. It may reset files only after checking children and acquiring the
// parent lock; parents still used by children need the separate repair flow.
func (h *hfArtifactTaskHandler) downloadParentAndAttachChild(
	ctx context.Context,
	input hfArtifactTaskInput,
	parent HfArtifactEntry,
	download hfArtifactDownloadFunc,
) (hfArtifactTaskResult, error) {
	root, err := input.modelStoreRoot()
	if err != nil {
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	if err := h.validateParentForInitialDownload(parent, root); err != nil {
		return newHfArtifactRetryResult(parent.Key, err), nil
	}

	parent, acquired, err := h.repository.TryAcquireLock(ctx, parent)
	if err != nil {
		if acquired {
			// The acquisition may have committed despite the error. Retain only
			// our attempted owner so retry can release it before acquiring again.
			h.pendingFailures.Store(parent.Key, &parent)
		}
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if !acquired {
		// Another task won the race. Reuse its completed files or wait; do not
		// reset its directory or start a second download.
		return h.reuseParent(ctx, input, parent)
	}
	// Acquisition returns current state, not the earlier snapshot. Recheck both
	// recorded and local children before resetting files under this lock.
	if err := h.validateParentForInitialDownload(parent, root); err != nil {
		return newHfArtifactRetryResult(parent.Key, errors.Join(err, h.markParentFailed(ctx, parent))), nil
	}

	if download == nil {
		return h.markLockedParentFailed(ctx, parent, fmt.Errorf("download function for shared Hugging Face parent %s is nil", parent.Key))
	}
	if err := h.files.ResetParentDirectory(parent.LocalPath); err != nil {
		return h.markLockedParentFailed(ctx, parent, err)
	}
	if err := download(parent.LocalPath); err != nil {
		return h.markLockedParentFailed(ctx, parent, err)
	}
	if err := h.files.WriteParentReadyMarker(parent); err != nil {
		return h.markLockedParentFailed(ctx, parent, err)
	}
	if err := h.markParentReady(ctx, parent); err != nil {
		// Keep the marker: a retry can finish this update without downloading again.
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	parent.Status = HfArtifactStatusReady
	parent.LockID = ""
	return h.attachChildToReadyParent(ctx, input, parent)
}

// validateParentForInitialDownload excludes parents that need shared-parent
// repair. Empty ConfigMap references alone do not prove there are no children.
func (h *hfArtifactTaskHandler) validateParentForInitialDownload(parent HfArtifactEntry, modelStoreRoot string) error {
	if len(parent.Children) != 0 {
		return fmt.Errorf("shared Hugging Face parent %s with children requires repair", parent.Key)
	}
	hasChildren, err := h.files.HasChildren(parent.LocalPath, modelStoreRoot)
	if err != nil {
		return err
	}
	if hasChildren {
		return fmt.Errorf("shared Hugging Face parent %s has local child symlinks without ConfigMap references", parent.Key)
	}
	return nil
}

// reuseParent never resets or downloads files. A matching marker can finish a
// download's pending Ready update; otherwise an Updating parent means wait.
func (h *hfArtifactTaskHandler) reuseParent(
	ctx context.Context,
	input hfArtifactTaskInput,
	parent HfArtifactEntry,
) (hfArtifactTaskResult, error) {
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
	if parent.Status != HfArtifactStatusReady {
		return newHfArtifactRetryResult(parent.Key, fmt.Errorf("shared Hugging Face parent %s has invalid status %q", parent.Key, parent.Status)), nil
	}
	return h.attachChildToReadyParent(ctx, input, parent)
}

// attachChildToReadyParent creates the local link, records both ConfigMap
// references atomically, then verifies the relationship. The link and ConfigMap
// writes are not atomic with each other.
func (h *hfArtifactTaskHandler) attachChildToReadyParent(
	ctx context.Context,
	input hfArtifactTaskInput,
	parent HfArtifactEntry,
) (hfArtifactTaskResult, error) {
	if !h.files.ParentReadyMarkerExists(parent) {
		// Normal reuse must not reset a parent whose existing children may need repair.
		return newHfArtifactRetryResult(parent.Key, fmt.Errorf("shared Hugging Face parent %s requires repair", parent.Key)), nil
	}
	if h.childPathConflictsWithParent(input.ChildModelPath, parent.LocalPath) {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskUseDefaultDownload}, nil
	}
	// The child may have been deleted or replaced while the parent downloaded.
	if h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	created, err := h.files.createChildSymlink(input.ChildModelPath, parent.LocalPath)
	if err != nil {
		if errors.Is(err, errHfArtifactChildPathConflict) {
			return hfArtifactTaskResult{Outcome: hfArtifactTaskUseDefaultDownload}, nil
		}
		return hfArtifactTaskResult{}, err
	}
	err = h.repository.AddModelReference(ctx, parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath)
	// Deletion or same-name recreation can race with the reference write.
	// The child path lock excludes replacement attaches until this task exits.
	// Roll back only the link this attempt created, never a pre-existing link.
	if h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
		if created && h.files.IsChildLinkedToParent(input.ChildModelPath, parent.LocalPath) {
			if err := h.files.RemoveChildSymlink(input.ChildModelPath, parent.LocalPath); err != nil {
				return newHfArtifactRetryResult(parent.Key, err), nil
			}
		}
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	if err != nil {
		// The write may have succeeded despite a lost response, and the link may
		// predate this task. Preserve it and confirm the reference on retry.
		return newHfArtifactRetryResult(parent.Key, err), nil
	}

	storedParent, found, err := h.repository.GetParentForChild(ctx, input.ChildModelKey)
	if err == nil && found && storedParent.Key == parent.Key {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	if err == nil {
		err = fmt.Errorf("child model %s parent reference was not recorded", input.ChildModelKey)
	}
	return newHfArtifactRetryResult(parent.Key, err), nil
}
