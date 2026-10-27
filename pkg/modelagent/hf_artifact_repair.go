package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/ome/pkg/constants"
)

// handleDownloadOverride validates under the current parent's lock. Only proven
// invalid files cause children to fail; download owns replacing corrupt data.
func (h *hfArtifactTaskHandler) handleDownloadOverride(
	ctx context.Context,
	input hfArtifactTaskInput,
	validate hfArtifactValidateFunc,
	download hfArtifactDownloadFunc,
) (hfArtifactTaskResult, error) {
	unlock, acquired, err := h.tryArtifactOperation(input)
	if err != nil || !acquired {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	defer unlock()
	if err := h.retryPendingLockRelease(ctx, input.Parent.Key); err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if result, pending := h.resumeDeletionBeforeDownload(ctx, input); pending {
		return result, nil
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
	input.Parent = parent
	if found && parent.blocksNewOperation() &&
		!h.files.ParentReadyMarkerMatchesLock(parent) {
		return newHfArtifactRetryResult(parent.Key, nil), nil
	}
	if err := h.repository.rememberDownload(ctx, input); err != nil {
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	parent, acquired, err = h.repository.TryAcquireLockForRepair(ctx, parent)
	if err != nil {
		if acquired {
			// A lost response does not prove the repair lock was not committed.
			h.pendingLockReleases.Store(parent.Key, &parent)
		}
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if !acquired {
		// A matching marker is the owner's durable completion record, not
		// permission to validate or download while it may still be writing.
		return h.reuseParent(ctx, input, parent)
	}
	if validate == nil {
		return h.finishParentWithError(ctx, parent, fmt.Errorf("validation function for shared Hugging Face parent %s is nil", parent.Key))
	}
	valid, err := validate(parent.LocalPath)
	if err != nil {
		return h.finishParentWithError(ctx, parent, err)
	}
	if err := ctx.Err(); err != nil {
		return h.finishParentWithError(ctx, parent, err)
	}
	// Validation may be slow; a superseded child must not authorize subsequent
	// writes or attach to a replacement relationship using this task's path.
	if h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
		if err := h.releaseParentLock(ctx, parent, HfArtifactStatusUpdating); err != nil {
			return newHfArtifactRetryResult(parent.Key, err), nil
		}
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	if err := h.validateChildReference(ctx, input); err != nil {
		result, cause := h.finishParentWithError(ctx, parent, err)
		if cause != nil {
			return newHfArtifactRetryResult(parent.Key, cause), nil
		}
		return result, nil
	}
	if !valid {
		// Proven corruption invalidates known children even if unknown local
		// users subsequently prevent file replacement. Preserve prior statuses
		// so a later successful validation can restore them.
		failedParent, err := h.repository.MarkChildrenFailedForRepair(ctx, parent)
		if err != nil {
			return h.finishParentWithError(ctx, parent, err)
		}
		parent = failedParent
		if h.updateChildStatuses != nil && len(parent.ChildStatusesBeforeRepair) != 0 {
			statuses, err := h.repository.ChildStatusesToRestore(ctx, parent)
			if err != nil {
				return h.finishParentWithError(ctx, parent, err)
			}
			for key := range statuses {
				statuses[key] = ModelStatusFailed
			}
			if len(statuses) != 0 {
				if err := h.updateChildStatuses(ctx, statuses); err != nil {
					return h.finishParentWithError(ctx, parent, err)
				}
			}
		}
		// ConfigMap loss can leave local users absent from parent.Children.
		// Valid bytes can be adopted, but replacing invalid bytes would bypass
		// status/ownership checks for those unknown users. This also protects
		// retries after only some siblings have rebuilt their references.
		unrecorded, err := h.files.HasUnrecordedChildren(parent, input.ModelStoreRoot)
		if err != nil {
			return h.finishParentWithError(ctx, parent, err)
		}
		if unrecorded {
			return h.finishParentWithError(ctx, parent, fmt.Errorf("shared Hugging Face parent %s has unrecorded child symlinks; refusing to replace files", parent.Key))
		}
		if download == nil {
			return h.finishParentWithError(ctx, parent, fmt.Errorf("download function for shared Hugging Face parent %s is nil", parent.Key))
		}
		// Leave the old marker alone until validation and status updates have
		// succeeded. The download callback may now replace corrupt files.
		if err := os.Remove(filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName)); err != nil && !os.IsNotExist(err) {
			return h.finishParentWithError(ctx, parent, err)
		}
		if err := download(parent.LocalPath); err != nil {
			return h.finishParentWithError(ctx, parent, err)
		}
	}
	if err := h.files.WriteParentReadyMarker(parent); err != nil {
		return h.finishParentWithError(ctx, parent, err)
	}
	if err := h.markParentReady(ctx, parent); err != nil {
		// Keep Updating and this lock's marker, including when node-label
		// restoration failed. Either download path can retry finalization.
		return newHfArtifactRetryResult(parent.Key, err), nil
	}
	parent.Status = HfArtifactStatusReady
	parent.LockID = ""
	parent.ChildStatusesBeforeRepair = nil
	return h.attachChildToReadyParent(ctx, input, parent)
}

func (h *hfArtifactTaskHandler) markParentReady(ctx context.Context, parent HfArtifactEntry) error {
	if h.updateChildStatuses != nil && len(parent.ChildStatusesBeforeRepair) != 0 {
		statuses, err := h.repository.ChildStatusesToRestore(ctx, parent)
		if err != nil {
			return err
		}
		if len(statuses) != 0 {
			if err := h.updateChildStatuses(ctx, statuses); err != nil {
				return err
			}
		}
	}
	return h.repository.MarkReady(ctx, parent)
}
