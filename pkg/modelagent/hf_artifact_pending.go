package modelagent

import (
	"context"
	"fmt"
)

// retryPendingParentFailure finishes a failed cleanup after an API outage. It
// cannot release a different owner's lock. The pending pointer also fences an
// old retry from clearing a newer cleanup for the same identity.
func (h *hfArtifactTaskHandler) retryPendingParentFailure(ctx context.Context, key string) error {
	value, pending := h.pendingFailures.Load(key)
	if !pending {
		return nil
	}
	expected := value.(*HfArtifactEntry)
	stored, found, err := h.repository.Get(ctx, expected.Identity)
	if err != nil {
		return err
	}
	if found && stored.Status == HfArtifactStatusUpdating && stored.LockID == expected.LockID {
		if err := h.markParentFailed(ctx, *expected); err != nil {
			return fmt.Errorf("finish failed shared artifact operation: %w", err)
		}
	}
	// Missing, terminal, or superseded state no longer belongs to this cleanup.
	h.pendingFailures.CompareAndDelete(key, expected)
	return nil
}
