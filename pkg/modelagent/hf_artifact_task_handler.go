package modelagent

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// hfArtifactTaskOutcome tells the caller what to do next. The handler neither
// enqueues retries nor invokes the default download path.
type hfArtifactTaskOutcome string

const (
	// Done also covers stale child tasks that must stop without further changes.
	hfArtifactTaskDone               hfArtifactTaskOutcome = "Done"
	hfArtifactTaskRetry              hfArtifactTaskOutcome = "Retry"
	hfArtifactTaskUseDefaultDownload hfArtifactTaskOutcome = "UseDefaultDownload"
)

// hfArtifactTaskResult applies when the handler returns no error.
type hfArtifactTaskResult struct {
	Outcome hfArtifactTaskOutcome
	// RetryParentKey identifies the parent to wait for or retry. RetryReason is
	// optional; waiting for another lock owner is not itself an error.
	RetryParentKey string
	RetryReason    error
}

// hfArtifactTaskInput identifies one child and its expected parent. It is not
// persisted state. Handlers leave it unchanged and load current parent state
// from the repository.
type hfArtifactTaskInput struct {
	Parent HfArtifactEntry

	// ChildModelKey identifies the child ConfigMap entry, e.g. default.basemodel.model-1.
	ChildModelKey string
	// ChildModelUID identifies the child CR instance, not the shared parent.
	// UID checks reject work from a deleted or superseded same-name CR.
	ChildModelUID types.UID
	// ChildModelPath is the child's symlink path, not the parent directory.
	ChildModelPath string

	// ModelStoreRoot must contain every child path that could use this parent,
	// e.g. /models. Scans also find child symlinks
	// whose ConfigMap references were never recorded.
	ModelStoreRoot string
}

func (input hfArtifactTaskInput) modelStoreRoot() (string, error) {
	root := strings.TrimSpace(input.ModelStoreRoot)
	if root == "" {
		return "", errors.New("model store root is required for shared Hugging Face artifacts")
	}
	return root, nil
}

// hfArtifactDownloadFunc must return nil only after downloading and validating
// the parent files. The handler trusts that result when writing its ready marker.
type hfArtifactDownloadFunc func(parentPath string) error

// hfArtifactTaskHandler coordinates local files and persisted relationships.
// Those operations are separate; they are not one filesystem/ConfigMap transaction.
type hfArtifactTaskHandler struct {
	repository *HfArtifactRepository
	files      hfArtifactFiles
}

func newHfArtifactTaskHandler(repository *HfArtifactRepository) *hfArtifactTaskHandler {
	return &hfArtifactTaskHandler{repository: repository}
}

// validateChildReference requires an existing child entry before local changes.
// Its parent reference may be empty for a first attachment; an existing reference
// must match the expected parent and path.
func (h *hfArtifactTaskHandler) validateChildReference(ctx context.Context, input hfArtifactTaskInput) error {
	if strings.TrimSpace(input.ChildModelKey) == "" || isHfArtifactConfigMapKey(input.ChildModelKey) {
		return fmt.Errorf("invalid child model ConfigMap key %q", input.ChildModelKey)
	}
	configMap, err := h.repository.configMaps.getConfigMap(ctx)
	if err != nil {
		return err
	}
	child, err := existingModelEntry(configMap.Data, input.ChildModelKey)
	if err != nil {
		return err
	}
	parent, found, err := parentForChildEntry(configMap.Data, input.ChildModelKey, child)
	if err != nil || !found {
		return err
	}
	if parent.Key != input.Parent.Key {
		return fmt.Errorf("child model %s already references shared Hugging Face parent %s", input.ChildModelKey, parent.Key)
	}
	return input.validateStoredChildPath(parent)
}

// validateStoredChildPath prevents old tasks from acting on a replacement path.
func (input hfArtifactTaskInput) validateStoredChildPath(parent HfArtifactEntry) error {
	storedChildPath, found := parent.Children[input.ChildModelKey]
	if !found || filepath.Clean(storedChildPath) != filepath.Clean(input.ChildModelPath) {
		return fmt.Errorf("child model %s path does not match its recorded shared parent path", input.ChildModelKey)
	}
	return nil
}

// childPathConflictsWithParent preserves real directories and unrelated links;
// only an absent path or an existing link to this parent is eligible for reuse.
func (h *hfArtifactTaskHandler) childPathConflictsWithParent(childPath, parentPath string) bool {
	return h.files.ChildPathExists(childPath) && !h.files.IsChildLinkedToParent(childPath, parentPath)
}

// markLockedParentFailed releases this owner's lock by publishing Failed.
// If the state update fails, return Retry with both errors rather than hiding it.
func (h *hfArtifactTaskHandler) markLockedParentFailed(
	ctx context.Context,
	parent HfArtifactEntry,
	cause error,
) (hfArtifactTaskResult, error) {
	if err := h.repository.MarkFailed(ctx, parent); err != nil {
		return newHfArtifactRetryResult(parent.Key, errors.Join(cause, err)), nil
	}
	return hfArtifactTaskResult{}, cause
}

func newHfArtifactRetryResult(parentKey string, reason error) hfArtifactTaskResult {
	return hfArtifactTaskResult{Outcome: hfArtifactTaskRetry, RetryParentKey: parentKey, RetryReason: reason}
}
