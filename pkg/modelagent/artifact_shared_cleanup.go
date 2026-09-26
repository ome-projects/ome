package modelagent

import (
	"context"
	"fmt"
)

// Receipts retain file ownership independently of task kind or transient model
// status. Every production resumer must protect local borrowers under its
// existing operation locks. Evict acknowledges deletion; restoration withdraws
// any old readiness left behind by an interrupted eviction before cleanup.
func (s *Gopher) guardSharedArtifactCleanup(task *GopherTask, input *hfArtifactTaskInput) {
	input.validateDeletion = func(ctx context.Context) error {
		_, err := s.validateSharedCleanupOwnership(ctx, task, *input)
		return err
	}
	input.prepareDeletion = func(ctx context.Context) error {
		if task.TaskType != Delete && artifactRestorationRequested(task) {
			return s.withdrawRestorationReadiness(ctx, task)
		}
		return nil
	}
	input.pathReferenced = func(ctx context.Context, path string) (bool, error) {
		data, err := s.validateSharedCleanupOwnership(ctx, task, *input)
		if err != nil {
			return false, err
		}
		used, err := s.sharedEvictionPathReferenced(ctx, task, data, path, path == input.ChildModelPath)
		if err == nil && used && task.TaskType != Delete && artifactRestorationRequested(task) &&
			path == input.ChildModelPath && s.sharedHfArtifactHandler().files.IsChildLinkedToParent(path, input.Parent.LocalPath) {
			// A borrower of this old link prevents required cleanup. A link
			// already reattached by another owner is no longer our obligation.
			parent, found, lookupErr := s.sharedHfArtifactHandler().repository.Get(ctx, input.Parent.Identity)
			if lookupErr != nil {
				return false, lookupErr
			}
			reused := false
			if found {
				reused, err = hfArtifactParentReferencesPath(parent, *input)
			}
			if err == nil && !reused {
				return false, fmt.Errorf("restoration cleanup child link remains borrowed")
			}
		}
		return used, err
	}
}

func (s *Gopher) validateSharedCleanupOwnership(ctx context.Context, task *GopherTask, input hfArtifactTaskInput) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if taskModelMeta(task) == nil || taskModelMeta(task).UID == "" || s.modelClient == nil {
		return nil, fmt.Errorf("shared cleanup requires a live model UID")
	}
	if err := s.validateArtifactCleanupRequest(ctx, task); err != nil {
		return nil, err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return nil, err
	}
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	if err != nil {
		return nil, err
	}
	if pending := entry.HfArtifactPendingDeletion; pending != nil {
		if entry.HfArtifactKey != "" || pending.ModelUID == "" || pending.Identity != input.Parent.Identity ||
			pending.ParentPath != input.Parent.LocalPath || pending.ChildPath != input.ChildModelPath {
			return nil, fmt.Errorf("shared cleanup receipt ownership changed")
		}
		// An old receipt UID is adopted only by the repository's existing
		// current-UID proof and exact-receipt CAS, while both locks are held.
		return cm.Data, nil
	}
	parent, found, err := parentForChildEntry(cm.Data, input.ChildModelKey, entry)
	if err != nil {
		return nil, err
	}
	if entry.ModelUID != "" && entry.ModelUID != input.ChildModelUID || !found || parent.Key != input.Parent.Key ||
		parent.LocalPath != input.Parent.LocalPath || parent.Children[input.ChildModelKey] != input.ChildModelPath {
		return nil, fmt.Errorf("shared cleanup ownership changed")
	}
	return cm.Data, nil
}
