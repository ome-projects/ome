package modelagent

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Receipts retain file ownership independently of task kind or transient model
// status. Every production resumer must protect local borrowers under its
// existing operation locks. Evict acknowledges deletion; restoration withdraws
// any old readiness left behind by an interrupted eviction before cleanup.
func (s *Gopher) guardSharedArtifactCleanup(task *GopherTask, input *hfArtifactTaskInput) {
	input.validateDeletion = func(ctx context.Context) error {
		return s.validateSharedCleanupOwnership(ctx, task, *input)
	}
	input.prepareDeletion = func(ctx context.Context) error {
		if task.TaskType != Delete && artifactRestorationRequested(task) {
			return s.withdrawRestorationReadiness(ctx, task)
		}
		return nil
	}
	input.pathReferenced = func(ctx context.Context, path string) (bool, error) {
		if err := s.validateSharedCleanupOwnership(ctx, task, *input); err != nil {
			return false, err
		}
		used, err := s.sharedEvictionPathReferenced(ctx, task, path, path == input.ChildModelPath)
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

func (s *Gopher) validateSharedCleanupOwnership(ctx context.Context, task *GopherTask, input hfArtifactTaskInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if taskModelMeta(task) == nil || taskModelMeta(task).UID == "" || s.modelClient == nil {
		return fmt.Errorf("shared cleanup requires a live model UID")
	}
	if task.TaskType == Delete {
		// Ordinary Delete retains its existing semantics: an absent Model or
		// the same live UID may release its recorded files, never a replacement.
		var latest metav1.Object
		var err error
		if task.BaseModel != nil {
			latest, err = s.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(ctx, task.BaseModel.Name, metav1.GetOptions{})
		} else {
			latest, err = s.modelClient.OmeV1beta1().ClusterBaseModels().Get(ctx, task.ClusterBaseModel.Name, metav1.GetOptions{})
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil && latest.GetUID() != taskModelMeta(task).UID {
			return fmt.Errorf("shared cleanup Model UID changed")
		}
	} else if err := s.validateArtifactDownload(ctx, task); err != nil {
		return err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	if err != nil {
		return err
	}
	if pending := entry.HfArtifactPendingDeletion; pending != nil {
		if entry.HfArtifactKey != "" || pending.ModelUID == "" || pending.Identity != input.Parent.Identity ||
			pending.ParentPath != input.Parent.LocalPath || pending.ChildPath != input.ChildModelPath {
			return fmt.Errorf("shared cleanup receipt ownership changed")
		}
		// An old receipt UID is adopted only by the repository's existing
		// current-UID proof and exact-receipt CAS, while both locks are held.
		return nil
	}
	parent, found, err := parentForChildEntry(cm.Data, input.ChildModelKey, entry)
	if err != nil {
		return err
	}
	if entry.ModelUID != "" && entry.ModelUID != input.ChildModelUID || !found || parent.Key != input.Parent.Key ||
		parent.LocalPath != input.Parent.LocalPath || parent.Children[input.ChildModelKey] != input.ChildModelPath {
		return fmt.Errorf("shared cleanup ownership changed")
	}
	return nil
}
