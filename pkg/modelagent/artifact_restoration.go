package modelagent

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Settle durable cleanup before selecting the new source or destination. The
// task's Model snapshot captures the request; every mutation reuses its live
// UID, annotation and input validation, never ordinary Delete authority.
func (s *Gopher) prepareArtifactRestoration(ctx context.Context, task *GopherTask, allowCleanup bool) (bool, error) {
	if !artifactRestorationRequested(task) {
		return false, nil
	}
	retry := func(err error) (bool, error) {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(gopherTaskModelKey(task), err))
	}
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		return retry(err)
	}
	if s.configMapReconciler == nil {
		return retry(fmt.Errorf("restoration requires persisted local ownership"))
	}
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		err = s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(*corev1.ConfigMap) (bool, error) {
			return false, s.validateArtifactDownload(ctx, task)
		})
		if err == nil {
			cm, err = s.configMapReconciler.getConfigMap(ctx)
		}
	}
	if err != nil {
		return retry(err)
	}
	if cm.Data[key] == "" {
		return false, nil
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return retry(err)
	}
	if entry.HfArtifactPendingDeletion != nil {
		_, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, allowCleanup)
		return waiting, err
	}
	if receipt := entry.DirectArtifactPendingEviction; receipt != nil {
		if !allowCleanup {
			s.demoteToNormalPriority(task)
			return true, nil
		}
		if err := s.settleDirectRestorationReceipt(ctx, task, *receipt); err != nil {
			return retry(err)
		}
		return false, nil
	}
	if entry.ModelUID == uid && completedArtifactEviction(entry) {
		err := s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
			if err := s.validateArtifactDownload(ctx, task); err != nil {
				return false, err
			}
			current, err := existingModelEntry(cm.Data, key)
			if err != nil {
				return false, err
			}
			if current.ModelUID != uid || !completedArtifactEviction(current) {
				return false, fmt.Errorf("completed eviction changed during restoration")
			}
			current.Status, current.DirectArtifactPath = ModelStatusUpdating, ""
			return writeModelEntry(cm.Data, key, current)
		})
		if err != nil {
			return retry(err)
		}
	}
	return false, nil
}

func (s *Gopher) settleDirectRestorationReceipt(ctx context.Context, task *GopherTask, receipt DirectArtifactPendingEviction) error {
	path, err := ownedArtifactPath(s.modelRootDir, receipt.Path)
	if err != nil || receipt.ModelUID != taskModelMeta(task).UID {
		return fmt.Errorf("restoration cleanup has invalid path or Model UID: %v", err)
	}
	return s.cleanupDirectArtifact(ctx, task, path, &receipt)
}
