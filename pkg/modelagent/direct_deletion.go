package modelagent

import (
	"context"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/utils/storage"
)

// DirectPendingDeletion records a committed cleanup intent for an independent
// model directory, regardless of download source. File removal can be
// repeated after interruption; StatusPending means only publication remains.
// Shared references and Local/PVC storage retain their own cleanup semantics.
type DirectPendingDeletion struct {
	ModelUID  types.UID `json:"modelUID"`
	Path      string    `json:"path"`
	SourceURI string    `json:"sourceURI"`
	Reason    string    `json:"reason"`
	Phase     string    `json:"phase"`
}

const (
	directFilesPending  = "FilesPending"
	directStatusPending = "StatusPending"
)

func (s *Gopher) loadDirectDeletionRecord(ctx context.Context, task *GopherTask) (*DirectPendingDeletion, error) {
	cm, err := s.configMapReconciler.getConfigMapForArtifactCleanup(ctx)
	if err != nil {
		return nil, err
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	if _, exists := cm.Data[key]; !exists {
		return nil, nil
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return nil, err
	}
	if entry.DirectPendingDeletion == nil {
		return nil, nil
	}
	pending := *entry.DirectPendingDeletion
	if err := s.validateDirectDeletion(task, pending); err != nil {
		return nil, err
	}
	return &pending, nil
}

func (s *Gopher) validateDirectDeletion(task *GopherTask, pending DirectPendingDeletion) error {
	spec := taskModelSpec(task)
	if pending.ModelUID == "" || pending.ModelUID != types.UID(getModelUID(task)) || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return fmt.Errorf("Direct cleanup model UID changed or is absent")
	}
	path, err := canonicalModelPath(getDestPath(&spec, s.modelRootDir))
	if err != nil {
		return err
	}
	if path == string(filepath.Separator) || path != pending.Path || pending.SourceURI != *spec.Storage.StorageUri || (pending.Phase != directFilesPending && pending.Phase != directStatusPending) {
		return fmt.Errorf("Direct cleanup path, source or phase changed")
	}
	if s.isTaskModelReplaced(task) {
		return fmt.Errorf("Direct cleanup model has been replaced")
	}
	return nil
}

func (s *Gopher) rememberDirectDeletion(ctx context.Context, task *GopherTask) (*DirectPendingDeletion, error) {
	spec := taskModelSpec(task)
	kind, err := storage.GetStorageType(*spec.Storage.StorageUri)
	if err != nil {
		return nil, err
	}
	if kind != storage.StorageTypeOCI && kind != storage.StorageTypeHuggingFace {
		return nil, nil
	}
	if kind == storage.StorageTypeHuggingFace && !s.isModelRemovalConfirmed(task) {
		return nil, nil
	}
	path, err := canonicalModelPath(getDestPath(&spec, s.modelRootDir))
	if err != nil {
		return nil, err
	}
	pending := DirectPendingDeletion{ModelUID: types.UID(getModelUID(task)), Path: path, SourceURI: *spec.Storage.StorageUri, Reason: "Delete", Phase: directFilesPending}
	if task.NodeIneligible {
		pending.Reason = "PlacementChanged"
	}
	if isModelResourceDeleting(task.BaseModel, task.ClusterBaseModel) {
		pending.Reason = "ModelDeleted"
	}
	if err := s.validateDirectDeletion(task, pending); err != nil {
		return nil, err
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	err = s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, pending.ModelUID, func(cm *corev1.ConfigMap) (bool, error) {
		entry := ModelEntry{Name: directDeletionModelName(task), ModelUID: pending.ModelUID, Status: ModelStatusUpdating}
		if _, exists := cm.Data[key]; exists {
			var err error
			entry, err = existingModelEntry(cm.Data, key)
			if err != nil {
				return false, err
			}
		}
		if entry.ModelUID != "" && entry.ModelUID != pending.ModelUID {
			return false, fmt.Errorf("Direct cleanup entry belongs to another model UID")
		}
		if entry.HfArtifactKey != "" || entry.HfArtifactPendingDeletion != nil || entry.HfArtifactPendingDownload != nil {
			return false, fmt.Errorf("Direct cleanup cannot replace Shared ownership")
		}
		if entry.DirectPendingDeletion != nil {
			if err := s.validateDirectDeletion(task, *entry.DirectPendingDeletion); err != nil {
				return false, err
			}
			pending = *entry.DirectPendingDeletion
			return false, nil
		}
		entry.ModelUID = pending.ModelUID
		entry.Status = ModelStatusUpdating
		entry.DirectPendingDeletion = &pending
		return writeModelEntry(cm.Data, key, entry)
	})
	return &pending, err
}

func (s *Gopher) updateDirectDeletionRecord(ctx context.Context, task *GopherTask, pending *DirectPendingDeletion, removeEntry bool) error {
	expected := *pending
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	return s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, expected.ModelUID, func(cm *corev1.ConfigMap) (bool, error) {
		if err := s.validateDirectDeletion(task, expected); err != nil {
			return false, err
		}
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		if entry.ModelUID != expected.ModelUID || entry.DirectPendingDeletion == nil || *entry.DirectPendingDeletion != expected || entry.HfArtifactKey != "" || entry.HfArtifactPendingDeletion != nil || entry.HfArtifactPendingDownload != nil {
			return false, fmt.Errorf("Direct cleanup record changed before completion")
		}
		if removeEntry {
			delete(cm.Data, key)
			return true, nil
		}
		next := expected
		next.Phase = directStatusPending
		entry.DirectPendingDeletion = &next
		return writeModelEntry(cm.Data, key, entry)
	})
}

func directDeletionModelName(task *GopherTask) string {
	if task.BaseModel != nil {
		return task.BaseModel.Name
	}
	return task.ClusterBaseModel.Name
}
