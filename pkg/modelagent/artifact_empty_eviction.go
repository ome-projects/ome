package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// A missing or initial Updating entry is not deletion authority. Only the
// absence of its local path permits acknowledgement without deleting files.
func unmaterializedArtifactEntry(data map[string]string, key string, uid types.UID) (bool, error) {
	if _, found := data[key]; found {
		entry, err := existingModelEntry(data, key)
		if err != nil {
			return false, err
		}
		if entry.ModelUID != "" && entry.ModelUID != uid {
			return false, fmt.Errorf("eviction model entry belongs to another model UID")
		}
		if entry.Status != ModelStatusUpdating || entry.Config != nil || entry.HfArtifactKey != "" || entry.HfArtifactPendingDownload != nil || entry.HfArtifactPendingDeletion != nil || entry.Progress != nil {
			return false, nil
		}
	}
	for parentKey, raw := range data {
		if !isHfArtifactConfigMapKey(parentKey) {
			continue
		}
		parent, err := decodeHfArtifactEntry(parentKey, raw)
		if err != nil {
			return false, err
		}
		if _, found := parent.Children[key]; found {
			return false, nil
		}
	}
	return true, nil
}

func (s *Gopher) evictUnmaterializedArtifact(ctx context.Context, guard *artifactCleanupGuard) (bool, error) {
	task := guard.task
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), types.UID(getModelUID(task))
	// Cached ownership survives report loss; it cannot authorize an empty acknowledgement.
	models, parents := s.configMapReconciler.cachedConfigMapEntries()
	for _, data := range []map[string]string{models, parents} {
		empty, err := unmaterializedArtifactEntry(data, key, uid)
		if err != nil {
			return false, err
		}
		if !empty {
			return false, fmt.Errorf("eviction requires recovery of recorded artifact ownership")
		}
	}
	spec := taskModelSpec(task)
	root, err := canonicalHfArtifactStoreRoot(s.modelRootDir)
	if err != nil {
		return false, err
	}
	destination := getDestPath(&spec, s.modelRootDir)
	if spec.Storage.Path == nil || *spec.Storage.Path == "" {
		destination = filepath.Clean(destination)
	}
	path, err := hfArtifactPathInRoot(destination, s.modelRootDir, root)
	if err != nil {
		return false, err
	}
	if path == root {
		return false, fmt.Errorf("cannot acknowledge eviction of the model store root")
	}
	if err := validateHfArtifactPathAncestors(root, path, true); err != nil {
		return false, err
	}
	acquired, err := s.tryLockDirectModelPath(ctx, path)
	if err != nil {
		return false, err
	}
	if !acquired {
		err := s.requeueTaskOnWait(task, gopherTaskWait)
		return err == nil, err
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("cannot evict an unrecorded local artifact at %s", path)
	}
	if err := guard.withdrawReady(ctx); err != nil {
		return false, err
	}
	return false, s.configMapReconciler.mutateConfigMapWithModelUID(ctx, key, uid, func(cm *corev1.ConfigMap) (bool, error) {
		empty, err := unmaterializedArtifactEntry(cm.Data, key, uid)
		if err != nil {
			return false, err
		}
		if !empty {
			return false, fmt.Errorf("artifact ownership changed before eviction acknowledgement")
		}
		_, _, name := GetModelTypeNamespaceAndName(task)
		return writeModelEntry(cm.Data, key, ModelEntry{Name: name, ModelUID: uid, Status: ModelStatusEvicted})
	})
}
