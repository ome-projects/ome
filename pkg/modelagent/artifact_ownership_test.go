package modelagent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestArtifactStatusPersistsUIDAndRejectsPreviousOwner(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	ctx := context.Background()
	require.NoError(t, g.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel: task.BaseModel, ModelStatus: ModelStatusReady,
	}))
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(cm.Data[input.ChildModelKey]), &entry))
	require.Equal(t, string(task.BaseModel.UID), entry["modelUID"])

	// A restarted agent has no in-memory invalidation of the previous instance.
	restarted := NewConfigMapReconciler(cm.Name, cm.Namespace, g.kubeClient, g.logger)
	old := task.BaseModel.DeepCopy()
	old.UID = "previous-uid"
	require.Error(t, restarted.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel: old, ModelStatus: ModelStatusFailed,
	}))
	after, err := restarted.getConfigMap(ctx)
	require.NoError(t, err)
	require.Equal(t, cm.Data, after.Data)
}

func TestArtifactProgressPersistsUID(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	ctx := context.Background()
	require.NoError(t, g.configMapReconciler.ReconcileModelProgress(ctx, &ConfigMapProgressOp{
		BaseModel: task.BaseModel, Progress: &DownloadProgress{Phase: "Downloading"},
	}))
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Equal(t, task.BaseModel.UID, entry.ModelUID)
}

func TestArtifactOrdinaryHfWaitsForSharedChildLock(t *testing.T) {
	g, task, input, source, downloads := newTestDirectHfSource(t)
	defer g.taskQueue.close()
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	lock, acquired, err := tryHfArtifactChildFileLock(input)
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.True(t, waiting)
	require.Zero(t, *downloads)
}

func TestArtifactSharedReadyRequiresChildLink(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
	require.NoError(t, os.Remove(input.ChildModelPath))
	unlock, err := g.lockHfChildStatus(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
	if unlock != nil {
		defer unlock()
	}
	require.Error(t, err, "a parent Ready marker alone cannot authorize child readiness")
}

func TestArtifactSharedReadyWaitsForChildLock(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
	lock, acquired, err := tryHfArtifactChildFileLock(input)
	require.NoError(t, err)
	require.True(t, acquired)
	defer lock.Close()
	unlock, err := g.lockHfChildStatus(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
	if unlock != nil {
		defer unlock()
	}
	require.Error(t, err, "publication must serialize with a writer for a different parent")
}

func TestArtifactLegacySymlinkCleanupWaitsForChildLock(t *testing.T) {
	g, task, input, source, downloads := newTestDirectHfSource(t)
	defer g.taskQueue.close()
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	target := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(target, "preserved"), []byte("owned elsewhere"), 0600))
	require.NoError(t, os.Symlink(target, input.ChildModelPath))
	lock, acquired, err := tryHfArtifactChildFileLock(input)
	require.NoError(t, err)
	require.True(t, acquired)
	defer lock.Close()
	waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.True(t, waiting)
	require.Zero(t, *downloads)
	actual, err := os.Readlink(input.ChildModelPath)
	require.NoError(t, err)
	require.Equal(t, target, actual)
	require.FileExists(t, filepath.Join(target, "preserved"))
}

func TestArtifactWriterRejectsUnsafeStorePaths(t *testing.T) {
	for _, name := range []string{"root", "shared-store", "locks", "ancestor-link", "file", "oci-link"} {
		t.Run(name, func(t *testing.T) {
			g, task, _ := newDirectArtifactTestModel(t)
			path := g.modelRootDir
			switch name {
			case "shared-store":
				path = filepath.Join(path, "_artifacts", "other")
			case "locks":
				path = filepath.Join(path, hfArtifactLockDirectory, "other")
			case "ancestor-link":
				link := filepath.Join(path, "alias")
				require.NoError(t, os.Symlink(t.TempDir(), link))
				path = filepath.Join(link, "model")
			case "file":
				path = filepath.Join(path, "file")
				require.NoError(t, os.WriteFile(path, []byte("preserved"), 0600))
			case "oci-link":
				path = filepath.Join(path, "link")
				require.NoError(t, os.Symlink(t.TempDir(), path))
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/models/o/model")
			}
			task.BaseModel.Spec.Storage.Path = &path
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			release, acquired, err := g.acquireDirectArtifactDownload(context.Background(), task)
			if release != nil {
				defer release()
			}
			require.Error(t, err)
			require.False(t, acquired)
		})
	}
}
