package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestDirectWriterOperationRetainsLockAndOwnership(t *testing.T) {
	g, task := newArtifactRequestValidationTest(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	g.modelRootDir = root
	path := filepath.Join(root, "model")
	require.NoError(t, os.MkdirAll(path, 0o700))
	task.BaseModel.Spec.Storage.Path = &path
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	ctx, finish := withDirectArtifactDownloadOperation(context.Background())
	defer finish()
	release, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
	require.NoError(t, err)
	require.True(t, acquired)
	release()
	_, acquired, err = g.acquireDirectArtifactPathLock(path)
	require.NoError(t, err)
	require.False(t, acquired, "source return must not release the execution's lock")
	require.NoError(t, g.validateDirectArtifactPublication(ctx, task, path))
	require.NoError(t, g.persistDirectArtifactPath(ctx, task, path))
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
	require.NoError(t, err)
	require.Equal(t, task.BaseModel.UID, entry.ModelUID)
	require.Equal(t, path, entry.DirectArtifactPath)
	finish()
	release, acquired, err = g.acquireDirectArtifactPathLock(path)
	require.NoError(t, err)
	require.True(t, acquired)
	release()
}

func TestDirectWriterRejectsChangedOwnerBeforePublication(t *testing.T) {
	g, task := newArtifactRequestValidationTest(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	g.modelRootDir = root
	path := filepath.Join(root, "model")
	require.NoError(t, os.MkdirAll(path, 0o700))
	task.BaseModel.Spec.Storage.Path = &path
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	ctx, finish := withDirectArtifactDownloadOperation(context.Background())
	defer finish()
	_, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
	require.NoError(t, err)
	require.True(t, acquired)
	live := task.BaseModel.DeepCopy()
	live.UID = "replacement"
	g.modelClient = omefake.NewSimpleClientset(live)
	require.Error(t, g.validateDirectArtifactPublication(ctx, task, path))
	require.Error(t, g.persistDirectArtifactPath(ctx, task, path))
}
