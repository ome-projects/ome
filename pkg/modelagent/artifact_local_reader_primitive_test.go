package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestLocalReaderRetainsSharedOperationLocks(t *testing.T) {
	for _, layout := range []string{"child", "parent", "external store"} {
		t.Run(layout, func(t *testing.T) {
			g, owner, input := newTestHfArtifactGopher(t)
			defer g.taskQueue.close()
			h := g.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(h, input))
			reader := owner.BaseModel.DeepCopy()
			reader.Name, reader.UID = "reader", "reader-uid"
			path := input.ChildModelPath
			if layout == "parent" {
				path = input.Parent.LocalPath
			}
			if layout == "external store" {
				g.modelRootDir = t.TempDir()
			}
			reader.Spec.Storage.Path = nil
			reader.Spec.Storage.StorageUri = stringPtr("local://" + path)
			g.modelClient = omefake.NewSimpleClientset(owner.BaseModel, reader)
			task := &GopherTask{TaskType: Download, BaseModel: reader}
			ctx, finish := withDirectArtifactDownloadOperation(context.Background())
			defer finish()
			release, acquired, err := g.acquireLocalArtifactReader(ctx, task)
			require.NoError(t, err)
			require.True(t, acquired)
			release()
			_, acquired, err = h.tryParentFileOperation(input.Parent, input.ModelStoreRoot)
			require.NoError(t, err)
			require.False(t, acquired)
			operation := directArtifactDownloadOperationFromContext(ctx)
			require.True(t, operation.readOnly)
			require.NotNil(t, operation.sharedReader)
			require.NoError(t, g.validateSharedArtifactReader(ctx, task, *operation.sharedReader))
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.Empty(t, cm.Data[getModelID(reader, nil)], "borrowing must not grant artifact ownership")
			finish()
			release, acquired, err = h.tryParentFileOperation(input.Parent, input.ModelStoreRoot)
			require.NoError(t, err)
			require.True(t, acquired)
			release()
		})
	}
}

func TestLocalReaderRequiresCurrentParentReadiness(t *testing.T) {
	g, owner, input := newTestHfArtifactGopher(t)
	defer g.taskQueue.close()
	require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
	reader := owner.BaseModel.DeepCopy()
	reader.Name, reader.UID = "reader", "reader-uid"
	reader.Spec.Storage.Path = nil
	reader.Spec.Storage.StorageUri = stringPtr("local://" + input.ChildModelPath)
	g.modelClient = omefake.NewSimpleClientset(owner.BaseModel, reader)
	require.NoError(t, os.Remove(filepath.Join(input.Parent.LocalPath, constants.HfArtifactReadyMarkerFileName)))
	release, acquired, err := g.acquireLocalArtifactReader(context.Background(), &GopherTask{TaskType: Download, BaseModel: reader})
	if release != nil {
		release()
	}
	require.Error(t, err)
	require.False(t, acquired)
}
