package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestSharedDownloadCannotUndoCompletedEviction(t *testing.T) {
	for _, keepParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("sibling=%v", keepParent), func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newTestHfArtifactGopher(t)
			defer g.taskQueue.close()
			handler := g.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			if keepParent {
				sibling := input
				sibling.ChildModelKey, sibling.ChildModelUID = "default.basemodel.sibling", "sibling-uid"
				sibling.ChildModelPath = filepath.Join(input.ModelStoreRoot, "sibling")
				seedTestChildModelEntry(t, handler.repository, sibling)
				require.NoError(t, runTestHfArtifactDownload(handler, sibling))
			}
			_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: g.configMapReconciler.nodeName, UID: "node-uid",
			}}, metav1.CreateOptions{})
			require.NoError(t, err)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Updating}))

			// The download pauses before its filesystem lock. Another process
			// can finish eviction without changing the first process's task cache.
			evict := *task
			evict.TaskType, evict.SharedArtifact = Evict, true
			evict.BaseModel = task.BaseModel.DeepCopy()
			evict.BaseModel.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
			_, err = g.modelClient.OmeV1beta1().BaseModels(evict.BaseModel.Namespace).Update(ctx, evict.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			second := &Gopher{
				modelRootDir: g.modelRootDir, kubeClient: g.kubeClient, modelClient: g.modelClient,
				configMapReconciler: NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger),
				nodeLabelReconciler: NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger),
				baseModelLister:     g.baseModelLister, clusterBaseModelLister: g.clusterBaseModelLister,
				logger: g.logger, taskQueue: newGopherTaskQueue(), gopherChan: make(chan *GopherTask, 10),
			}
			defer second.taskQueue.close()
			waiting, err := second.processArtifactEviction(ctx, &evict)
			require.NoError(t, err)
			require.False(t, waiting)
			assertChildPathMissing(t, input.ChildModelPath)
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusEvicted, entry.Status)

			writes := 0
			_, _ = g.runHfArtifactDownload(ctx, task, input, true, nil, func(path string) error {
				writes++
				return os.WriteFile(filepath.Join(path, "config.json"), []byte("stale"), 0600)
			})
			require.Zero(t, writes, "the evicted child's old task must not download")
			assertChildPathMissing(t, input.ChildModelPath)
			if keepParent {
				require.DirExists(t, input.Parent.LocalPath)
			} else {
				require.NoDirExists(t, input.Parent.LocalPath)
			}
			cm, err = g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err = existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusEvicted, entry.Status)
			require.Empty(t, entry.HfArtifactKey)
		})
	}
}

func TestSharedDownloadRejectsChildChangedDuringTransfer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.BaseModel)
	}{
		{name: "UID", change: func(model *v1beta1.BaseModel) { model.UID = "replacement-uid" }},
		{name: "source", change: func(model *v1beta1.BaseModel) {
			model.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/replacement")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newTestHfArtifactGopher(t)
			defer g.taskQueue.close()
			writes := 0
			result, err := g.runHfArtifactDownload(ctx, task, input, true, nil, func(path string) error {
				writes++
				latest := task.BaseModel.DeepCopy()
				tc.change(latest)
				_, err := g.modelClient.OmeV1beta1().BaseModels(latest.Namespace).Update(ctx, latest, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(path, "config.json"), []byte("complete"), 0600)
			})
			require.NoError(t, err)
			require.Equal(t, 1, writes)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.ErrorContains(t, result.RetryReason, "no longer current")
			assertChildPathMissing(t, input.ChildModelPath)
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Empty(t, entry.HfArtifactKey)
			parent, found, err := g.sharedHfArtifactHandler().repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			require.Empty(t, parent.Children)
		})
	}
}
