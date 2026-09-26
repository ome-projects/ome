package modelagent

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestSharedReceiptCleanupPreservesLiveLocalBorrower(t *testing.T) {
	for _, restoring := range []bool{false, true} {
		for _, child := range []bool{false, true} {
			t.Run(fmt.Sprintf("restoring=%v/child=%v", restoring, child), func(t *testing.T) {
				ctx := context.Background()
				g, task, input := newSharedEvictionTestModel(t)
				task.TaskType = Download
				delete(task.BaseModel.Annotations, ArtifactResidencyAnnotation)
				if restoring {
					task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore"
				}
				_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
				h := g.sharedHfArtifactHandler()
				_, err = h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
				require.NoError(t, err)
				path := input.Parent.LocalPath
				if child {
					path = input.ChildModelPath
				}
				borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "borrower", Namespace: "default", UID: "borrower"},
					Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
				_, err = g.modelClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(ctx, borrower, metav1.CreateOptions{})
				require.NoError(t, err)
				handled, waiting, err := g.resumeHfArtifactChildDeletion(ctx, task, true)
				require.NoError(t, err)
				require.True(t, handled)
				require.DirExists(t, input.Parent.LocalPath)
				if child {
					require.True(t, h.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
					if restoring {
						require.True(t, waiting, "borrowed old link cannot be settled for restoration")
					}
				}
				if restoring {
					node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
					require.NoError(t, err)
					require.NotEqual(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
				}
			})
		}
	}
}

func TestSharedRestorationCleanupValidationDoesNotPrepare(t *testing.T) {
	ctx := context.Background()
	g, task, input := newSharedEvictionTestModel(t)
	task.TaskType = Download
	delete(task.BaseModel.Annotations, ArtifactResidencyAnnotation)
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore"
	_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	g.guardSharedArtifactCleanup(task, &input)
	before := directArtifactEntry(t, g, task)
	for range 3 {
		require.NoError(t, input.validateDeletion(ctx))
	}
	require.Equal(t, before, directArtifactEntry(t, g, task))
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
}
