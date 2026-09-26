package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestSharedCleanupReadsOneSnapshotPerBoundary(t *testing.T) {
	for _, boundary := range []string{"eviction validation", "eviction path", "cleanup path"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newSharedEvictionTestModel(t)
			g.guardSharedEvictionCleanup(task, &input)
			if boundary == "cleanup path" {
				task.TaskType = Delete
				g.guardSharedArtifactCleanup(task, &input)
			}
			check := func() (bool, error) {
				if boundary == "eviction validation" {
					return false, input.validateDeletion(ctx)
				}
				return input.pathReferenced(ctx, input.ChildModelPath)
			}
			client := g.kubeClient.(*k8sfake.Clientset)
			client.ClearActions()
			used, err := check()
			require.NoError(t, err)
			require.False(t, used)
			require.Equal(t, 1, countConfigMapGets(client))

			require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
				return writeModelEntry(cm.Data, "default.basemodel.borrower", ModelEntry{ModelUID: "borrower",
					Status: ModelStatusReady, DirectArtifactPath: input.ChildModelPath})
			}))
			client.ClearActions()
			used, err = check()
			if boundary == "eviction validation" {
				require.ErrorContains(t, err, "shared child path remains referenced")
			} else {
				require.NoError(t, err)
				require.True(t, used, "each boundary must reload newly persisted borrowers")
			}
			require.Equal(t, 1, countConfigMapGets(client))
		})
	}
}

func TestSharedCleanupValidationIsReadOnly(t *testing.T) {
	for _, restoring := range []bool{false, true} {
		t.Run(map[bool]string{false: "eviction", true: "restoration"}[restoring], func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newSharedEvictionTestModel(t)
			if restoring {
				task.TaskType = Download
				delete(task.BaseModel.Annotations, ArtifactResidencyAnnotation)
				task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore"
				_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
				g.guardSharedArtifactCleanup(task, &input)
			} else {
				g.guardSharedEvictionCleanup(task, &input)
			}
			before := directArtifactEntry(t, g, task)
			for range 3 {
				require.NoError(t, input.validateDeletion(ctx))
			}
			require.Equal(t, before, directArtifactEntry(t, g, task))
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
		})
	}
}
