package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

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
