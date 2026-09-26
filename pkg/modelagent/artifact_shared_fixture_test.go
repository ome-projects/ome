package modelagent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/constants"
)

func newSharedEvictionTestModel(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	return newSharedEvictionTestModelAt(t, "model-1")
}

func newSharedEvictionTestModelAt(t *testing.T, childPath string) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	t.Helper()
	g, task, input := newTestHfArtifactGopher(t)
	custom := testHfArtifactTaskInput(t, input.ModelStoreRoot, childPath)
	custom.ChildModelKey, custom.ChildModelUID = input.ChildModelKey, input.ChildModelUID
	input = custom
	task.BaseModel.Spec.Storage.Path = &input.ChildModelPath
	require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
	require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		if err != nil {
			return false, err
		}
		entry.ModelUID, entry.Status = input.ChildModelUID, ModelStatusReady
		return writeModelEntry(cm.Data, input.ChildModelKey, entry)
	}))
	task.TaskType = Evict
	task.BaseModel.Annotations[ArtifactResidencyAnnotation] = string(ModelStatusEvicted)
	_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	g.samePathWaitDelay = time.Millisecond
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
	_, err = g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: g.configMapReconciler.nodeName, Labels: map[string]string{
			"kubernetes.io/hostname": g.configMapReconciler.nodeName,
			constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name): "Ready",
		},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	return g, task, input
}
