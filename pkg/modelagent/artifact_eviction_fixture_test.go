package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func newEvictionTestModel(t *testing.T) (*Gopher, *GopherTask, string) {
	g, task, path := newDirectArtifactTestModel(t)
	task.TaskType = "Evict"
	task.BaseModel.Annotations = map[string]string{"ome.io/artifact-residency": "Evicted"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	// A completed download records which path this UID actually owns.
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry := directArtifactEntry(t, g, task)
	entry.Config = &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{getModelID(task.BaseModel, nil): path}}}
	_, err = writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
	require.NoError(t, err)
	_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	return g, task, path
}
