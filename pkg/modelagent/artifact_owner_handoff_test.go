package modelagent

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestOrdinaryReplacementCanPublishAfterAgentRestart(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	task.TaskType = Download
	task.BaseModel.Annotations = nil
	task.BaseModel.UID = "replacement-uid"
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)

	// The previous process left a Ready entry owned by "uid". The new
	// process sees only the replacement CR, so no old delete event arrives.
	current, err := g.currentArtifactTask(context.Background(), task)
	require.NoError(t, err)
	require.NotNil(t, current)
	release, acquired, err := g.acquireDirectArtifactDownload(context.Background(), task)
	require.NoError(t, err)
	require.True(t, acquired)
	defer release()

	t.Run("publication", func(t *testing.T) {
		require.NoError(t, g.validateDirectArtifactPublication(context.Background(), task, path))
	})
	t.Run("status", func(t *testing.T) {
		require.NoError(t, g.configMapReconciler.updateModelStatusInConfigMap(context.Background(), &ConfigMapStatusOp{
			BaseModel: task.BaseModel, ModelStatus: ModelStatusReady,
		}))
	})
}

func TestOrdinaryOwnerHandoffPreservesCleanupAndSharedReferences(t *testing.T) {
	for _, state := range []string{"shared receipt", "shared child", "legacy parent", "legacy child", "one-sided parent"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			key := getModelID(task.BaseModel, nil)
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry := ModelEntry{Name: task.BaseModel.Name, ModelUID: task.BaseModel.UID, Status: ModelStatusReady}
			switch state {
			case "shared receipt":
				entry.HfArtifactPendingDeletion = &HfArtifactPendingDeletion{ModelUID: task.BaseModel.UID, ChildPath: path}
			case "shared child":
				entry.HfArtifactKey = "shared-parent"
			case "legacy parent":
				entry.Config = &ModelConfig{Artifact: Artifact{ChildrenPaths: []string{path + "-child"}}}
			case "legacy child":
				entry.Config = &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"another-model": path}}}
			case "one-sided parent":
				input := testHfArtifactTaskInput(t, g.modelRootDir, "shared")
				parent := input.Parent
				parent.Children = map[string]string{key: path}
				_, err := writeHfArtifactEntry(cm.Data, parent)
				require.NoError(t, err)
			}
			_, err = writeModelEntry(cm.Data, key, entry)
			require.NoError(t, err)
			_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			task.BaseModel.UID = "replacement"
			task.BaseModel.Annotations = nil
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			err = g.handoffOrdinaryArtifactOwner(ctx, task)
			require.Error(t, err)
			after, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.Equal(t, cm.Data, after.Data)
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestOrdinaryOwnerHandoffRecoversLostResponse(t *testing.T) {
	ctx := context.Background()
	g, task, _ := newDirectArtifactTestModel(t)
	old := task.BaseModel.DeepCopy()
	key := getModelID(old, nil)
	g.configMapReconciler.modelCache[key] = &CacheEntry{ModelName: old.Name, ModelUID: old.UID, ModelStatus: ModelStatusReady}
	task.BaseModel.UID = "replacement"
	task.BaseModel.Annotations = nil
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	client := g.kubeClient.(*k8sfake.Clientset)
	lost := false
	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if lost {
			return false, nil, nil
		}
		lost = true
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		return true, nil, fmt.Errorf("lost update response")
	})
	require.ErrorContains(t, g.handoffOrdinaryArtifactOwner(ctx, task), "lost update response")
	require.Equal(t, old.UID, g.configMapReconciler.modelCache[key].ModelUID)
	require.NoError(t, g.handoffOrdinaryArtifactOwner(ctx, task))
	require.Equal(t, task.BaseModel.UID, g.configMapReconciler.modelCache[key].ModelUID)
	require.True(t, g.configMapReconciler.isModelMutationBlocked(key, old.UID))
	require.NoError(t, g.configMapReconciler.updateModelStatusInConfigMap(ctx, &ConfigMapStatusOp{BaseModel: task.BaseModel, ModelStatus: ModelStatusReady}))
	_ = g.configMapReconciler.updateModelStatusInConfigMap(ctx, &ConfigMapStatusOp{BaseModel: old, ModelStatus: ModelStatusFailed})
	require.Equal(t, task.BaseModel.UID, directArtifactEntry(t, g, task).ModelUID)
	require.Equal(t, ModelStatusReady, directArtifactEntry(t, g, task).Status)
}

func TestOrdinaryOwnerHandoffRejectsStaleTask(t *testing.T) {
	g, task, _ := newDirectArtifactTestModel(t)
	task.BaseModel.UID = "stale-replacement"
	require.Error(t, g.handoffOrdinaryArtifactOwner(context.Background(), task))
	require.EqualValues(t, "uid", directArtifactEntry(t, g, task).ModelUID)
}
