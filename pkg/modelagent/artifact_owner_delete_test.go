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
	"sigs.k8s.io/ome/pkg/constants"
)

func TestRestartedOrdinaryDeleteClearsPreviousOwner(t *testing.T) {
	for _, deleting := range []bool{true, false} {
		t.Run(fmt.Sprint("deleting=", deleting), func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			// The agent was offline when the old UID was removed and this replacement
			// was created and then marked for deletion. Scout skips its download Add
			// event and reconcilePendingDeletions dispatches this Delete directly.
			task.TaskType = Delete
			task.BaseModel.UID = "replacement-uid"
			task.BaseModel.Spec.Storage.StorageUri = stringPtr("local://" + path)
			if deleting {
				now := metav1.Now()
				task.BaseModel.DeletionTimestamp = &now
			} else {
				task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"pool": "elsewhere"}
				g.configMapReconciler.modelCache[getModelID(task.BaseModel, nil)] = &CacheEntry{ModelUID: "uid", ModelStatus: ModelStatusReady}
			}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)

			err := g.processTask(task)
			cm, getErr := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, getErr)
			node, getErr := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.NotContains(t, node.Labels, constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name), "the name-based Ready label was withdrawn")
			require.FileExists(t, filepath.Join(path, "weights"), "Local deletion must remain read-only")
			require.NoError(t, err, "startup deletion of the current CR must finish ordinary bookkeeping cleanup")
			require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil), "old Ready state must not survive current-CR deletion")
		})
	}
}

func TestOrdinaryDeletionHandoffRequiresLiveProof(t *testing.T) {
	for _, scenario := range []string{"absent", "replaced", "request changed", "no client", "shared receipt", "direct receipt"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			key := getModelID(task.BaseModel, nil)
			task.BaseModel.UID = "replacement"
			live := task.BaseModel.DeepCopy()
			switch scenario {
			case "replaced":
				live.UID = "third-owner"
			case "request changed":
				live.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "new-request"}
			case "shared receipt", "direct receipt":
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry := directArtifactEntry(t, g, task)
				if scenario == "shared receipt" {
					entry.HfArtifactPendingDeletion = &HfArtifactPendingDeletion{ModelUID: "uid", ChildPath: path}
				} else {
					entry.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: "uid", Path: path}
				}
				_, err = writeModelEntry(cm.Data, key, entry)
				require.NoError(t, err)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			g.modelClient = omefake.NewSimpleClientset(live)
			if scenario == "absent" {
				g.modelClient = omefake.NewSimpleClientset()
			} else if scenario == "no client" {
				g.modelClient = nil
			}
			before, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.Error(t, g.handoffOrdinaryArtifactDeletion(ctx, task))
			after, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.Equal(t, before.Data, after.Data)
			require.False(t, g.configMapReconciler.isModelMutationBlocked(key, task.BaseModel.UID))
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestOrdinaryDeleteRetriesAfterVerifiedHandoff(t *testing.T) {
	ctx := context.Background()
	g, task, _ := newDirectArtifactTestModel(t)
	task.BaseModel.UID = "replacement"
	now := metav1.Now()
	task.BaseModel.DeletionTimestamp = &now
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	client := g.kubeClient.(*k8sfake.Clientset)
	failed := false
	key := getModelID(task.BaseModel, nil)
	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if !failed && cm.Data[key] == "" {
			failed = true
			return true, nil, fmt.Errorf("transient final removal failure")
		}
		return false, nil, nil
	})
	op := &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted}
	require.ErrorContains(t, g.safeNodeLabelReconciliation(ctx, op), "transient final removal failure")
	require.Equal(t, task.BaseModel.UID, directArtifactEntry(t, g, task).ModelUID)
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, op))
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	require.NotContains(t, cm.Data, key)
}
