package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestSharedEvictionReceiptSurvivesRestart(t *testing.T) {
	for _, phase := range []string{"receipt", "lost receipt response", "unlink", "parent removal", "parent entry", "lost parent entry response", "ack", "lost ack response"} {
		t.Run(phase, func(t *testing.T) {
			g, task, input := newSharedEvictionTestModel(t)
			client := g.kubeClient.(*k8sfake.Clientset)
			injected := false
			client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
				cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				_, parentPresent := cm.Data[input.Parent.Key]
				trigger := false
				switch phase {
				case "receipt", "lost receipt response", "unlink", "parent removal":
					trigger = entry.HfArtifactPendingDeletion != nil
				case "parent entry", "lost parent entry response":
					trigger = !parentPresent
				case "ack", "lost ack response":
					trigger = entry.Status == ModelStatusEvicted
				}
				if !trigger || injected {
					return false, nil, nil
				}
				injected = true
				switch phase {
				case "unlink":
					require.NoError(t, os.Remove(input.ChildModelPath))
					require.NoError(t, os.Mkdir(input.ChildModelPath, 0700))
					return false, nil, nil
				case "parent removal":
					if os.Geteuid() == 0 {
						t.Skip("root bypasses directory permissions")
					}
					require.NoError(t, os.Chmod(filepath.Dir(input.Parent.LocalPath), 0500))
					t.Cleanup(func() { _ = os.Chmod(filepath.Dir(input.Parent.LocalPath), 0700) })
					return false, nil, nil
				case "lost receipt response", "lost parent entry response", "lost ack response":
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				}
				return true, nil, errors.New("injected lost operation")
			})
			require.NoError(t, g.processTask(task))
			require.True(t, injected)
			entry := directArtifactEntry(t, g, task)
			if phase != "lost ack response" {
				require.NotEqual(t, ModelStatusEvicted, entry.Status)
				if phase != "receipt" {
					require.NotNil(t, entry.HfArtifactPendingDeletion)
				}
			}
			if phase == "unlink" {
				require.NoError(t, os.Remove(input.ChildModelPath))
			}
			if phase == "parent removal" {
				require.NoError(t, os.Chmod(filepath.Dir(input.Parent.LocalPath), 0700))
			}
			restarted := sharedReaderPeer(t, g)
			task.Sequence = 0
			require.NoError(t, restarted.processTask(task))
			entry = directArtifactEntry(t, restarted, task)
			require.Equal(t, ModelStatusEvicted, entry.Status)
			require.Nil(t, entry.HfArtifactPendingDeletion)
			require.NoDirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedEvictionRevalidatesAfterReceiptCAS(t *testing.T) {
	for _, change := range []string{"UID", "source", "path", "policy", "intent"} {
		t.Run(change, func(t *testing.T) {
			g, task, input := newSharedEvictionTestModel(t)
			changed := false
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
				cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if changed || entry.HfArtifactPendingDeletion == nil {
					return false, nil, nil
				}
				changed = true
				latest := task.BaseModel.DeepCopy()
				switch change {
				case "UID":
					latest.UID = "replacement"
				case "source":
					latest.Spec.Storage.StorageUri = stringPtr("hf://other/model")
				case "path":
					latest.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "new-path"))
				case "policy":
					latest.Spec.Storage.DownloadPolicy = nil
				case "intent":
					delete(latest.Annotations, ArtifactResidencyAnnotation)
				}
				require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
				return false, nil, nil
			})
			require.NoError(t, g.processTask(task))
			require.True(t, changed)
			require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
			require.NotNil(t, directArtifactEntry(t, g, task).HfArtifactPendingDeletion)
			// Obsolete eviction releases its barrier even though the receipt remains.
			latest, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(context.Background(), task.BaseModel.Name, metav1.GetOptions{})
			require.NoError(t, err)
			_, finish, proceed, err := g.beginTask(&GopherTask{TaskType: Download, BaseModel: latest})
			require.NoError(t, err)
			require.True(t, proceed)
			finish(false)
		})
	}
}

func TestSharedEvictionDeleteUsesOriginalReceiptPaths(t *testing.T) {
	g, task, input := newSharedEvictionTestModel(t)
	failed := false
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if failed || entry.HfArtifactPendingDeletion == nil {
			return false, nil, nil
		}
		failed = true
		require.NoError(t, g.kubeClient.(*k8sfake.Clientset).Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		return true, nil, errors.New("lost receipt response")
	})
	require.NoError(t, g.processTask(task))
	unowned := filepath.Join(g.modelRootDir, "unowned")
	require.NoError(t, os.Mkdir(unowned, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(unowned, "weights"), []byte("other"), 0600))
	task.BaseModel.Spec.Storage.Path = &unowned
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("hf://other/model")
	task.TaskType, task.Sequence = Delete, 0
	require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Delete(context.Background(), task.BaseModel.Name, metav1.DeleteOptions{}))
	restarted := sharedReaderPeer(t, g)
	require.NoError(t, restarted.processTask(task))
	require.FileExists(t, filepath.Join(unowned, "weights"))
	require.NoDirExists(t, input.Parent.LocalPath)
	cm, err := restarted.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.NotContains(t, cm.Data, input.ChildModelKey)
}
