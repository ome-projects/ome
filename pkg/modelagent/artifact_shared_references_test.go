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
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

func TestSharedEvictionProtectsReferences(t *testing.T) {
	for _, where := range []string{"child", "subpath", "parent", "alias child", "alias parent", "unrecorded link", "unrelated missing"} {
		for _, persisted := range []bool{false, true} {
			t.Run(where+map[bool]string{false: "/live", true: "/persisted"}[persisted], func(t *testing.T) {
				g, task, input := newSharedEvictionTestModel(t)
				path := input.ChildModelPath
				switch where {
				case "subpath":
					path = filepath.Join(path, "config.json")
				case "parent", "alias parent":
					path = input.Parent.LocalPath
				case "unrelated missing":
					path = filepath.Join(t.TempDir(), "missing")
				}
				if where == "alias child" || where == "alias parent" {
					alias := filepath.Join(t.TempDir(), "alias")
					require.NoError(t, os.Symlink(path, alias))
					path = alias
				}
				if where == "unrecorded link" {
					require.NoError(t, os.Symlink(input.Parent.LocalPath, filepath.Join(g.modelRootDir, "orphan")))
				} else if persisted {
					require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
						return writeModelEntry(cm.Data, "default.basemodel.foreign", ModelEntry{ModelUID: "foreign", Status: ModelStatusReady,
							Config: &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"foreign": path}}}})
					}))
				} else {
					model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "default", UID: "foreign"},
						Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
					require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
				}
				require.NoError(t, g.processTask(task))
				entry := directArtifactEntry(t, g, task)
				if where == "child" || where == "subpath" || where == "alias child" {
					require.NotEqual(t, ModelStatusEvicted, entry.Status)
					require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
				} else {
					require.Equal(t, ModelStatusEvicted, entry.Status)
				}
				if where == "unrelated missing" {
					require.NoDirExists(t, input.Parent.LocalPath)
				} else {
					require.DirExists(t, input.Parent.LocalPath)
				}
			})
		}
	}
}

func TestSharedEvictionPlacementOnlyFiltersUnpersistedReferences(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		g, task, input := newSharedEvictionTestModel(t)
		other := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "default", UID: "foreign"},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + input.ChildModelPath),
				Path: stringPtr(input.ChildModelPath), NodeSelector: map[string]string{"off": "node"}}}}
		require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(other))
		index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		require.NoError(t, index.Add(task.BaseModel))
		require.NoError(t, index.Add(other))
		g.baseModelLister = modelslister.NewBaseModelLister(index)
		g.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
		if persisted {
			require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
				return writeModelEntry(cm.Data, getModelID(other, nil), ModelEntry{ModelUID: other.UID, Status: ModelStatusReady,
					Config: &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"foreign": input.ChildModelPath}}}})
			}))
		}
		require.NoError(t, g.processTask(task))
		if persisted {
			require.NotEqual(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
			require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
			require.DirExists(t, input.Parent.LocalPath)
		} else {
			require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
			require.NoDirExists(t, input.Parent.LocalPath)
		}
	}
}

func TestSharedEvictionLookupFailureRetainsReceipt(t *testing.T) {
	g, task, input := newSharedEvictionTestModel(t)
	receiptWritten := false
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if entry.HfArtifactPendingDeletion != nil {
			receiptWritten = true
		}
		return false, nil, nil
	})
	g.modelClient.(*omefake.Clientset).PrependReactor("list", "basemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
		if receiptWritten {
			return true, nil, errors.New("API unavailable")
		}
		return false, nil, nil
	})
	require.NoError(t, g.processTask(task))
	require.True(t, receiptWritten)
	require.NotNil(t, directArtifactEntry(t, g, task).HfArtifactPendingDeletion)
	require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
	_, finish, proceed, err := g.beginTask(&GopherTask{TaskType: Download, BaseModel: task.BaseModel})
	require.NoError(t, err)
	require.False(t, proceed, "an actually queued cleanup retry retains the barrier")
	finish(false)
}

func TestSharedEvictionActiveParentAndIndependentAttach(t *testing.T) {
	g, task, input := newSharedEvictionTestModelAt(t, "family/model-1")
	peer := sharedReaderPeer(t, g)
	unlock, acquired, err := peer.sharedHfArtifactHandler().tryArtifactOperation(input)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, g.processTask(task))
	require.NotEqual(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
	require.DirExists(t, input.Parent.LocalPath)
	unlock()
	// A different agent can attach only after deletion releases its parent lock.
	unlock, acquired, err = g.sharedHfArtifactHandler().tryArtifactOperation(input)
	require.NoError(t, err)
	require.True(t, acquired)
	sibling := testHfArtifactTaskInput(t, input.ModelStoreRoot, "family/sibling")
	seedTestChildModelEntry(t, peer.sharedHfArtifactHandler().repository, sibling)
	result, err := peer.sharedHfArtifactHandler().handleDownload(context.Background(), sibling, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	unlock()
	require.NoError(t, runTestHfArtifactDownload(peer.sharedHfArtifactHandler(), sibling))
	require.NoError(t, g.processTask(task))
	require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
	require.DirExists(t, input.Parent.LocalPath)
}

func TestSharedEvictionProtectsEffectiveFallbackReferences(t *testing.T) {
	for _, source := range []string{"local", "hf", "oci"} {
		for _, cluster := range []bool{false, true} {
			for _, empty := range []bool{false, true} {
				for _, persisted := range []bool{false, true} {
					g, task, input := newSharedEvictionTestModelAt(t, source+":")
					uri := "local://" + input.ChildModelPath
					if source == "hf" {
						uri = "hf://org/model"
					}
					if source == "oci" {
						uri = "oci://n/ns/b/models/o/model"
					}
					model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "default", UID: "reader"},
						Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri}}}
					if empty {
						model.Spec.Storage.Path = stringPtr("")
					}
					key := getModelID(model, nil)
					if cluster {
						cbm := &v1beta1.ClusterBaseModel{ObjectMeta: model.ObjectMeta, Spec: model.Spec}
						cbm.Namespace = ""
						key = getModelID(nil, cbm)
						require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(cbm))
					} else {
						require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
					}
					if persisted {
						require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
							return writeModelEntry(cm.Data, key, ModelEntry{ModelUID: model.UID, Status: ModelStatusReady})
						}))
					}
					require.NoError(t, g.processTask(task))
					require.NotEqual(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status, "%s cluster=%v empty=%v persisted=%v", source, cluster, empty, persisted)
					require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
				}
			}
		}
	}
}

func TestSharedEvictionPreservesParentForAnotherReceipt(t *testing.T) {
	g, task, input := newSharedEvictionTestModel(t)
	other := testHfArtifactTaskInput(t, input.ModelStoreRoot, "other")
	seedTestChildModelEntry(t, g.sharedHfArtifactHandler().repository, other)
	require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), other))
	_, err := g.sharedHfArtifactHandler().repository.removeModelReference(context.Background(), other.Parent, other.ChildModelKey, other.ChildModelUID, other.ChildModelPath, true)
	require.NoError(t, err)
	require.NoError(t, os.Remove(other.ChildModelPath))
	require.NoError(t, g.processTask(task))
	require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
	require.DirExists(t, input.Parent.LocalPath)
	pending, err := g.sharedHfArtifactHandler().repository.pendingDeletion(context.Background(), other.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, pending)
}

func TestSharedEvictionWithdrawsReadyBeforeReceiptAndUnlink(t *testing.T) {
	g, task, input := newSharedEvictionTestModel(t)
	require.NoError(t, g.configMapReconciler.ReconcileModelProgress(context.Background(), &ConfigMapProgressOp{BaseModel: task.BaseModel, Progress: &DownloadProgress{CompletedBytes: 10}}))
	checked := false
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if checked || entry.HfArtifactPendingDeletion == nil {
			return false, nil, nil
		}
		checked = true
		require.Equal(t, ModelStatusEvicting, entry.Status)
		require.Nil(t, entry.Progress)
		object, err := g.kubeClient.(*k8sfake.Clientset).Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", g.configMapReconciler.nodeName)
		require.NoError(t, err)
		node := object.(*corev1.Node)
		label, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
		require.NoError(t, err)
		require.NotContains(t, node.Labels, label)
		require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
		return false, nil, nil
	})
	require.NoError(t, g.processTask(task))
	require.True(t, checked)
}
