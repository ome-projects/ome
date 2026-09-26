package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/modelparser"
)

func sharedReaderPeer(t *testing.T, g *Gopher) *Gopher {
	t.Helper()
	api := omefake.NewSimpleClientset()
	api.ReactionChain = nil
	api.AddReactor("*", "*", k8stesting.ObjectReaction(g.modelClient.(*omefake.Clientset).Tracker()))
	kube := k8sfake.NewSimpleClientset()
	kube.ReactionChain = nil
	kube.AddReactor("*", "*", k8stesting.ObjectReaction(g.kubeClient.(*k8sfake.Clientset).Tracker()))
	return &Gopher{
		configMapReconciler: NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, kube, g.logger),
		nodeLabelReconciler: NewNodeLabelReconciler(g.configMapReconciler.nodeName, kube, 1, g.logger),
		modelConfigParser:   modelparser.NewModelConfigParser(api, g.logger),
		modelClient:         api, kubeClient: kube, modelRootDir: g.modelRootDir, logger: g.logger,
		metrics: NewMetrics(prometheus.NewRegistry()), gopherChan: make(chan *GopherTask, 10), samePathWaitDelay: time.Millisecond,
	}
}

func TestSharedLocalReaderHoldsParentAndChildThroughReady(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, useParent := range []bool{false, true} {
			t.Run(map[bool]string{false: "managed/", true: "external/"}[external]+map[bool]string{false: "child link", true: "parent"}[useParent], func(t *testing.T) {
				g, target, input := newSharedEvictionTestModel(t)
				if external {
					g.modelRootDir = t.TempDir()
				}
				peer := sharedReaderPeer(t, g)
				path := input.ChildModelPath
				if useParent {
					path = input.Parent.LocalPath
				}
				model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "default", UID: "reader"},
					Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
				require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
				task := &GopherTask{TaskType: Download, BaseModel: model}
				checked := false
				peer.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
					if strings.Contains(string(action.(k8stesting.PatchAction).GetPatch()), `"Ready"`) {
						checked = true
						unlock, acquired, err := g.sharedHfArtifactHandler().tryArtifactOperation(input)
						require.NoError(t, err)
						if acquired {
							unlock()
						}
						require.False(t, acquired, "the actual parent's deletion lock must still be held at Ready")
					}
					return false, nil, nil
				})
				require.NoError(t, peer.processTask(task))
				require.True(t, checked)
				entry := directArtifactEntry(t, peer, task)
				require.Equal(t, ModelStatusReady, entry.Status)
				require.Empty(t, entry.DirectArtifactPath)
				require.Empty(t, entry.HfArtifactKey)
				require.NoError(t, g.processTask(target))
				require.DirExists(t, input.Parent.LocalPath)
				if useParent {
					require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, target).Status)
				} else {
					require.NotEqual(t, ModelStatusEvicted, directArtifactEntry(t, g, target).Status)
					require.True(t, g.sharedHfArtifactHandler().files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
				}
			})
		}
	}
}

func TestSharedLocalReaderWaitsForCleanupAndDoesNotRecreate(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, useParent := range []bool{false, true} {
			g, target, input := newSharedEvictionTestModel(t)
			if external {
				g.modelRootDir = t.TempDir()
			}
			peer := sharedReaderPeer(t, g)
			path := input.ChildModelPath
			if useParent {
				path = input.Parent.LocalPath
			}
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "default", UID: "reader"},
				Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
			task := &GopherTask{TaskType: Download, BaseModel: model}
			// Simulate arrival after the final reference list while cleanup owns locks.
			unlock, acquired, err := g.sharedHfArtifactHandler().tryArtifactOperation(input)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
			require.NoError(t, peer.processTask(task))
			require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, task).Status)
			unlock()
			// No published reader remains: the queued reader must check presence anew.
			require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(model.Namespace).Delete(context.Background(), model.Name, metav1.DeleteOptions{}))
			require.NoError(t, peer.configMapReconciler.DeleteModelFromConfigMap(context.Background(), model, nil))
			require.NoError(t, g.processTask(target))
			require.NoDirExists(t, input.Parent.LocalPath)
			model.UID = "new-reader"
			require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
			task.Sequence = 0
			if useParent {
				require.NoError(t, peer.processTask(task))
			} else {
				require.Error(t, peer.processTask(task))
			}
			require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, task).Status)
			require.NoDirExists(t, input.ChildModelPath)
			require.NoDirExists(t, input.Parent.LocalPath)
		}
	}
}

func TestSharedLocalReaderRequiresPersistedParentIdentity(t *testing.T) {
	g, _, input := newSharedEvictionTestModel(t)
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	delete(cm.Data, input.Parent.Key)
	_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	peer := sharedReaderPeer(t, g)
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "default", UID: "reader"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + input.Parent.LocalPath)}}}
	require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
	task := &GopherTask{TaskType: Download, BaseModel: model}
	require.NoError(t, peer.processTask(task))
	require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, task).Status)
}

func TestSharedLocalReaderArrivingDuringRealEvictionCannotPublishReady(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, useParent := range []bool{false, true} {
			// External child contention is exercised before unlink above.
			if external && !useParent {
				continue
			}
			g, target, input := newSharedEvictionTestModel(t)
			if external {
				g.modelRootDir = t.TempDir()
				probe := *target
				probe.TaskType = Download
				planned, eligible, err := newHfArtifactTaskInput(&probe, probe.BaseModel.Spec.Storage, g.modelRootDir, input.Parent.Identity)
				require.NoError(t, err)
				require.True(t, eligible)
				require.Equal(t, input.ModelStoreRoot, planned.ModelStoreRoot)
			}
			peer := sharedReaderPeer(t, g)
			path := input.ChildModelPath
			if useParent {
				path = input.Parent.LocalPath
			}
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "late-reader", Namespace: "default", UID: "late-reader"},
				Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
			task := &GopherTask{TaskType: Download, BaseModel: model}
			injected := false
			g.modelClient.(*omefake.Clientset).PrependReactor("list", "clusterbasemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
				_, err := os.Lstat(input.ChildModelPath)
				if injected || !os.IsNotExist(err) {
					return false, nil, nil
				}
				// The last-parent reference snapshot is complete, but RemoveAll has
				// not run yet. A different process sees only shared API state/disk.
				injected = true
				require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
				require.NoError(t, peer.processTask(task))
				require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, task).Status)
				return true, &v1beta1.ClusterBaseModelList{}, nil
			})
			require.NoError(t, g.processTask(target))
			require.True(t, injected)
			require.NoDirExists(t, input.Parent.LocalPath)
			_ = peer.processTask(task)
			require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, task).Status)
			require.Empty(t, directArtifactEntry(t, peer, task).DirectArtifactPath)
			require.NoDirExists(t, input.Parent.LocalPath)
		}
	}
}

func TestSharedLocalReaderRejectsUnprotectedManagedAlias(t *testing.T) {
	g, _, input := newSharedEvictionTestModel(t)
	// Root-level parents use independent locks, even though both are below
	// _artifacts. Borrowing an alias in a different parent's files is unsafe.
	alias := filepath.Join(g.modelRootDir, "_artifacts", "other-parent", "alias")
	require.NoError(t, os.MkdirAll(filepath.Dir(alias), 0700))
	require.NoError(t, os.Symlink(input.Parent.LocalPath, alias))
	peer := sharedReaderPeer(t, g)
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "default", UID: "reader"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + alias)}}}
	require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
	task := &GopherTask{TaskType: Download, BaseModel: model}
	require.NoError(t, peer.processTask(task))
	require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, peer, task).Status)
}
