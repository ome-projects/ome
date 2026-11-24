package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func directDeletionJSON(t *testing.T, cm *corev1.ConfigMap, key string) map[string]interface{} {
	t.Helper()
	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(cm.Data[key]), &entry))
	pending, _ := entry["directPendingDeletion"].(map[string]interface{})
	return pending
}

func restartDirectDeletionGopher(t *testing.T, old *Gopher, task *GopherTask) *Gopher {
	t.Helper()
	c := old.configMapReconciler
	g := newGopherForProcessTask(makeConfigMap(c.nodeName, map[string]string{}))
	g.modelRootDir = old.modelRootDir
	g.metrics = old.metrics
	g.downloadRetry = old.downloadRetry
	g.configMapReconciler = NewConfigMapReconciler(c.nodeName, c.namespace, c.kubeClient, g.logger)
	g.nodeLabelReconciler = NewNodeLabelReconciler(c.nodeName, c.kubeClient, 1, g.logger)
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}
	g.gopherChan = make(chan *GopherTask, 10)
	g.samePathWaitDelay = time.Millisecond
	return g
}

func TestDirectDeletionPersistsAndResumes(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		for _, failure := range []string{"intent", "files complete", "final removal"} {
			t.Run(uri+"/"+failure, func(t *testing.T) {
				g, task := newDirectOperationTestGopher(t, uri)
				task.TaskType = Delete
				task.NodeIneligible = true
				path := *task.BaseModel.Spec.Storage.Path
				key := getModelID(task.BaseModel, nil)
				client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					raw, exists := cm.Data[key]
					var entry map[string]interface{}
					if exists {
						require.NoError(t, json.Unmarshal([]byte(raw), &entry))
					}
					pending, _ := entry["directPendingDeletion"].(map[string]interface{})
					fail := failure == "intent" && pending["phase"] == "FilesPending" || failure == "files complete" && pending["phase"] == "StatusPending" || failure == "final removal" && !exists
					if fail {
						return true, nil, errors.New("interrupted deletion")
					}
					return false, nil, nil
				})
				require.ErrorContains(t, g.processTask(task), "interrupted deletion")
				if failure == "intent" {
					require.FileExists(t, filepath.Join(path, "weights"))
				} else {
					require.NoDirExists(t, path)
				}
				cm, err := g.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				pending := directDeletionJSON(t, cm, key)
				if failure == "intent" {
					require.Nil(t, pending)
				} else {
					require.Equal(t, string(task.BaseModel.UID), pending["modelUID"])
					canonical, err := canonicalModelPath(path)
					require.NoError(t, err)
					require.Equal(t, canonical, pending["path"])
					// Simulate a process restart: no tracker, queue or in-memory cache survives.
					if failure == "files complete" {
						require.Equal(t, "FilesPending", pending["phase"])
					} else {
						require.Equal(t, "StatusPending", pending["phase"])
					}
				}
				client.ReactionChain = client.ReactionChain[1:]
				restarted := restartDirectDeletionGopher(t, g, task)
				retry := *task
				retry.Sequence = 0
				require.NoError(t, restarted.processTask(&retry))
				require.NoDirExists(t, path)
				cm, err = restarted.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				require.NotContains(t, cm.Data, key)
			})
		}
	}
}

func TestDirectDeletionStartupReplaysIneligibleModel(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	task.TaskType, task.NodeIneligible = Delete, true
	key := getModelID(task.BaseModel, nil)
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if _, ok := cm.Data[key]; !ok {
			return true, nil, errors.New("interrupt final status")
		}
		return false, nil, nil
	})
	require.Error(t, g.processTask(task))
	client.ReactionChain = client.ReactionChain[1:]
	moved := task.BaseModel.DeepCopy()
	moved.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
	_, tasks, _ := startHfArtifactScout(t, g.configMapReconciler, moved)
	select {
	case cleanup := <-tasks:
		require.Equal(t, Delete, cleanup.TaskType)
		require.True(t, cleanup.NodeIneligible)
		restarted := restartDirectDeletionGopher(t, g, cleanup)
		require.NoError(t, restarted.processTask(cleanup))
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not replay persisted Direct cleanup")
	}
}

func interruptDirectDeletionBeforeFiles(t *testing.T, g *Gopher, task *GopherTask) {
	t.Helper()
	require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStateOnNode: Ready}))
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("interrupt before files")
	})
	task.TaskType, task.NodeIneligible = Delete, true
	require.ErrorContains(t, g.processTask(task), "interrupt before files")
	client.ReactionChain = client.ReactionChain[1:]
	spec := taskModelSpec(task)
	require.FileExists(t, filepath.Join(*spec.Storage.Path, "weights"))
}

func TestDirectDeletionRetainsReservedAndReferencedFiles(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		for _, reason := range []string{"reserved", "referenced"} {
			t.Run(uri+"/"+reason, func(t *testing.T) {
				g, task := newDirectOperationTestGopher(t, uri)
				path := *task.BaseModel.Spec.Storage.Path
				if reason == "reserved" {
					task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
				} else {
					borrower := task.BaseModel.DeepCopy()
					borrower.Name, borrower.UID = "borrower", "borrower-uid"
					g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, borrower}}
				}
				task.TaskType, task.NodeIneligible = Delete, true
				require.NoError(t, g.processTask(task))
				require.FileExists(t, filepath.Join(path, "weights"))
				cm, err := g.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil), "intentional retention completes deletion")
			})
		}
	}
}

func TestDirectDeletionRejectsReplacementAndChangedDestination(t *testing.T) {
	for _, change := range []string{"UID", "path", "source", "receipt"} {
		t.Run(change, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
			interruptDirectDeletionBeforeFiles(t, g, task)
			oldPath := *task.BaseModel.Spec.Storage.Path
			next := *task
			next.Sequence = 0
			next.BaseModel = task.BaseModel.DeepCopy()
			switch change {
			case "UID":
				next.BaseModel.UID = "replacement-uid"
			case "path":
				next.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "other"))
			case "source":
				next.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/another")
			case "receipt":
				cm, err := g.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				key := getModelID(task.BaseModel, nil)
				entry, err := existingModelEntry(cm.Data, key)
				require.NoError(t, err)
				entry.DirectPendingDeletion.ModelUID = "replacement-uid"
				_, err = writeModelEntry(cm.Data, key, entry)
				require.NoError(t, err)
				_, err = g.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			restarted := restartDirectDeletionGopher(t, g, &next)
			require.Error(t, restarted.processTask(&next))
			require.FileExists(t, filepath.Join(oldPath, "weights"))
			cm, err := restarted.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			require.Contains(t, cm.Data, getModelID(task.BaseModel, nil))
		})
	}
}

func TestDirectDeletionResumesBeforeEligibleDownload(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newDirectOperationTestGopher(t, uri)
			interruptDirectDeletionBeforeFiles(t, g, task)
			restarted := restartDirectDeletionGopher(t, g, task)
			download := *task
			download.TaskType, download.NodeIneligible, download.Sequence = Download, false, 0
			calls := 0
			require.NoError(t, runDirectTestDownload(restarted, &download, func(_ context.Context, path string) error {
				calls++
				require.NoFileExists(t, filepath.Join(path, "weights"), "committed cleanup must finish before a new write")
				cm, err := restarted.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				require.Nil(t, directDeletionJSON(t, cm, getModelID(task.BaseModel, nil)))
				require.NoError(t, os.MkdirAll(path, 0o755))
				return os.WriteFile(filepath.Join(path, "new-weights"), []byte("new"), 0o600)
			}))
			require.Equal(t, 1, calls)
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "new-weights"))
		})
	}
}

func TestDirectDeletionCachesReceiptAndBlocksOrdinaryMutation(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	interruptDirectDeletionBeforeFiles(t, g, task)
	c := g.configMapReconciler
	// Neither status publication nor generic record removal can consume a receipt.
	require.Error(t, c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: task.BaseModel, ModelStatus: ModelStatusReady}))
	require.Error(t, c.DeleteModelFromConfigMap(context.Background(), task.BaseModel, nil))
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(context.Background(), c.nodeName, metav1.DeleteOptions{}))
	cm, err := c.getConfigMapForArtifactCleanup(context.Background())
	require.NoError(t, err)
	require.NotNil(t, directDeletionJSON(t, cm, getModelID(task.BaseModel, nil)), "failed cleanup must retain its cached receipt")
}

func TestDirectDeletionClusterModelStartupResume(t *testing.T) {
	g, original := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	model := &v1beta1.ClusterBaseModel{ObjectMeta: *original.BaseModel.ObjectMeta.DeepCopy(), Spec: original.BaseModel.Spec}
	model.Namespace = ""
	task := &GopherTask{TaskType: Delete, ClusterBaseModel: model, NodeIneligible: true}
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{model}}
	interruptDirectDeletionBeforeFiles(t, g, task)
	model = model.DeepCopy()
	model.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
	_, tasks, _ := startHfArtifactScout(t, g.configMapReconciler, model)
	select {
	case cleanup := <-tasks:
		require.Equal(t, Delete, cleanup.TaskType)
		require.NotNil(t, cleanup.ClusterBaseModel)
		g.taskTracker = gopherTaskTracker{}
		g.taskQueue = newGopherTaskQueue()
		g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{model}}
		require.NoError(t, g.processTask(cleanup))
		require.NoDirExists(t, *model.Spec.Storage.Path)
		cm, err := g.configMapReconciler.getConfigMap(context.Background())
		require.NoError(t, err)
		require.NotContains(t, cm.Data, getModelID(nil, model))
	case <-time.After(3 * time.Second):
		t.Fatal("cluster model cleanup was not resumed")
	}
}

func TestDirectDeletionStartupIgnoresReplacementReceipt(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	interruptDirectDeletionBeforeFiles(t, g, task)
	replacement := task.BaseModel.DeepCopy()
	replacement.UID = "replacement-uid"
	_, tasks, _ := startHfArtifactScout(t, g.configMapReconciler, replacement)
	select {
	case next := <-tasks:
		require.Equal(t, Download, next.TaskType, "old receipt must not create a Delete for a new UID")
		require.Equal(t, replacement.UID, next.BaseModel.UID)
	case <-time.After(3 * time.Second):
		t.Fatal("replacement download was not queued")
	}
	require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights"))
}

func TestDirectDeletionStatusResumeDoesNotDeleteNewFiles(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	task.TaskType, task.NodeIneligible = Delete, true
	key := getModelID(task.BaseModel, nil)
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if _, exists := cm.Data[key]; !exists {
			return true, nil, errors.New("interrupt final removal")
		}
		return false, nil, nil
	})
	require.Error(t, g.processTask(task))
	path := *task.BaseModel.Spec.Storage.Path
	require.NoError(t, os.MkdirAll(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "later-file"), []byte("preserve"), 0o600))
	client.ReactionChain = client.ReactionChain[1:]
	restarted := restartDirectDeletionGopher(t, g, task)
	retry := *task
	retry.Sequence = 0
	require.NoError(t, restarted.processTask(&retry))
	require.FileExists(t, filepath.Join(path, "later-file"), "StatusPending resumes publication only")
}

func TestDirectDeletionResumeWaitKeepsDownloadIntent(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "hf://org/model")
	path := *task.BaseModel.Spec.Storage.Path
	parent := filepath.Join(g.modelRootDir, "parent")
	require.NoError(t, os.MkdirAll(parent, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "weights"), []byte("parent"), 0o600))
	require.NoError(t, os.RemoveAll(path))
	require.NoError(t, os.Symlink(parent, path))
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		BaseModel: task.BaseModel, ModelMetadata: ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{"default.basemodel.parent": parent}}},
	}))
	interruptDirectDeletionBeforeFiles(t, g, task)
	restarted := restartDirectDeletionGopher(t, g, task)
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: g.modelRootDir, ChildModelPath: parent})
	require.NoError(t, err)
	require.True(t, acquired)
	defer lock.Close()
	download := *task
	download.TaskType, download.NodeIneligible, download.Sequence = Download, false, 0
	calls := 0
	write := func(_ context.Context, path string) error {
		calls++
		require.NoError(t, os.MkdirAll(path, 0o755))
		return os.WriteFile(filepath.Join(path, "new-weights"), []byte("new"), 0o600)
	}
	require.NoError(t, runDirectTestDownload(restarted, &download, write))
	require.Zero(t, calls)
	retry := receiveDirectRetry(t, restarted)
	require.Same(t, &download, retry, "cleanup contention must retain the original download")
	require.Equal(t, Download, retry.TaskType)
	require.NoError(t, lock.Close())
	require.NoError(t, runDirectTestDownload(restarted, retry, write))
	require.Equal(t, 1, calls)
	require.NoDirExists(t, parent)
	require.FileExists(t, filepath.Join(path, "new-weights"))
}

func TestDirectDeletionResumesPartiallyRemovedDirectory(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	interruptDirectDeletionBeforeFiles(t, g, task)
	path := *task.BaseModel.Spec.Storage.Path
	require.NoError(t, os.Remove(filepath.Join(path, "weights")))
	require.NoError(t, os.WriteFile(filepath.Join(path, "remaining"), []byte("partial cleanup"), 0o600))
	restarted := restartDirectDeletionGopher(t, g, task)
	retry := *task
	retry.Sequence = 0
	require.NoError(t, restarted.processTask(&retry))
	require.NoDirExists(t, path)
}

func TestDirectDeletionFinalWriteRetryKeepsReceipt(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "oci://n/ns/b/bucket/o/model")
	task.TaskType = Delete
	now := metav1.Now()
	task.BaseModel.DeletionTimestamp = &now
	key := getModelID(task.BaseModel, nil)
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if _, exists := cm.Data[key]; !exists {
			return true, nil, errors.New("final write failed")
		}
		return false, nil, nil
	})
	require.ErrorContains(t, g.processTask(task), "final write failed")
	cached, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.Equal(t, directStatusPending, directDeletionJSON(t, cached, key)["phase"])
	client.ReactionChain = client.ReactionChain[1:]
	retry := *task
	retry.Sequence = 0
	require.NoError(t, g.processTask(&retry))
	cm, err := client.CoreV1().ConfigMaps(g.configMapReconciler.namespace).Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, cm.Data, key)
}

func TestDirectDeletionParentFailureKeepsFilePhase(t *testing.T) {
	g, task := newDirectOperationTestGopher(t, "hf://org/model")
	task.TaskType, task.NodeIneligible = Delete, true
	path := *task.BaseModel.Spec.Storage.Path
	parent := filepath.Join(g.modelRootDir, "parent")
	require.NoError(t, os.MkdirAll(parent, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "weights"), []byte("parent"), 0o600))
	require.NoError(t, os.RemoveAll(path))
	require.NoError(t, os.Symlink(parent, path))
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		BaseModel: task.BaseModel, ModelMetadata: ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{"default.basemodel.parent": parent}}},
	}))
	require.NoError(t, os.Chmod(parent, 0o500))
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	require.Error(t, g.processTask(task))
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	key := getModelID(task.BaseModel, nil)
	require.Equal(t, directFilesPending, directDeletionJSON(t, cm, key)["phase"])
	require.FileExists(t, filepath.Join(parent, "weights"))
	require.NoError(t, os.Chmod(parent, 0o755))
	retry := *task
	retry.Sequence = 0
	require.NoError(t, restartDirectDeletionGopher(t, g, task).processTask(&retry))
	require.NoDirExists(t, parent)
}
