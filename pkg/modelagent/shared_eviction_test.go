package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestScoutSharedEviction(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(map[bool]string{false: "BaseModel", true: "ClusterBaseModel"}[cluster], func(t *testing.T) {
			scout, tasks := newScoutForUpdateTest(t)
			old := newBaseModel("model", v1beta1.ReuseIfExists, "hf://org/model")
			model := old.DeepCopy()
			model.Annotations = map[string]string{"ome.io/artifact-residency": "Evicted"}
			checkScoutUpdateTask(t, scout, tasks, cluster, old, model, GopherTaskType("Evict"))
		})
	}
}

func TestSharedEvictionRetainsModelEntry(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	require.NoError(t, s.processTask(task))
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Equal(t, ModelStatus("Evicted"), entry.Status)
	require.Empty(t, entry.HfArtifactKey)
	require.Nil(t, entry.HfArtifactPendingDeletion)
	assertChildPathMissing(t, input.ChildModelPath)
	require.NoDirExists(t, input.Parent.LocalPath)
	// Completion is durable, including when the process-local routing cache is gone.
	s.artifactRouting = gopherArtifactRouting{}
	task.Sequence = 0
	require.NoError(t, s.processTask(task))
}

func newSharedEvictionFixture(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	t.Helper()
	s, task, input := newTestHfArtifactGopher(t)
	require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), input))
	task.TaskType = GopherTaskType("Evict")
	// Surface cleanup retry reasons synchronously in failure-injection tests.
	s.samePathWaitTimeout = time.Nanosecond
	task.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
	task.BaseModel.Annotations["ome.io/artifact-residency"] = "Evicted"
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	label, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
	require.NoError(t, err)
	_, err = client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: s.configMapReconciler.nodeName, UID: "node-uid", Labels: map[string]string{label: string(Ready)},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	s.kubeClient = client
	s.nodeLabelReconciler = NewNodeLabelReconciler(s.configMapReconciler.nodeName, client, 1, s.logger)
	WithArtifactEviction(omefake.NewSimpleClientset(task.BaseModel.DeepCopy()), "node-uid")(s)
	return s, task, input
}

func TestSharedEvictionRejectsChangedIdentityAndIntent(t *testing.T) {
	for _, change := range []string{"UID", "intent", "source", "path", "node", "missing node", "lookup", "withdrawal"} {
		t.Run(change, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			ctx := context.Background()
			model := task.BaseModel.DeepCopy()
			switch change {
			case "UID":
				model.UID = "replacement"
			case "intent":
				delete(model.Annotations, "ome.io/artifact-residency")
			case "source":
				model.Spec.Storage.StorageUri = ptr("hf://different/source")
			case "path":
				model.Spec.Storage.Path = ptr(filepath.Join(input.ModelStoreRoot, "new"))
			case "node":
				node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				node.UID = "replacement"
				_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "missing node":
				require.NoError(t, s.kubeClient.CoreV1().Nodes().Delete(ctx, s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
			case "lookup":
				s.omeClient.(*omefake.Clientset).PrependReactor("get", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("lookup failed") })
			case "withdrawal":
				s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("patch failed") })
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(ctx, model, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.Error(t, s.processTask(task))
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			require.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedEvictionRejectsCurrentLocalSource(t *testing.T) {
	for _, state := range []string{"shared ownership", "pending cleanup", "completed duplicate"} {
		t.Run(state, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			ctx := context.Background()
			if state == "pending cleanup" {
				_, err := s.sharedHfArtifactHandler().repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
				require.NoError(t, err)
			}
			if state == "completed duplicate" {
				require.NoError(t, s.processTask(task))
				task.Sequence = 0
			}
			task.BaseModel.Spec.Storage.StorageUri = ptr("local://" + input.ChildModelPath)
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			before, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			nodeBefore, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.ErrorContains(t, s.processTask(task), "Hugging Face or OCI")
			after, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.Equal(t, before.Data, after.Data, "unsupported sources must not release ownership or acknowledge eviction")
			nodeAfter, err := s.kubeClient.CoreV1().Nodes().Get(ctx, nodeBefore.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, nodeBefore.Labels, nodeAfter.Labels)
			if state != "completed duplicate" {
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
				require.DirExists(t, input.Parent.LocalPath)
			}
		})
	}
}

func TestSharedEvictionRetriesDuringSiblingRepair(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	ctx := context.Background()
	s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Minute
	task.SamePathWaitStartedAt = time.Time{}
	sibling := task.BaseModel.DeepCopy()
	sibling.Name, sibling.UID = "sibling", "sibling-uid"
	delete(sibling.Annotations, artifactResidencyAnnotation)
	sibling.Spec.Storage.Path = ptr(filepath.Join(input.ModelStoreRoot, "sibling"))
	_, err := s.omeClient.OmeV1beta1().BaseModels(sibling.Namespace).Create(ctx, sibling, metav1.CreateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, sibling}}
	second := input
	second.ChildModelKey, second.ChildModelUID, second.ChildModelPath = getModelID(sibling, nil), sibling.UID, *sibling.Spec.Storage.Path
	h := s.sharedHfArtifactHandler()
	seedTestChildModelEntry(t, h.repository, second)
	result, err := h.handleDownload(ctx, second, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome, "sibling setup: %+v", result)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	done := make(chan hfArtifactTaskResult, 1)
	go func() {
		result, err := h.handleDownloadOverride(ctx, second, func(string) (bool, error) {
			close(started)
			<-release
			return true, nil
		}, nil)
		if err != nil {
			result = newHfArtifactRetryResult(second.Parent.Key, err)
		}
		done <- result
	}()
	select {
	case <-started:
	case result := <-done:
		t.Fatalf("repair exited before entering validation: %+v", result)
	case <-time.After(2 * time.Second):
		t.Fatal("repair did not enter validation")
	}
	err = s.processTask(task)
	unblock()
	result = <-done
	require.Equal(t, hfArtifactTaskDone, result.Outcome, "sibling repair: %+v", result)
	require.NoError(t, err, "active sibling repair must queue eviction for retry")
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	requeued := receiveDirectRetry(t, s)
	require.Same(t, task, requeued)
	require.NoError(t, s.processTask(requeued))
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Equal(t, ModelStatusEvicted, entry.Status)
	assertChildPathMissing(t, input.ChildModelPath)
	assertChildSymlinkTarget(t, second.ChildModelPath, input.Parent.LocalPath)
}

func TestSharedEvictionProtectsBorrowers(t *testing.T) {
	for _, source := range []string{"direct child", "local child", "local parent", "local descendant", "pending old path", "shared sibling"} {
		t.Run(source, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			ctx := context.Background()
			path := input.ChildModelPath
			if source == "local parent" {
				path = input.Parent.LocalPath
			}
			if source == "local descendant" {
				path = filepath.Join(input.Parent.LocalPath, "config.json")
			}
			borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "borrower", Namespace: "default", UID: "borrower-uid"}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: ptr("local://" + path)}}}
			if source == "direct child" {
				borrower.Spec.Storage.StorageUri = ptr("hf://different/model")
				borrower.Spec.Storage.Path = &path
			}
			if source == "pending old path" || source == "shared sibling" {
				other := input
				other.ChildModelKey = "default.basemodel.borrower"
				other.ChildModelUID = borrower.UID
				if source == "shared sibling" {
					other.ChildModelPath = filepath.Join(input.ModelStoreRoot, "sibling")
				}
				seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, other)
				require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), other))
				if source == "pending old path" {
					_, err := s.sharedHfArtifactHandler().repository.removeModelReference(ctx, other.Parent, other.ChildModelKey, other.ChildModelUID, other.ChildModelPath, true)
					require.NoError(t, err)
				}
			} else {
				_, err := s.omeClient.OmeV1beta1().BaseModels("default").Create(ctx, borrower, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			require.NoError(t, s.processTask(task))
			require.DirExists(t, input.Parent.LocalPath)
			if source == "direct child" || source == "local child" || source == "pending old path" {
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			} else {
				assertChildPathMissing(t, input.ChildModelPath)
			}
		})
	}
}

func TestSharedEvictionUnsupportedOwnershipPreservesFiles(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	require.NoError(t, os.Remove(input.ChildModelPath))
	require.NoError(t, os.Mkdir(input.ChildModelPath, 0755))
	require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		delete(cm.Data, input.Parent.Key)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		entry.HfArtifactKey = ""
		return writeModelEntry(cm.Data, input.ChildModelKey, entry)
	}))
	require.Error(t, s.processTask(task))
	require.DirExists(t, input.ChildModelPath)
}

func TestSharedEvictionBlocksReadyPublication(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	stale := task.BaseModel.DeepCopy()
	delete(stale.Annotations, "ome.io/artifact-residency")
	require.Error(t, s.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: stale, ModelStateOnNode: Ready}))
	require.NoError(t, s.processTask(task))
	require.Error(t, s.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: stale, ModelStateOnNode: Ready}))
	assertChildPathMissing(t, input.ChildModelPath)
}

func TestSharedEvictionRevalidatesConflict(t *testing.T) {
	for _, change := range []string{"UID", "intent", "source", "local source", "node"} {
		t.Run(change, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			client := s.kubeClient.(*fake.Clientset)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if entry.HfArtifactPendingDeletion == nil || injected {
					return false, nil, nil
				}
				injected = true
				model := task.BaseModel.DeepCopy()
				switch change {
				case "UID":
					model.UID = "replaced"
				case "intent":
					delete(model.Annotations, artifactResidencyAnnotation)
				case "source":
					model.Spec.Storage.StorageUri = ptr("hf://other/model")
				case "local source":
					model.Spec.Storage.StorageUri = ptr("local://" + input.ChildModelPath)
				case "node":
					object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := object.(*corev1.Node)
					node.UID = "replacement"
					err = client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, "")
					require.NoError(t, err)
				}
				_, err = s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
				require.NoError(t, err)
				return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("retry"))
			})
			require.Error(t, s.processTask(task))
			require.True(t, injected)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			require.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedEvictionResumesInterruptedCleanup(t *testing.T) {
	for _, stage := range []string{"before unlink", "after unlink", "receipt CAS", "receipt response lost"} {
		t.Run(stage, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			client := s.kubeClient.(*fake.Clientset)
			fail := true
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if !fail {
					return false, nil, nil
				}
				if stage == "before unlink" && entry.HfArtifactPendingDeletion != nil && !injected {
					injected = true
					require.NoError(t, os.Remove(input.ChildModelPath))
					require.NoError(t, os.Mkdir(input.ChildModelPath, 0755))
				}
				_, parentPresent := cm.Data[input.Parent.Key]
				if stage == "after unlink" && !parentPresent || (stage == "receipt CAS" || stage == "receipt response lost") && entry.Status == ModelStatusEvicted {
					injected = true
					if stage == "receipt response lost" {
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					}
					return true, nil, errors.New("interrupted cleanup")
				}
				return false, nil, nil
			})
			require.Error(t, s.processTask(task))
			require.True(t, injected)
			fail = false
			if stage == "before unlink" {
				require.NoError(t, os.Remove(input.ChildModelPath))
				require.NoError(t, os.Symlink(input.Parent.LocalPath, input.ChildModelPath))
			}
			// A restart must use the receipt's old path even when current policy
			// names a different source/destination for eventual rehydration.
			path := filepath.Join(input.ModelStoreRoot, "new-destination")
			require.NoError(t, os.Mkdir(path, 0755))
			task.BaseModel.Spec.Storage.Path = &path
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://different/source")
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, client, s.logger)
			s.hfArtifactHandlerOnce = sync.Once{}
			s.artifactRouting = gopherArtifactRouting{}
			WithArtifactEviction(s.omeClient, "node-uid")(s)
			task.Sequence = 0
			require.NoError(t, s.processTask(task))
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusEvicted, entry.Status)
			require.Nil(t, entry.HfArtifactPendingDeletion)
			assertChildPathMissing(t, input.ChildModelPath)
			require.NoDirExists(t, input.Parent.LocalPath)
			require.DirExists(t, path)
		})
	}
}

func TestSharedEvictionLostResponseRefreshesCache(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	client := s.kubeClient.(*fake.Clientset)
	injected := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if entry.Status != ModelStatusEvicted || injected {
			return false, nil, nil
		}
		injected = true
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		return true, nil, errors.New("response lost")
	})
	require.Error(t, s.processTask(task))
	task.Sequence = 0
	require.NoError(t, s.processTask(task))
	require.NoError(t, client.CoreV1().ConfigMaps(s.configMapReconciler.namespace).Delete(context.Background(), s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
	s.configMapReconciler.reconcileConfigMaps()
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Equal(t, ModelStatusEvicted, entry.Status)
	require.Nil(t, entry.HfArtifactPendingDeletion)
}

func TestSharedEvictionWaitsForExistingDownload(t *testing.T) {
	s, eviction, input := newSharedEvictionFixture(t)
	s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Minute
	model := eviction.BaseModel.DeepCopy()
	model.Spec.Storage.StorageUri = ptr("hf://org/model")
	delete(model.Annotations, artifactResidencyAnnotation)
	_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
	require.NoError(t, err)
	eviction.BaseModel = model.DeepCopy()
	eviction.BaseModel.Annotations[artifactResidencyAnnotation] = "Evicted"
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}
	download := &GopherTask{TaskType: Download, BaseModel: model}
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- s.processTaskWithSourceAdapters(download, true, func(ctx context.Context, _ *GopherTask, _ v1beta1.BaseModelSpec, _ bool) (bool, error) {
			unlock, acquired, err := s.sharedHfArtifactHandler().tryArtifactOperation(input)
			if err != nil || !acquired {
				return false, errors.New("download lock unavailable")
			}
			defer unlock()
			started <- ctx
			<-release
			return false, os.WriteFile(filepath.Join(input.Parent.LocalPath, "late-weights"), []byte("late"), 0600)
		}, nil)
	}()
	writer := <-started
	_, err = s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), eviction.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.processTask(eviction))
	require.ErrorIs(t, writer.Err(), context.Canceled)
	retry := receiveDirectRetry(t, s)
	require.Same(t, eviction, retry)
	require.DirExists(t, input.Parent.LocalPath)
	unblock()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, s.processTask(retry))
	assertChildPathMissing(t, input.ChildModelPath)
	require.NoDirExists(t, input.Parent.LocalPath)
	require.Error(t, s.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready}))
}

func TestSharedEvictionOrdinaryDeleteRemovesEntry(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	task.TaskType = Delete
	require.NoError(t, s.processTask(task))
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.NotContains(t, cm.Data, input.ChildModelKey)
}

func TestSharedEvictionLongNames(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(map[bool]string{false: "BaseModel", true: "ClusterBaseModel"}[cluster], func(t *testing.T) {
			s, task, _ := newSharedEvictionFixture(t)
			model := task.BaseModel.DeepCopy()
			model.Name = strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 60)
			delete(model.Annotations, artifactResidencyAnnotation)
			if cluster {
				task.BaseModel = nil
				task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: model.ObjectMeta, Spec: model.Spec}
				task.ClusterBaseModel.Namespace = ""
				_, err := s.omeClient.OmeV1beta1().ClusterBaseModels().Create(context.Background(), task.ClusterBaseModel, metav1.CreateOptions{})
				require.NoError(t, err)
			} else {
				task.BaseModel = model
				_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Create(context.Background(), model, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			require.NoError(t, s.validateArtifactDownload(context.Background(), task), "a ConfigMap key cannot be reversed into a long API name")
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStatus: ModelStatusReady}))
		})
	}
}

func TestSharedEvictionAfterConfigMapLoss(t *testing.T) {
	s, task, first := newSharedEvictionFixture(t)
	ctx := context.Background()
	second := first
	second.ChildModelKey, second.ChildModelUID, second.ChildModelPath = "default.basemodel.second", "second-uid", filepath.Join(first.ModelStoreRoot, "second")
	seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, second)
	require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), second))
	other := task.BaseModel.DeepCopy()
	other.Name, other.UID, other.Spec.Storage.Path = "second", second.ChildModelUID, &second.ChildModelPath
	for _, model := range []*v1beta1.BaseModel{task.BaseModel, other} {
		resident := model.DeepCopy()
		delete(resident.Annotations, artifactResidencyAnnotation)
		_, err := s.omeClient.OmeV1beta1().BaseModels(resident.Namespace).Update(ctx, resident, metav1.UpdateOptions{})
		if apierrors.IsNotFound(err) {
			_, err = s.omeClient.OmeV1beta1().BaseModels(resident.Namespace).Create(ctx, resident, metav1.CreateOptions{})
		}
		require.NoError(t, err)
		require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: resident, ModelStatus: ModelStatusReady}))
	}
	for _, model := range []*v1beta1.BaseModel{task.BaseModel, other} {
		_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(ctx, model, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	require.NoError(t, s.kubeClient.CoreV1().ConfigMaps(s.configMapReconciler.namespace).Delete(ctx, s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
	// Recovery must not restore either cached Ready acknowledgement.
	s.configMapReconciler.reconcileConfigMaps()
	for _, model := range []*v1beta1.BaseModel{task.BaseModel, other} {
		require.NoError(t, s.processTask(&GopherTask{TaskType: Evict, BaseModel: model}))
	}
	require.NoDirExists(t, first.Parent.LocalPath)
}

func TestSharedEvictionClearsParsedOwnership(t *testing.T) {
	s, task, first := newSharedEvictionFixture(t)
	ctx := context.Background()
	second := first
	second.ChildModelKey, second.ChildModelUID, second.ChildModelPath = "default.basemodel.second", "second-uid", filepath.Join(first.ModelStoreRoot, "second")
	seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, second)
	require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), second))
	require.NoError(t, s.configMapReconciler.ReconcileModelMetadata(ctx, &ConfigMapMetadataOp{BaseModel: task.BaseModel, ModelMetadata: ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{first.Parent.Key: first.Parent.LocalPath}}}}))
	other := task.BaseModel.DeepCopy()
	other.Name, other.UID, other.Spec.Storage.Path = "second", second.ChildModelUID, &second.ChildModelPath
	_, err := s.omeClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.processTask(task))
	require.NoError(t, s.processTask(&GopherTask{TaskType: Evict, BaseModel: other}))
	require.NoDirExists(t, first.Parent.LocalPath, "a completed eviction must not retain dead parent ownership in parsed metadata")
}

func TestSharedEvictionSuppressesStartupRepairReady(t *testing.T) {
	for _, marker := range []bool{false, true} {
		t.Run(map[bool]string{false: "interrupted writer", true: "completed repair marker"}[marker], func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			ctx := context.Background()
			h := s.sharedHfArtifactHandler()
			// Model was resident when repair began, and became evicted while
			// that old process was preparing its completion marker.
			model := task.BaseModel.DeepCopy()
			delete(model.Annotations, artifactResidencyAnnotation)
			_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(ctx, model, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
			parent, acquired, err := h.repository.TryAcquireLockForRepair(ctx, input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			parent, err = h.repository.MarkChildrenFailedForRepair(ctx, parent)
			require.NoError(t, err)
			if marker {
				require.NoError(t, h.files.WriteParentReadyMarker(parent))
			}
			_, err = s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			label, err := getModelLabelKey(&NodeLabelOp{BaseModel: model})
			require.NoError(t, err)
			node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			delete(node.Labels, label)
			_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, newHfArtifactStartup(h).recover(ctx))
			node, err = s.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotEqual(t, string(Ready), node.Labels[label])
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.NotEqual(t, ModelStatusReady, entry.Status)
			require.NoError(t, s.processTask(task))
			require.NoDirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedEvictionOwnershipLookupFailure(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	s.omeClient.(*omefake.Clientset).PrependReactor("list", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("ownership lookup failed")
	})
	require.Error(t, s.processTask(task))
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	require.DirExists(t, input.Parent.LocalPath)
}

func TestSharedEvictionBorrowerPathSpellings(t *testing.T) {
	for _, spelling := range []string{"relative", "alias", "trailing slash", "alias pending missing child"} {
		t.Run(spelling, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			path := input.ChildModelPath
			switch spelling {
			case "relative":
				cwd, err := os.Getwd()
				require.NoError(t, err)
				path, err = filepath.Rel(cwd, path)
				require.NoError(t, err)
			case "trailing slash":
				path += "/"
			default:
				alias := filepath.Join(t.TempDir(), "alias")
				require.NoError(t, os.Symlink(input.ModelStoreRoot, alias))
				path = filepath.Join(alias, filepath.Base(input.ChildModelPath))
			}
			other := task.BaseModel.DeepCopy()
			other.Name, other.UID = "borrower", "borrower-uid"
			other.Spec.Storage.StorageUri = ptr("local://" + path)
			other.Spec.Storage.Path = nil
			_, err := s.omeClient.OmeV1beta1().BaseModels(other.Namespace).Create(context.Background(), other, metav1.CreateOptions{})
			require.NoError(t, err)
			if spelling == "alias pending missing child" {
				// A missing final component still has an aliased parent directory.
				require.NoError(t, os.Remove(input.ChildModelPath))
				_, err = s.sharedHfArtifactHandler().repository.removeModelReference(context.Background(), input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
				require.NoError(t, err)
			}
			require.NoError(t, s.processTask(task))
			require.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedEvictionDeletedCachedReadyDoesNotBlockRecovery(t *testing.T) {
	s, task, _ := newSharedEvictionFixture(t)
	ctx := context.Background()
	other := task.BaseModel.DeepCopy()
	other.Name, other.UID = "deleted", "deleted-uid"
	delete(other.Annotations, artifactResidencyAnnotation)
	_, err := s.omeClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: other, ModelStatus: ModelStatusReady}))
	require.NoError(t, s.omeClient.OmeV1beta1().BaseModels(other.Namespace).Delete(ctx, other.Name, metav1.DeleteOptions{}))
	require.NoError(t, s.kubeClient.CoreV1().ConfigMaps(s.configMapReconciler.namespace).Delete(ctx, s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
	s.configMapReconciler.reconcileConfigMaps()
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(other, nil))
	require.NoError(t, err)
	require.NotEqual(t, ModelStatusReady, entry.Status)
	require.NoError(t, s.processTask(task))
}

func TestSharedEvictionFinalCASRequiresExactReceipt(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	client := s.kubeClient.(*fake.Clientset)
	injected := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if entry.Status != ModelStatusEvicted || injected {
			return false, nil, nil
		}
		injected = true
		object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
		require.NoError(t, err)
		changed := object.(*corev1.ConfigMap)
		entry, err = existingModelEntry(changed.Data, input.ChildModelKey)
		require.NoError(t, err)
		entry.HfArtifactPendingDeletion.ChildPath = filepath.Join(input.ModelStoreRoot, "replacement-receipt")
		_, err = writeModelEntry(changed.Data, input.ChildModelKey, entry)
		require.NoError(t, err)
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("receipt replaced"))
	})
	require.Error(t, s.processTask(task))
	require.True(t, injected)
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.NotEqual(t, ModelStatusEvicted, entry.Status)
	require.Equal(t, filepath.Join(input.ModelStoreRoot, "replacement-receipt"), entry.HfArtifactPendingDeletion.ChildPath)
}

func TestSharedEvictionIntentChangedBeforeDownload(t *testing.T) {
	s, eviction, _ := newSharedEvictionFixture(t)
	model := eviction.BaseModel.DeepCopy()
	model.Spec.Storage.StorageUri = ptr("hf://org/model")
	delete(model.Annotations, artifactResidencyAnnotation)
	_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}
	s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		changed := model.DeepCopy()
		changed.Annotations[artifactResidencyAnnotation] = string(ModelStatusEvicted)
		_, err := s.omeClient.OmeV1beta1().BaseModels(changed.Namespace).Update(context.Background(), changed, metav1.UpdateOptions{})
		require.NoError(t, err)
		return false, nil, nil
	})
	writes := 0
	err = s.processTaskWithSourceAdapters(&GopherTask{TaskType: Download, BaseModel: model}, true, func(context.Context, *GopherTask, v1beta1.BaseModelSpec, bool) (bool, error) {
		writes++
		return true, nil
	}, nil)
	require.Error(t, err)
	require.Zero(t, writes, "new eviction intent must prevent starting the source writer")
}

func TestSharedEvictionPreservesAliasesToChild(t *testing.T) {
	for _, liveLocal := range []bool{false, true} {
		t.Run(map[bool]string{false: "filesystem alias", true: "live Local alias"}[liveLocal], func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			alias := filepath.Join(input.ModelStoreRoot, "borrower-alias")
			require.NoError(t, os.Symlink(input.ChildModelPath, alias))
			if liveLocal {
				borrower := task.BaseModel.DeepCopy()
				borrower.Name, borrower.UID = "borrower", "borrower-uid"
				delete(borrower.Annotations, artifactResidencyAnnotation)
				borrower.Spec.Storage.StorageUri, borrower.Spec.Storage.Path = ptr("local://"+alias), nil
				_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(context.Background(), borrower, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			require.NoError(t, s.processTask(task))
			_, err := os.Stat(alias)
			require.NoError(t, err, "alias must remain usable after Shared eviction")
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			require.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedEvictionReadyRetryChecksCurrentIntent(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(map[bool]string{false: "download publication", true: "repair publication"}[repair], func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			ctx := context.Background()
			resident := task.BaseModel.DeepCopy()
			delete(resident.Annotations, artifactResidencyAnnotation)
			_, err := s.omeClient.OmeV1beta1().BaseModels(resident.Namespace).Update(ctx, resident, metav1.UpdateOptions{})
			require.NoError(t, err)
			node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			key, err := getModelLabelKey(&NodeLabelOp{BaseModel: resident})
			require.NoError(t, err)
			node.Labels[key] = string(Updating)
			_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.nodeLabelReconciler.opRetry = 2
			attempts := 0
			s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
				attempts++
				if attempts == 1 {
					_, err := s.omeClient.OmeV1beta1().BaseModels(resident.Namespace).Update(ctx, task.BaseModel.DeepCopy(), metav1.UpdateOptions{})
					require.NoError(t, err)
					return true, nil, errors.New("retry after live intent changed")
				}
				return false, nil, nil
			})
			if repair {
				err = s.updateHfArtifactChildLabels(ctx, map[string]ModelStatus{input.ChildModelKey: ModelStatusReady})
			} else {
				err = s.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: resident, ModelStateOnNode: Ready})
			}
			node, getErr := s.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.NotEqual(t, string(Ready), node.Labels[key], "a retry must recheck live intent before publication")
			require.Error(t, err)
			require.Equal(t, 1, attempts, "changed intent must prevent a second Ready patch")
		})
	}
}

func TestSharedEvictionPreservesAliasToChildDescendant(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	require.NoError(t, os.Mkdir(filepath.Join(input.Parent.LocalPath, "borrowed-subdir"), 0755))
	alias := filepath.Join(input.ModelStoreRoot, "borrower-alias")
	require.NoError(t, os.Symlink(filepath.Join(input.ChildModelPath, "borrowed-subdir"), alias))
	borrower := task.BaseModel.DeepCopy()
	borrower.Name, borrower.UID = "borrower", "borrower-uid"
	delete(borrower.Annotations, artifactResidencyAnnotation)
	borrower.Spec.Storage.StorageUri, borrower.Spec.Storage.Path = ptr("local://"+alias), nil
	_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(context.Background(), borrower, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = os.Stat(alias)
	require.NoError(t, err)
	require.NoError(t, s.processTask(task))
	_, err = os.Stat(alias)
	require.NoError(t, err, "alias through a child subdirectory must remain usable")
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
}
