package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestHfArtifactInterruptedDownloadDeleteUsesOriginalIdentity(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{false: "partial download", true: "ready before attach"}[ready], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			client := s.configMapReconciler.kubeClient.(*fake.Clientset)
			if ready {
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					child, err := existingModelEntry(cm.Data, input.ChildModelKey)
					require.NoError(t, err)
					if child.HfArtifactKey != "" {
						return true, nil, context.Canceled
					}
					return false, nil, nil
				})
			}
			result, err := h.handleDownload(context.Background(), input, func(path string) error {
				require.NoError(t, writeTestHfArtifactFiles(path))
				if !ready {
					return context.Canceled
				}
				return nil
			})
			if ready {
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			parent, found, err := h.repository.Get(context.Background(), input.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			require.Empty(t, parent.Children)
			if ready {
				require.Equal(t, HfArtifactStatusReady, parent.Status)
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			}
			// The delete must use the original record, even after a process restart
			// and an update that removes reuse metadata and changes the destination.
			s = &Gopher{configMapReconciler: NewConfigMapReconciler(s.configMapReconciler.nodeName,
				s.configMapReconciler.namespace, client, s.logger), modelRootDir: s.modelRootDir,
				logger: s.logger, baseModelLister: s.baseModelLister, clusterBaseModelLister: s.clusterBaseModelLister}
			replacement := filepath.Join(input.ModelStoreRoot, "replacement")
			require.NoError(t, os.Mkdir(replacement, 0o755))
			task.BaseModel = task.BaseModel.DeepCopy()
			task.BaseModel.Annotations = nil
			task.BaseModel.Spec.Storage.Path = &replacement
			task.TaskType, task.NodeIneligible = Delete, true
			require.NoError(t, s.loadArtifactRouting(context.Background()))
			s.artifactRouting.mutex.Lock()
			s.routeArtifactTaskLocked(task)
			s.artifactRouting.mutex.Unlock()
			for attempt := 0; attempt < 2; attempt++ {
				handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
				require.NoError(t, err)
				require.True(t, handled)
				require.False(t, waiting)
				assert.NoDirExists(t, input.Parent.LocalPath)
				assertChildPathMissing(t, input.ChildModelPath)
				assert.DirExists(t, replacement)
				_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
				require.NoError(t, err)
				assert.False(t, found)
			}
		})
	}
}

func TestHfArtifactInterruptedDownloadDeleteRetriesParentRemoval(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	_, err := h.handleDownload(context.Background(), input, func(path string) error {
		require.NoError(t, writeTestHfArtifactFiles(path))
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	fail := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if _, present := cm.Data[input.Parent.Key]; !present && fail {
			return true, nil, errors.New("parent removal unavailable")
		}
		return false, nil, nil
	})
	task.TaskType, task.SharedArtifact = Delete, true
	handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, waiting)
	require.NoDirExists(t, input.Parent.LocalPath)
	fail = false
	handled, waiting, err = s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.True(t, handled)
	require.False(t, waiting)
	_, found, err := h.repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found)
}

func interruptTestHfArtifactDownload(t *testing.T, h *hfArtifactTaskHandler, input hfArtifactTaskInput) {
	t.Helper()
	_, err := h.handleDownload(context.Background(), input, func(path string) error {
		require.NoError(t, writeTestHfArtifactFiles(path))
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestHfArtifactInterruptedDownloadProtectsConsumers(t *testing.T) {
	for _, consumer := range []string{"attached", "pending download", "unrecorded link", "reserve"} {
		t.Run(consumer, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			interruptTestHfArtifactDownload(t, h, input)
			other := testHfArtifactTaskInput(t, input.ModelStoreRoot, "other")
			switch consumer {
			case "attached":
				seedTestChildModelEntry(t, h.repository, other)
				require.NoError(t, runTestHfArtifactDownload(h, other))
			case "pending download":
				seedTestChildModelEntry(t, h.repository, other)
				interruptTestHfArtifactDownload(t, h, other)
			case "unrecorded link":
				require.NoError(t, os.Symlink(input.Parent.LocalPath, other.ChildModelPath))
			case "reserve":
				task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
			}
			task.TaskType, task.SharedArtifact = Delete, true
			handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
			require.NoError(t, err)
			require.True(t, handled)
			require.False(t, waiting)
			assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
			parent, found, err := h.repository.Get(context.Background(), input.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			assert.Empty(t, parent.LockID)
			if consumer == "pending download" {
				task.BaseModel = task.BaseModel.DeepCopy()
				task.BaseModel.Name, task.BaseModel.UID = "other", other.ChildModelUID
				task.BaseModel.Spec.Storage.Path = &other.ChildModelPath
				_, waiting, err = s.processSharedHfArtifactDelete(context.Background(), task)
				require.NoError(t, err)
				require.False(t, waiting)
				assert.NoDirExists(t, input.Parent.LocalPath)
			}
		})
	}
}

func TestHfArtifactInterruptedDownloadWaitsForActiveOwner(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	started, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := h.handleDownload(context.Background(), input, func(path string) error {
			if err := writeTestHfArtifactFiles(path); err != nil {
				return err
			}
			close(started)
			<-release
			return context.Canceled
		})
		finished <- err
	}()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	task.TaskType, task.SharedArtifact = Delete, true
	_, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.True(t, waiting)
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
	close(release)
	require.ErrorIs(t, <-finished, context.Canceled)
	_, waiting, err = s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestHfArtifactInterruptedDownloadRetryClearsIntent(t *testing.T) {
	s, _, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	interruptTestHfArtifactDownload(t, h, input)
	require.NoError(t, runTestHfArtifactDownload(h, input))
	pending, err := h.repository.pendingDownload(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	assert.Nil(t, pending)
	assertParentEntryReady(t, h.repository, input.Parent, map[string]string{input.ChildModelKey: input.ChildModelPath})
}

func TestHfArtifactInterruptedDownloadRetryPreservesReusedParentPath(t *testing.T) {
	for _, allowCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "high priority", true: "normal priority"}[allowCleanup], func(t *testing.T) {
			s, task, original := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			first := testHfArtifactTaskInput(t, filepath.Join(original.ModelStoreRoot, "store-a"), "first")
			first.ModelStoreRoot = original.ModelStoreRoot
			seedTestChildModelEntry(t, h.repository, first)
			require.NoError(t, runTestHfArtifactDownload(h, first))

			second := testHfArtifactTaskInput(t, filepath.Join(original.ModelStoreRoot, "store-b"), "second")
			second.ModelStoreRoot = original.ModelStoreRoot
			seedTestChildModelEntry(t, h.repository, second)
			task.BaseModel.Name, task.BaseModel.UID = "second", second.ChildModelUID
			task.BaseModel.Spec.Storage.Path = &second.ChildModelPath
			task.SharedArtifact = true
			client := s.configMapReconciler.kubeClient.(*fake.Clientset)
			failAttach := true
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				child, err := existingModelEntry(cm.Data, second.ChildModelKey)
				require.NoError(t, err)
				if failAttach && child.HfArtifactKey != "" {
					return true, nil, errors.New("attach unavailable")
				}
				return false, nil, nil
			})
			noDownload := func(string) error {
				t.Fatal("retry must reuse the completed parent without downloading")
				return nil
			}
			result, err := s.runHfArtifactDownload(ctx, task, second, true, nil, noDownload)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			failAttach = false
			pending, err := h.repository.pendingDownload(ctx, second.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, pending)
			require.Equal(t, first.Parent.LocalPath, pending.ParentPath)

			// The remaining intent protects the completed copy after its last
			// registered child is deleted, even though it lives in another store.
			result, err = h.handleDelete(ctx, first)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			require.DirExists(t, first.Parent.LocalPath)
			retry, eligible, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, s.modelRootDir)
			require.NoError(t, err)
			require.True(t, eligible)
			require.NotEqual(t, pending.ParentPath, retry.Parent.LocalPath)
			waiting, err := s.cleanupHfArtifactPendingDownload(ctx, task, &retry, allowCleanup)
			require.NoError(t, err)
			require.False(t, waiting)
			require.False(t, task.NormalPriorityOnly)
			require.FileExists(t, filepath.Join(first.Parent.LocalPath, "config.json"))
			assertChildSymlinkTarget(t, second.ChildModelPath, first.Parent.LocalPath)
			remaining, err := h.repository.pendingDownload(ctx, second.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, pending, remaining)

			result, err = s.runHfArtifactDownload(ctx, task, retry, allowCleanup, nil, noDownload)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			remaining, err = h.repository.pendingDownload(ctx, second.ChildModelKey)
			require.NoError(t, err)
			assert.Nil(t, remaining)
			assertParentEntryReady(t, h.repository, first.Parent, map[string]string{second.ChildModelKey: second.ChildModelPath})
			assertChildSymlinkTarget(t, second.ChildModelPath, first.Parent.LocalPath)
			assert.NoDirExists(t, second.Parent.LocalPath)
		})
	}
}

func TestHfArtifactInterruptedDownloadChangedTargetCleansParent(t *testing.T) {
	for _, change := range []string{"identity", "child path", "UID"} {
		t.Run(change, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			interruptTestHfArtifactDownload(t, h, input)
			switch change {
			case "identity":
				task.BaseModel.Annotations[hfModelIDAnnotationKey] = "Qwen/another-model"
				uri := "oci://n/ns/b/models/o/artifacts/Qwen/another-model/" + input.Parent.Identity.CommitSHA
				task.BaseModel.Spec.Storage.StorageUri = &uri
			case "child path":
				path := filepath.Join(input.ModelStoreRoot, "replacement")
				task.BaseModel.Spec.Storage.Path = &path
			case "UID":
				task.BaseModel.UID = "replacement-uid"
			}
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			require.NoError(t, indexer.Add(task.BaseModel))
			s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			next, eligible, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, s.modelRootDir)
			require.NoError(t, err)
			require.True(t, eligible)
			waiting, err := s.cleanupHfArtifactPendingDownload(context.Background(), task, &next, true)
			require.NoError(t, err)
			require.False(t, waiting)
			assert.NoDirExists(t, input.Parent.LocalPath)
			pending, err := h.repository.pendingDownload(context.Background(), input.ChildModelKey)
			require.NoError(t, err)
			assert.Nil(t, pending)
		})
	}
}

func TestHfArtifactInterruptedDownloadOptOutCleansOldParent(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	handled, waiting, err := s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, false)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, waiting)
	require.DirExists(t, input.Parent.LocalPath)
	handled, waiting, err = s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.False(t, handled)
	assert.False(t, waiting)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestHfArtifactInterruptedDownloadUIDReplacement(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
	replacement := task.BaseModel.DeepCopy()
	replacement.UID = "new-uid"
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(replacement))
	s.baseModelLister = modelslister.NewBaseModelLister(indexer)
	task.TaskType, task.SharedArtifact = Delete, true
	_, _, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.Error(t, err)
	require.DirExists(t, input.Parent.LocalPath)
	task.BaseModel = replacement
	_, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestHfArtifactInterruptedDownloadSurvivesConfigMapLoss(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
	c := s.configMapReconciler
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(context.Background(), c.nodeName, metav1.DeleteOptions{}))
	c.reconcileConfigMaps()
	task.TaskType, task.SharedArtifact = Delete, true
	_, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestHfArtifactInterruptedDownloadDeleteBeforeConfigMapRecovery(t *testing.T) {
	for _, window := range []string{"before delete", "after receipt"} {
		t.Run(window, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			ctx := context.Background()
			interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
			c := s.configMapReconciler
			s.nodeLabelReconciler = NewNodeLabelReconciler(c.nodeName, c.kubeClient, 1, s.logger)
			client := c.kubeClient.(*fake.Clientset)
			dropped := false
			if window == "before delete" {
				require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
				dropped = true
			} else {
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					if dropped {
						return false, nil, nil
					}
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					child, err := existingModelEntry(cm.Data, input.ChildModelKey)
					require.NoError(t, err)
					if child.HfArtifactPendingDeletion == nil {
						return false, nil, nil
					}
					// Lose the ConfigMap after receipt publication succeeds, before
					// the next cleanup read. No periodic recovery runs in between.
					dropped = true
					require.NoError(t, client.Tracker().Update(action.GetResource(), cm, cm.Namespace))
					require.NoError(t, client.Tracker().Delete(action.GetResource(), cm.Namespace, cm.Name))
					return true, cm, nil
				})
			}
			task.TaskType, task.SharedArtifact, task.NodeIneligible = Delete, true, true
			require.NoError(t, s.processTask(task))
			require.True(t, dropped)
			require.NoDirExists(t, input.Parent.LocalPath)
			c.reconcileConfigMaps()
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.NotContains(t, cm.Data, input.ChildModelKey)
			assert.NotContains(t, cm.Data, input.Parent.Key)
		})
	}
}

func TestHfArtifactInterruptedDownloadDeleteRetriesConfigMapRecovery(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "response lost"}[committed], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			ctx := context.Background()
			interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
			c := s.configMapReconciler
			s.nodeLabelReconciler = NewNodeLabelReconciler(c.nodeName, c.kubeClient, 1, s.logger)
			s.samePathWaitDelay = time.Millisecond
			client := c.kubeClient.(*fake.Clientset)
			require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
			injected := false
			client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if injected {
					return false, nil, nil
				}
				injected = true
				if committed {
					cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
					require.NoError(t, client.Tracker().Create(action.GetResource(), cm, cm.Namespace))
				}
				return true, nil, errors.New("recovery response unavailable")
			})
			task.TaskType, task.SharedArtifact, task.NodeIneligible = Delete, true, true
			require.NoError(t, s.processTask(task))
			require.True(t, injected)
			require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
			models, _ := c.cachedConfigMapEntries()
			child, err := existingModelEntry(models, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, child.HfArtifactPendingDownload)
			select {
			case retried := <-s.gopherChan:
				require.Same(t, task, retried)
			case <-time.After(time.Second):
				t.Fatal("failed ConfigMap recovery must requeue Delete")
			}
			require.NoError(t, s.processTask(task))
			require.NoDirExists(t, input.Parent.LocalPath)
			c.reconcileConfigMaps()
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.NotContains(t, cm.Data, input.ChildModelKey)
			assert.NotContains(t, cm.Data, input.Parent.Key)
		})
	}
}

func TestHfArtifactInterruptedDownloadIntentPrecedesFiles(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "response lost"}[committed], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			client := s.configMapReconciler.kubeClient.(*fake.Clientset)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				child, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if injected || child.HfArtifactPendingDownload == nil {
					return false, nil, nil
				}
				injected = true
				if committed {
					require.NoError(t, client.Tracker().Update(action.GetResource(), cm, cm.Namespace))
				}
				return true, nil, errors.New("download intent response unavailable")
			})
			result, err := h.handleDownload(context.Background(), input, func(string) error {
				t.Fatal("unconfirmed intent must not authorize file writes")
				return nil
			})
			require.NoError(t, err)
			require.True(t, injected)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.NoDirExists(t, input.Parent.LocalPath)
			if committed {
				task.TaskType, task.SharedArtifact = Delete, true
				_, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
				require.NoError(t, err)
				require.False(t, waiting)
			} else {
				require.NoError(t, runTestHfArtifactDownload(h, input))
			}
		})
	}
}

func TestHfArtifactInterruptedDownloadCleanupUncertainWrites(t *testing.T) {
	for _, phase := range []string{"receipt", "lock", "parent removal"} {
		for _, committed := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "/rejected", true: "/response lost"}[committed], func(t *testing.T) {
				s, task, input := newTestHfArtifactGopher(t)
				interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
				client := s.configMapReconciler.kubeClient.(*fake.Clientset)
				injected := false
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					child, err := existingModelEntry(cm.Data, input.ChildModelKey)
					require.NoError(t, err)
					_, parentExists := cm.Data[input.Parent.Key]
					pending := child.HfArtifactPendingDeletion
					matches := pending != nil && (phase == "receipt" && pending.ParentLockID == "" ||
						phase == "lock" && parentExists && pending.ParentLockID != "" ||
						phase == "parent removal" && !parentExists)
					if injected || !matches {
						return false, nil, nil
					}
					injected = true
					if committed {
						require.NoError(t, client.Tracker().Update(action.GetResource(), cm, cm.Namespace))
					}
					return true, nil, errors.New("cleanup response unavailable")
				})
				task.TaskType, task.SharedArtifact = Delete, true
				_, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
				require.NoError(t, err)
				require.True(t, injected)
				require.True(t, waiting)
				if phase != "parent removal" {
					require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
				} else {
					require.NoDirExists(t, input.Parent.LocalPath)
				}
				// Retry with no process-local ownership state.
				s = &Gopher{configMapReconciler: NewConfigMapReconciler(s.configMapReconciler.nodeName,
					s.configMapReconciler.namespace, client, s.logger), modelRootDir: s.modelRootDir,
					logger: s.logger, baseModelLister: s.baseModelLister, clusterBaseModelLister: s.clusterBaseModelLister}
				_, waiting, err = s.processSharedHfArtifactDelete(context.Background(), task)
				require.NoError(t, err)
				require.False(t, waiting)
				assert.NoDirExists(t, input.Parent.LocalPath)
				_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
				require.NoError(t, err)
				assert.False(t, found)
			})
		}
	}
}

func TestHfArtifactInterruptedDownloadFailedPublicationRecovers(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if raw, exists := cm.Data[input.Parent.Key]; exists {
			parent, err := decodeHfArtifactEntry(input.Parent.Key, raw)
			require.NoError(t, err)
			if parent.Status == HfArtifactStatusFailed {
				return true, nil, errors.New("Failed publication unavailable")
			}
		}
		return false, nil, nil
	})
	result, err := h.handleDownload(context.Background(), input, func(path string) error {
		require.NoError(t, writeTestHfArtifactFiles(path))
		return context.Canceled
	})
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	parent, found, err := h.repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, HfArtifactStatusUpdating, parent.Status)
	client.ReactionChain = client.ReactionChain[1:]
	s = &Gopher{configMapReconciler: NewConfigMapReconciler(s.configMapReconciler.nodeName,
		s.configMapReconciler.namespace, client, s.logger), modelRootDir: s.modelRootDir,
		logger: s.logger, baseModelLister: s.baseModelLister, clusterBaseModelLister: s.clusterBaseModelLister}
	task.TaskType, task.SharedArtifact = Delete, true
	_, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestHfArtifactInterruptedDownloadUnresolvedRevisionPreservesIntent(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
	source.resolve = func(context.Context, string, string, string, string) (string, error) {
		return "", errors.New("revision unavailable")
	}
	waiting, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.True(t, waiting)
	assert.Zero(t, *downloads)
	assert.DirExists(t, input.Parent.LocalPath)
	pending, err := s.sharedHfArtifactHandler().repository.pendingDownload(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, input.Parent.Identity, pending.Identity)
}
