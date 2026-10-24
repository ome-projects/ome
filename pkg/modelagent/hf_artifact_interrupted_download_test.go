package modelagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeinformers "sigs.k8s.io/ome/pkg/client/informers/externalversions"
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
	parent := testRepairParent(t, h, input)
	require.Equal(t, HfArtifactStatusUpdating, parent.Status)
	require.Empty(t, parent.LockID)
}

func TestHfArtifactCancellationKeepsParentRetryable(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(fmt.Sprint(wrapped), func(t *testing.T) {
			s, _, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := h.handleDownload(ctx, input, func(path string) error {
				require.NoError(t, writeTestHfArtifactFiles(path))
				cancel()
				if wrapped {
					return fmt.Errorf("download interrupted: %w", ctx.Err())
				}
				return ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
			parent := testRepairParent(t, h, input)
			require.Equal(t, HfArtifactStatusUpdating, parent.Status)
			require.Empty(t, parent.LockID)
			require.NotEmpty(t, parent.LastCompletedLockID)
			require.FileExists(t, filepath.Join(parent.LocalPath, "config.json"))
			// A new process must not turn a cancellation into Failed, and a
			// retry must acquire a fresh owner instead of waiting indefinitely.
			restarted := newHfArtifactTaskHandler(newHfArtifactRepository(NewConfigMapReconciler(
				s.configMapReconciler.nodeName, s.configMapReconciler.namespace,
				s.configMapReconciler.kubeClient, s.logger)))
			require.NoError(t, newHfArtifactStartup(restarted).recover(context.Background()))
			parent = testRepairParent(t, restarted, input)
			require.Equal(t, HfArtifactStatusUpdating, parent.Status)
			require.Empty(t, parent.LockID)
			require.NoError(t, runTestHfArtifactDownload(restarted, input))
			assertParentEntryReady(t, restarted.repository, input.Parent,
				map[string]string{input.ChildModelKey: input.ChildModelPath})
		})
	}
}

func TestHfArtifactCancellationDoesNotHideDownloadErrors(t *testing.T) {
	for _, failure := range []error{errors.New("download failed"), context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			s, _, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := h.handleDownload(ctx, input, func(string) error {
				cancel()
				return failure
			})
			require.ErrorIs(t, err, failure)
			parent := testRepairParent(t, h, input)
			assert.Equal(t, HfArtifactStatusFailed, parent.Status)
			assert.Empty(t, parent.LockID)
		})
	}
}

func TestHfOCIArtifactCancellationKeepsParentRetryable(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	s.downloadRetry = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	canceled := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if raw, exists := cm.Data[input.Parent.Key]; exists && !canceled {
			parent, err := decodeHfArtifactEntry(input.Parent.Key, raw)
			require.NoError(t, err)
			if parent.Status == HfArtifactStatusUpdating && parent.LockID != "" {
				canceled = true
				cancel()
			}
		}
		return false, nil, nil
	})

	handled, _, err := s.processHfOCIArtifact(ctx, task, task.BaseModel.Spec, true)
	require.True(t, handled)
	require.True(t, canceled)
	assert.ErrorIs(t, err, context.Canceled)
	parent := testRepairParent(t, s.sharedHfArtifactHandler(), input)
	assert.Equal(t, HfArtifactStatusUpdating, parent.Status)
	assert.Empty(t, parent.LockID)
	assert.NotEmpty(t, parent.LastCompletedLockID)
}

func TestHfArtifactDownloadRechecksCompletedParentAfterConcurrentValidation(t *testing.T) {
	for _, cause := range []error{context.Canceled, errors.New("validation failed")} {
		t.Run(cause.Error(), func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			// This parent is created after startup, so reuse has no pending
			// startup validation that could mask a stale routing decision.
			require.NoError(t, s.hfArtifactStartup.recover(ctx))
			parent, acquired, err := h.repository.TryAcquireLock(ctx, input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
			require.NoError(t, h.files.WriteParentReadyMarker(parent))
			require.NoError(t, h.repository.MarkReady(ctx, parent))
			sibling := testHfArtifactTaskInput(t, input.ModelStoreRoot, "sibling")
			seedTestChildModelEntry(t, h.repository, sibling)
			readyCM, err := h.repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)
			// Capture the real persisted state after B releases its validation
			// lock without modifying the complete parent files.
			_, err = h.handleDownloadOverride(ctx, sibling,
				func(string) (bool, error) { return false, cause }, nil)
			require.ErrorIs(t, err, cause)
			releasedCM, err := h.repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)
			client := s.configMapReconciler.kubeClient.(*fake.Clientset)
			resource := corev1.SchemeGroupVersion.WithResource("configmaps")
			require.NoError(t, client.Tracker().Update(resource, readyCM, readyCM.Namespace))
			gets, injected := 0, false
			client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				gets++
				if gets != 2 {
					return false, nil, nil
				}
				// A's routing read sees Ready. B releases its lock before A's
				// handler acquires the parent lock and reads current state.
				injected = true
				require.NoError(t, client.Tracker().Update(resource, releasedCM, releasedCM.Namespace))
				return true, readyCM.DeepCopy(), nil
			})
			validations, downloads := 0, 0
			validate := func(string) (bool, error) { validations++; return true, nil }
			download := func(path string) error { downloads++; return writeTestHfArtifactFiles(path) }
			result, err := s.runHfArtifactDownload(ctx, task, input, true, validate, download)
			require.NoError(t, err)
			require.True(t, injected)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.Zero(t, downloads)
			require.FileExists(t, filepath.Join(parent.LocalPath, "config.json"))

			result, err = s.runHfArtifactDownload(ctx, task, input, true, validate, download)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			assert.Equal(t, 1, validations)
			assert.Zero(t, downloads)
			assertParentEntryReady(t, h.repository, input.Parent,
				map[string]string{input.ChildModelKey: input.ChildModelPath})
		})
	}
}

func TestHfArtifactStartupPreservesDeletionOwner(t *testing.T) {
	for _, filesRemoved := range []bool{false, true} {
		t.Run(fmt.Sprint(filesRemoved), func(t *testing.T) {
			s, _, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			require.NoError(t, runTestHfArtifactDownload(h, input))
			parent := testRepairParent(t, h, input)
			removed, err := h.repository.removeModelReference(ctx, parent, input.ChildModelKey,
				input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			require.True(t, removed.LastReferenceRemoved)
			if filesRemoved {
				require.NoError(t, os.RemoveAll(parent.LocalPath))
			} else {
				require.NoError(t, os.Remove(filepath.Join(parent.LocalPath, "config.json")))
			}
			restarted := newHfArtifactTaskHandler(newHfArtifactRepository(NewConfigMapReconciler(
				s.configMapReconciler.nodeName, s.configMapReconciler.namespace,
				s.configMapReconciler.kubeClient, s.logger)))
			require.NoError(t, newHfArtifactStartup(restarted).recover(ctx))
			parent = testRepairParent(t, restarted, input)
			require.Equal(t, removed.Artifact.LockID, parent.LockID)
			require.Equal(t, HfArtifactStatusUpdating, parent.Status)
			// B cannot start a download or register an intent that would block
			// A's already committed parent deletion.
			other := testHfArtifactTaskInput(t, input.ModelStoreRoot, "other")
			seedTestChildModelEntry(t, restarted.repository, other)
			result, err := restarted.handleDownload(ctx, other, func(string) error {
				t.Fatal("download must wait for parent deletion")
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			pending, err := restarted.repository.pendingDownload(ctx, other.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, pending)
			result, err = restarted.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			require.NoDirExists(t, parent.LocalPath)
			require.NoError(t, runTestHfArtifactDownload(restarted, other))
			assertParentEntryReady(t, restarted.repository, other.Parent,
				map[string]string{other.ChildModelKey: other.ChildModelPath})
		})
	}
}

func TestHfArtifactStartupResumesPlacementCleanup(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, stage := range []string{"pending download", "deletion locked", "parent removed"} {
			t.Run(fmt.Sprintf("cluster=%v/%s", cluster, stage), func(t *testing.T) {
				ctx := context.Background()
				s, task, input := newTestHfArtifactGopher(t)
				if cluster {
					task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
					task.ClusterBaseModel.Namespace = ""
					task.BaseModel = nil
					input.ChildModelKey = getModelID(nil, task.ClusterBaseModel)
					seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, input)
				}
				h := s.sharedHfArtifactHandler()
				if stage == "pending download" {
					interruptTestHfArtifactDownload(t, h, input)
				} else {
					require.NoError(t, runTestHfArtifactDownload(h, input))
					parent := testRepairParent(t, h, input)
					removal, err := h.repository.removeModelReference(ctx, parent, input.ChildModelKey,
						input.ChildModelUID, input.ChildModelPath, true)
					require.NoError(t, err)
					require.True(t, removal.LastReferenceRemoved)
					if stage == "parent removed" {
						require.NoError(t, os.Remove(input.ChildModelPath))
						require.NoError(t, os.RemoveAll(parent.LocalPath))
					}
				}

				var model runtime.Object
				if cluster {
					moved := task.ClusterBaseModel.DeepCopy()
					moved.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
					model = moved
				} else {
					moved := task.BaseModel.DeepCopy()
					moved.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
					model = moved
				}
				c := NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace,
					s.configMapReconciler.kubeClient, s.logger)
				scout, tasks, _ := startHfArtifactScout(t, c, model)
				var cleanup *GopherTask
				select {
				case cleanup = <-tasks:
				case <-time.After(time.Second):
					t.Fatal("startup did not replay the interrupted placement cleanup")
				}
				require.Equal(t, Delete, cleanup.TaskType)
				require.True(t, cleanup.NodeIneligible)
				require.True(t, cleanup.SharedArtifact)
				require.Equal(t, input.ChildModelKey, getModelID(cleanup.BaseModel, cleanup.ClusterBaseModel))
				_, err := c.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName}}, metav1.CreateOptions{})
				require.NoError(t, err)
				restarted := &Gopher{configMapReconciler: c, modelRootDir: s.modelRootDir, logger: s.logger,
					baseModelLister: scout.baseModelLister, clusterBaseModelLister: scout.clusterBaseModelLister,
					nodeLabelReconciler: NewNodeLabelReconciler(c.nodeName, c.kubeClient, 1, s.logger)}
				require.NoError(t, restarted.processTask(cleanup))
				require.NoDirExists(t, input.Parent.LocalPath)
				assertChildPathMissing(t, input.ChildModelPath)
				cm, err := c.getConfigMap(ctx)
				require.NoError(t, err)
				require.NotContains(t, cm.Data, input.ChildModelKey)
				require.NotContains(t, cm.Data, input.Parent.Key)

				other := testHfArtifactTaskInput(t, input.ModelStoreRoot, "model-2")
				seedTestChildModelEntry(t, restarted.sharedHfArtifactHandler().repository, other)
				result, err := restarted.runHfArtifactDownload(ctx, &GopherTask{TaskType: Download}, other, true,
					func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome)
				assertParentEntryReady(t, restarted.sharedHfArtifactHandler().repository, other.Parent,
					map[string]string{other.ChildModelKey: other.ChildModelPath})
			})
		}
	}
}

func TestHfArtifactStartupCleanupDoesNotReplayLaterAdds(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	interruptTestHfArtifactDownload(t, h, input)
	moved := task.BaseModel.DeepCopy()
	moved.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
	_, tasks, client := startHfArtifactScout(t, s.configMapReconciler, moved)
	var cleanup *GopherTask
	select {
	case cleanup = <-tasks:
	case <-time.After(time.Second):
		t.Fatal("startup cleanup was not queued")
	}
	ctx := context.Background()
	handled, waiting, err := s.processSharedHfArtifactDelete(ctx, cleanup)
	require.NoError(t, err)
	require.True(t, handled)
	require.False(t, waiting)
	require.NoError(t, s.configMapReconciler.DeleteModelFromConfigMap(ctx, moved, nil))
	require.NoError(t, client.OmeV1beta1().BaseModels(moved.Namespace).Delete(ctx, moved.Name, metav1.DeleteOptions{}))
	select {
	case deleted := <-tasks:
		require.Equal(t, Delete, deleted.TaskType)
	case <-time.After(time.Second):
		t.Fatal("model deletion event was not delivered")
	}

	replacement := moved.DeepCopy()
	replacement.UID = "replacement-uid"
	_, err = client.OmeV1beta1().BaseModels(moved.Namespace).Create(ctx, replacement, metav1.CreateOptions{})
	require.NoError(t, err)
	// The following Add is delivered after the replacement's Add by the same
	// informer listener, so receiving it also proves the stale snapshot was ignored.
	sentinel := task.BaseModel.DeepCopy()
	sentinel.Name, sentinel.UID = "sentinel", "sentinel-uid"
	_, err = client.OmeV1beta1().BaseModels(sentinel.Namespace).Create(ctx, sentinel, metav1.CreateOptions{})
	require.NoError(t, err)
	select {
	case next := <-tasks:
		require.Equal(t, Download, next.TaskType)
		require.Equal(t, sentinel.Name, next.BaseModel.Name)
	case <-time.After(time.Second):
		t.Fatal("sentinel Add was not delivered")
	}
}

func TestHfArtifactStartupCleanupRequiresConfigMapRead(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, nil)
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API unavailable")
	})
	template, tasks := newScoutForUpdateTest(t)
	factory := omeinformers.NewSharedInformerFactory(omefake.NewSimpleClientset(), 0)
	scout, err := NewScout(context.Background(), template.nodeName,
		factory.Ome().V1beta1().BaseModels(), factory.Ome().V1beta1().ClusterBaseModels(),
		factory, tasks, template.kubeClient, repository.configMaps, omefake.NewSimpleClientset(), template.logger)
	require.ErrorContains(t, err, "API unavailable")
	require.Nil(t, scout)
}

func TestHfArtifactStartupKeepsEligibleModels(t *testing.T) {
	for _, missingConfigMap := range []bool{false, true} {
		t.Run(fmt.Sprint(missingConfigMap), func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
			c := s.configMapReconciler
			if missingConfigMap {
				require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(context.Background(), c.nodeName, metav1.DeleteOptions{}))
			}
			_, tasks, _ := startHfArtifactScout(t, c, task.BaseModel)
			select {
			case next := <-tasks:
				require.Equal(t, Download, next.TaskType)
				require.Equal(t, input.ChildModelUID, next.BaseModel.UID)
			case <-time.After(time.Second):
				t.Fatal("eligible model was not queued for download")
			}
		})
	}
}

func TestHfArtifactStartupCleanupWaitsForModelCaches(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	interruptTestHfArtifactDownload(t, s.sharedHfArtifactHandler(), input)
	moved := task.BaseModel.DeepCopy()
	moved.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
	scout, tasks, _ := newHfArtifactScout(t, s.configMapReconciler, moved)
	var clusterSynced atomic.Bool
	scout.clusterBaseModelSynced = clusterSynced.Load
	defer clusterSynced.Store(true)
	stop := make(chan struct{})
	scout.startupStopCh = stop
	t.Cleanup(func() { close(stop); scout.informerFactory.Shutdown() })
	scout.informerFactory.Start(stop)
	syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.True(t, cache.WaitForCacheSync(syncCtx.Done(), scout.baseModelSynced))
	select {
	case <-tasks:
		t.Fatal("cleanup was dispatched before both model caches were ready")
	case <-time.After(100 * time.Millisecond):
	}
	clusterSynced.Store(true)
	select {
	case cleanup := <-tasks:
		require.Equal(t, Delete, cleanup.TaskType)
		require.True(t, cleanup.NodeIneligible)
	case <-time.After(time.Second):
		t.Fatal("cleanup was not dispatched after both model caches were ready")
	}
}

func newHfArtifactScout(t *testing.T, c *ConfigMapReconciler, models ...runtime.Object) (*Scout, chan *GopherTask, *omefake.Clientset) {
	t.Helper()
	// The generated fake client does not implement the watch-list bookmark.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	template, tasks := newScoutForUpdateTest(t)
	client := omefake.NewSimpleClientset(models...)
	factory := omeinformers.NewSharedInformerFactory(client, 0)
	baseModels := factory.Ome().V1beta1().BaseModels()
	clusterBaseModels := factory.Ome().V1beta1().ClusterBaseModels()
	scout, err := NewScout(context.Background(), template.nodeName, baseModels, clusterBaseModels,
		factory, tasks, template.kubeClient, c, client, c.logger)
	require.NoError(t, err)
	return scout, tasks, client
}

func startHfArtifactScout(t *testing.T, c *ConfigMapReconciler, models ...runtime.Object) (*Scout, chan *GopherTask, *omefake.Clientset) {
	t.Helper()
	scout, tasks, client := newHfArtifactScout(t, c, models...)
	stop := make(chan struct{})
	scout.startupStopCh = stop
	go scout.runDeleteConfirmationWorker()
	t.Cleanup(func() {
		close(stop)
		scout.deleteQueue.ShutDown()
		scout.informerFactory.Shutdown()
	})
	scout.informerFactory.Start(stop)
	syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.True(t, cache.WaitForCacheSync(syncCtx.Done(), scout.baseModelSynced, scout.clusterBaseModelSynced))
	scout.reconcilePendingDeletions()
	return scout, tasks, client
}

func TestHfArtifactCancelledRepairPreservesSiblingSafety(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	_, err := h.handleDownloadOverride(context.Background(), first,
		func(string) (bool, error) { return false, nil },
		func(string) error { return context.Canceled })
	require.ErrorIs(t, err, context.Canceled)
	parent := testRepairParent(t, h, first)
	require.Equal(t, HfArtifactStatusUpdating, parent.Status)
	require.Empty(t, parent.LockID)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{
		first.ChildModelKey: ModelStatusFailed, second.ChildModelKey: ModelStatusFailed})
	require.NoError(t, newHfArtifactStartup(h).recover(context.Background()))
	result, err := h.handleDownloadOverride(context.Background(), second,
		func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{
		first.ChildModelKey: ModelStatusReady, second.ChildModelKey: ModelStatusReady})
}

func TestHfArtifactCancelledValidationPreservesCompletedParent(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	failAttach := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		child, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if failAttach && child.HfArtifactKey != "" {
			return true, nil, errors.New("attach unavailable")
		}
		return false, nil, nil
	})
	result, err := h.handleDownload(context.Background(), input, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	failAttach = false
	require.Empty(t, testRepairParent(t, h, input).Children)
	sentinel := filepath.Join(input.Parent.LocalPath, "keep-completed-files")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o644))
	_, err = h.handleDownloadOverride(context.Background(), input,
		func(string) (bool, error) { return false, context.Canceled }, nil)
	require.ErrorIs(t, err, context.Canceled)
	validated := false
	result, err = s.runHfArtifactDownload(context.Background(), task, input, true,
		func(string) (bool, error) { validated = true; return true, nil },
		func(string) error { t.Fatal("completed parent must be validated and reused"); return nil })
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.True(t, validated)
	require.FileExists(t, sentinel)
}

func TestHfArtifactCancelledValidationCannotPublishReady(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := h.handleDownloadOverride(ctx, first, func(string) (bool, error) {
		cancel()
		return true, nil
	}, nil)
	require.ErrorIs(t, err, context.Canceled)
	parent := testRepairParent(t, h, first)
	require.Equal(t, HfArtifactStatusUpdating, parent.Status)
	require.Empty(t, parent.LockID)
}

func TestHfArtifactCancellationSurvivesConfigMapLoss(t *testing.T) {
	s, _, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	interruptTestHfArtifactDownload(t, h, input)
	c := s.configMapReconciler
	require.NoError(t, c.kubeClient.CoreV1().ConfigMaps(c.namespace).Delete(context.Background(), c.nodeName, metav1.DeleteOptions{}))
	_, err := c.getConfigMapForHfArtifactCleanup(context.Background())
	require.NoError(t, err)
	parent := testRepairParent(t, h, input)
	require.Equal(t, HfArtifactStatusUpdating, parent.Status)
	require.Empty(t, parent.LockID)
	require.NotEmpty(t, parent.LastCompletedLockID)
	require.NoError(t, runTestHfArtifactDownload(h, input))
}

func TestHfArtifactCancellationRetriesUncertainLockRelease(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			s, _, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			client := s.configMapReconciler.kubeClient.(*fake.Clientset)
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				parent, exists := cm.Data[input.Parent.Key]
				if !exists {
					return false, nil, nil
				}
				entry, err := decodeHfArtifactEntry(input.Parent.Key, parent)
				require.NoError(t, err)
				if entry.Status != HfArtifactStatusUpdating || entry.LockID != "" {
					return false, nil, nil
				}
				if committed {
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				}
				return true, nil, errors.New("lock release response unavailable")
			})
			result, err := h.handleDownload(context.Background(), input, func(path string) error {
				require.NoError(t, writeTestHfArtifactFiles(path))
				return context.Canceled
			})
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			client.ReactionChain = client.ReactionChain[1:]
			restarted := newHfArtifactTaskHandler(newHfArtifactRepository(NewConfigMapReconciler(
				s.configMapReconciler.nodeName, s.configMapReconciler.namespace, client, s.logger)))
			require.NoError(t, newHfArtifactStartup(restarted).recover(context.Background()))
			parent := testRepairParent(t, restarted, input)
			require.Equal(t, HfArtifactStatusUpdating, parent.Status)
			require.Empty(t, parent.LockID)
			other := testHfArtifactTaskInput(t, input.ModelStoreRoot, "other")
			seedTestChildModelEntry(t, restarted.repository, other)
			require.NoError(t, runTestHfArtifactDownload(restarted, other))
			require.NoError(t, h.retryPendingLockRelease(context.Background(), input.Parent.Key))
			assertParentEntryReady(t, restarted.repository, other.Parent,
				map[string]string{other.ChildModelKey: other.ChildModelPath})
		})
	}
}

func TestHfArtifactInterruptedDownloadRejectsMissingOwnership(t *testing.T) {
	s, _, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	parent := input.Parent
	parent.Status = HfArtifactStatusUpdating
	require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
	require.NoError(t, h.repository.configMaps.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		return writeHfArtifactEntry(cm.Data, parent)
	}))
	// No lock owner and no completed attempt is corrupt ownership, rather than
	// an interruption. It must not authorize a destructive download reset.
	result, err := h.handleDownload(context.Background(), input, func(string) error {
		t.Fatal("invalid ownership must not authorize file writes")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	require.FileExists(t, filepath.Join(parent.LocalPath, "config.json"))
}

func TestHfArtifactCancelledChildDeleteWaitsForSiblingDownload(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	interruptTestHfArtifactDownload(t, h, input)
	other := testHfArtifactTaskInput(t, input.ModelStoreRoot, "other")
	seedTestChildModelEntry(t, h.repository, other)
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		result, err := h.handleDownload(context.Background(), other, func(path string) error {
			if err := writeTestHfArtifactFiles(path); err != nil {
				return err
			}
			close(started)
			<-release
			return nil
		})
		if err == nil && result.Outcome != hfArtifactTaskDone {
			err = errors.New("sibling did not finish")
		}
		done <- err
	}()
	<-started
	task.TaskType, task.SharedArtifact = Delete, true
	handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	assert.NoError(t, err)
	assert.True(t, handled)
	assert.True(t, waiting)
	assert.FileExists(t, filepath.Join(other.Parent.LocalPath, "config.json"))
	close(release)
	require.NoError(t, <-done)
	_, waiting, err = s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	assertParentEntryReady(t, h.repository, other.Parent,
		map[string]string{other.ChildModelKey: other.ChildModelPath})
	assertChildSymlinkTarget(t, other.ChildModelPath, other.Parent.LocalPath)
}

func TestHfArtifactStartupIsolatesInvalidDeletionReceipt(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			s, _, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			parent, acquired, err := h.repository.TryAcquireLock(ctx, input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
			client := s.configMapReconciler.kubeClient.(*fake.Clientset)
			cm, err := h.repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)
			if corrupt {
				cm.Data[input.ChildModelKey] = fmt.Sprintf(`{"hfArtifactPendingDeletion":{"parentLockId":%q`, parent.LockID)
			} else {
				child, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				child.HfArtifactPendingDeletion = &HfArtifactPendingDeletion{
					ParentLockID: parent.LockID, Identity: HfArtifactIdentity{ModelID: "wrong"}, ParentPath: parent.LocalPath}
				_, err = writeModelEntry(cm.Data, input.ChildModelKey, child)
				require.NoError(t, err)
			}
			_, err = client.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			startup := newHfArtifactStartup(h)
			require.NoError(t, startup.recover(ctx), "one invalid receipt must not close the global startup gate")
			require.True(t, startup.recovered)
			require.Error(t, startup.recoverParent(ctx, parent.Key))
			stored := testRepairParent(t, h, input)
			require.Equal(t, parent.LockID, stored.LockID)
		})
	}
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
			handled, waiting, err := s.reconcileHfArtifactCleanup(ctx, task, &retry, allowCleanup)
			require.NoError(t, err)
			require.False(t, handled, "a matching retry has no cleanup to perform")
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
			handled, waiting, err := s.reconcileHfArtifactCleanup(context.Background(), task, &next, true)
			require.NoError(t, err)
			require.True(t, handled, "changed targets must finish the previous cleanup")
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
		return errors.New("download failed")
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

func TestHfArtifactStaleDeletePreservesReplacementDownloadIntent(t *testing.T) {
	old, staleTask, input := newTestHfArtifactGopher(t)
	ctx := context.Background()
	client := old.configMapReconciler.kubeClient.(*fake.Clientset)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(staleTask.BaseModel))
	old.baseModelLister = modelslister.NewBaseModelLister(indexer)
	replacement := staleTask.BaseModel.DeepCopy()
	replacement.UID = "replacement-uid"
	next := input
	next.ChildModelUID = replacement.UID
	// A second agent has finished replacement bytes, but reference publication
	// failed. Its durable pending download still owns this complete copy.
	current := newHfArtifactTaskHandler(newHfArtifactRepository(NewConfigMapReconciler(
		old.configMapReconciler.nodeName, old.configMapReconciler.namespace, client, old.logger)))
	failAttach := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		child, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if failAttach && child.HfArtifactKey != "" {
			return true, nil, errors.New("attach response unavailable")
		}
		return false, nil, nil
	})
	result, err := current.handleDownload(ctx, next, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	failAttach = false
	expected, err := current.repository.pendingDownload(ctx, next.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, expected)
	require.Equal(t, replacement.UID, expected.ModelUID)
	// The stale Delete passes the lister check, then the replacement event
	// arrives before cleanup reads the new UID's pending download.
	injected := false
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if !injected {
			injected = true
			require.NoError(t, indexer.Update(replacement))
		}
		return false, nil, nil
	})
	staleTask.TaskType, staleTask.SharedArtifact, staleTask.NodeIneligible = Delete, true, true
	_, waiting, err := old.processSharedHfArtifactDelete(ctx, staleTask)
	require.NoError(t, err)
	require.True(t, injected)
	require.True(t, waiting)
	pending, err := current.repository.pendingDownload(ctx, next.ChildModelKey)
	require.NoError(t, err)
	assert.Equal(t, expected, pending, "a stale Delete must not convert the replacement UID's download intent")
	// The current model's normal retry must reuse its completed copy.
	result, err = current.handleDownload(ctx, next, func(string) error {
		t.Fatal("current download must reuse its complete parent")
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.FileExists(t, filepath.Join(next.Parent.LocalPath, "config.json"), "the current retry must not delete its complete copy")
}

func TestHfArtifactPendingDownloadHandoffRechecksUIDAfterConflict(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	ctx := context.Background()
	interruptTestHfArtifactDownload(t, h, input)
	expected, err := h.repository.pendingDownload(ctx, input.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, expected)
	replacement := task.BaseModel.DeepCopy()
	replacement.UID = "replacement-uid"
	task.BaseModel = replacement
	task.TaskType, task.SharedArtifact = Delete, true
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(replacement))
	s.baseModelLister = modelslister.NewBaseModelLister(indexer)
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	conflicted := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		child, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if !conflicted && child.HfArtifactPendingDeletion != nil {
			conflicted = true
			latest := replacement.DeepCopy()
			latest.UID = "latest-uid"
			require.NoError(t, indexer.Update(latest))
			return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("ownership changed"))
		}
		return false, nil, nil
	})
	_, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
	require.NoError(t, err)
	require.True(t, conflicted)
	require.True(t, waiting)
	pending, err := h.repository.pendingDownload(ctx, input.ChildModelKey)
	require.NoError(t, err)
	assert.Equal(t, expected, pending, "a handoff must recheck the current UID on every CAS attempt")
	assert.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
}

func TestHfArtifactPendingDownloadCleanupAdoptsCurrentUID(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	interruptTestHfArtifactDownload(t, h, input)
	// Another agent recorded the current UID's intent, while this process's
	// cache still belongs to the previous CR instance.
	c := s.configMapReconciler
	c.cacheMutex.Lock()
	c.modelCache[input.ChildModelKey].ModelUID = "previous-uid"
	c.cacheMutex.Unlock()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(task.BaseModel))
	s.baseModelLister = modelslister.NewBaseModelLister(indexer)
	task.TaskType, task.SharedArtifact = Delete, true
	handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.False(t, waiting, "the current UID must be able to finish cleanup despite an outdated local cache")
	assert.NoDirExists(t, input.Parent.LocalPath)
}
