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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestGopherHfArtifactDeleteRetainsReceiptUntilModelRemoval(t *testing.T) {
	for _, preserve := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete files", true: "reserve artifact"}[preserve], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			require.NoError(t, runTestHfArtifactDownload(h, input))
			task.TaskType = Delete
			task.SharedArtifact = true
			if preserve {
				task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
			}
			// The CR now names a different destination; cleanup must keep using
			// the persisted path, including after the first attempt finishes.
			replacementPath := filepath.Join(input.ModelStoreRoot, "replacement")
			require.NoError(t, os.Mkdir(replacementPath, 0o755))
			task.BaseModel.Spec.Storage.Path = &replacementPath
			for attempt := 0; attempt < 2; attempt++ {
				handled, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
				require.NoError(t, err)
				require.True(t, handled, "completed shared deletion must never fall through to legacy cleanup")
				require.False(t, waiting)
				pending, err := s.sharedHfArtifactHandler().repository.pendingDeletion(ctx, input.ChildModelKey)
				require.NoError(t, err)
				require.NotNil(t, pending, "retain ownership until the model entry is removed")
				assert.Equal(t, input.ChildModelPath, pending.ChildPath)
				assert.DirExists(t, replacementPath)
				if preserve {
					assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
					assert.DirExists(t, input.Parent.LocalPath)
				} else {
					assertChildPathMissing(t, input.ChildModelPath)
					assert.NoDirExists(t, input.Parent.LocalPath)
				}
				// A new process must discover the receipt without the old cache.
				s = &Gopher{configMapReconciler: NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace,
					s.configMapReconciler.kubeClient, s.logger), modelRootDir: s.modelRootDir, logger: s.logger,
					baseModelLister: s.baseModelLister, clusterBaseModelLister: s.clusterBaseModelLister}
			}
			require.NoError(t, s.configMapReconciler.DeleteModelFromConfigMap(ctx, task.BaseModel, nil))
			pending, err := s.sharedHfArtifactHandler().repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			assert.Nil(t, pending)
		})
	}
}

func TestGopherHfArtifactDeleteRetriesFinalStatusRemoval(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "update failed", true: "response lost"}[committed], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			ctx := context.Background()
			require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), input))
			task.TaskType, task.SharedArtifact = Delete, true
			now := metav1.Now()
			task.BaseModel.DeletionTimestamp = &now
			replacementPath := filepath.Join(input.ModelStoreRoot, "replacement")
			require.NoError(t, os.Mkdir(replacementPath, 0o755))
			task.BaseModel.Spec.Storage.Path = &replacementPath
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			require.NoError(t, indexer.Add(task.BaseModel))
			s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			c := s.configMapReconciler
			client := c.kubeClient.(*fake.Clientset)
			_, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName}}, metav1.CreateOptions{})
			require.NoError(t, err)
			s.nodeLabelReconciler = NewNodeLabelReconciler(c.nodeName, client, 1, s.logger)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				if _, present := cm.Data[input.ChildModelKey]; present || injected {
					return false, nil, nil
				}
				injected = true
				if committed {
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				}
				return true, nil, errors.New("final child deletion response unavailable")
			})

			require.ErrorContains(t, s.processTask(task), "final child deletion response unavailable")
			require.True(t, injected)
			require.True(t, c.isModelMutationBlocked(input.ChildModelKey, input.ChildModelUID))
			assertChildPathMissing(t, input.ChildModelPath)
			require.NoDirExists(t, input.Parent.LocalPath)
			handled, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
			require.NoError(t, err)
			require.True(t, handled, "completed cleanup must remain handled even when the child entry is already absent")
			require.False(t, waiting)
			task.Sequence = 0 // A new reconciliation queues a fresh delete after the failed attempt.
			require.NoError(t, s.processTask(task))
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.NotContains(t, cm.Data, input.ChildModelKey, "retry must finish despite its own UID invalidation")
			assert.DirExists(t, replacementPath, "retry must not fall through to ordinary deletion of the new CR path")
		})
	}
}

func TestGopherHfArtifactDeleteRejectsSupersededInvalidatedUID(t *testing.T) {
	for _, proof := range []string{"cached owner", "current CR"} {
		t.Run(proof, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			require.NoError(t, runTestHfArtifactDownload(h, input))
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: task.BaseModel, ModelStatus: ModelStatusReady,
			}))
			task.TaskType, task.SharedArtifact = Delete, true
			replacement := task.BaseModel.DeepCopy()
			replacement.UID = "replacement-uid"
			unlock, acquired, err := h.tryArtifactOperation(input)
			require.NoError(t, err)
			require.True(t, acquired)
			_, err = h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, pending)
			require.NoError(t, h.repository.claimPendingDeletion(ctx, input.ChildModelKey, replacement.UID, pending, false, func() (bool, error) { return true, nil }))
			unlock()
			if proof == "current CR" {
				// The cache may be evicted before this delayed task resumes; the
				// current CR is still independent evidence of replacement.
				c := s.configMapReconciler
				c.configMapMutationMutex.Lock()
				c.cacheMutex.Lock()
				c.evictCachedModelLocked(input.ChildModelKey)
				c.cacheMutex.Unlock()
				c.configMapMutationMutex.Unlock()
				indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
				require.NoError(t, indexer.Add(replacement))
				s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			}
			s.configMapReconciler.cacheMutex.RLock()
			invalidated := s.configMapReconciler.isModelUIDInvalidatedLocked(input.ChildModelKey, input.ChildModelUID)
			s.configMapReconciler.cacheMutex.RUnlock()
			require.True(t, invalidated, "handoff also invalidates the old owner")
			before, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)

			// The old task already passed beginTask before the handoff.
			_, _, err = s.processSharedHfArtifactDelete(ctx, task)
			require.Error(t, err, "a superseded task must not proceed to final model removal")
			// Also cover handoff after the earlier Gopher guard has passed.
			cachedOwner := s.configMapReconciler.modelCache[input.ChildModelKey]
			now := metav1.Now()
			task.BaseModel.DeletionTimestamp = &now
			require.Error(t, s.configMapReconciler.DeleteModelFromConfigMap(ctx, task.BaseModel, nil))
			assert.Equal(t, cachedOwner, s.configMapReconciler.modelCache[input.ChildModelKey])
			after, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, before.Data, after.Data)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			assert.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestHfArtifactFinalDeletionRejectsSupersededUIDAfterOptOut(t *testing.T) {
	h, input, _ := newRecoveryCoveragePendingDeletion(t)
	ctx := context.Background()
	c := h.repository.configMaps
	oldModel := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{
		Name: "child", Namespace: "default", UID: input.ChildModelUID,
	}}
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel: oldModel, ModelStatus: ModelStatusUpdating,
	}))
	replacement := oldModel.DeepCopy()
	replacement.UID = "replacement-uid"
	input.ChildModelUID = replacement.UID
	h.isCurrentChildUID = func(hfArtifactTaskInput) (bool, error) { return true, nil }

	// The replacement finishes the old cleanup before switching to an ordinary
	// source. The delayed old delete has already passed its Gopher UID check.
	result, err := h.handleDelete(ctx, input)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel: replacement, ModelStatus: ModelStatusReady,
	}))
	before, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	child, err := existingModelEntry(before.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Empty(t, child.HfArtifactKey)
	require.Nil(t, child.HfArtifactPendingDeletion)
	require.True(t, c.isModelUIDInvalidatedLocked(input.ChildModelKey, oldModel.UID))
	cached := *c.modelCache[input.ChildModelKey]

	require.Error(t, c.DeleteModelFromConfigMap(ctx, oldModel, nil))
	after, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	assert.Equal(t, before.Data, after.Data)
	require.NotNil(t, c.modelCache[input.ChildModelKey])
	assert.Equal(t, cached, *c.modelCache[input.ChildModelKey])
}

func TestHfArtifactFinalDeletionRechecksSharedOwnerOnConflict(t *testing.T) {
	for _, state := range []string{"pending cleanup", "reattached child"} {
		t.Run(state, func(t *testing.T) {
			h, input, _ := newRecoveryCoveragePendingDeletion(t)
			ctx := context.Background()
			c := h.repository.configMaps
			replacement, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			child, err := existingModelEntry(replacement.Data, input.ChildModelKey)
			require.NoError(t, err)
			child.HfArtifactPendingDeletion.ModelUID = "replacement-uid"
			if state == "reattached child" {
				child.HfArtifactPendingDeletion = nil
				child.HfArtifactKey, child.Status = input.Parent.Key, ModelStatusReady
				parent := input.Parent
				parent.Status, parent.LockID = HfArtifactStatusReady, ""
				parent.Children = map[string]string{input.ChildModelKey: input.ChildModelPath}
				_, err = writeHfArtifactEntry(replacement.Data, parent)
				require.NoError(t, err)
			}
			_, err = writeModelEntry(replacement.Data, input.ChildModelKey, child)
			require.NoError(t, err)
			client := c.kubeClient.(*fake.Clientset)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if injected {
					return false, nil, nil
				}
				injected = true
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				require.NotContains(t, cm.Data, input.ChildModelKey)
				// A second process adopts or reattaches the child after the first
				// read. Its API write must survive the original delete's CAS retry.
				require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), replacement, replacement.Namespace))
				return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("shared owner changed"))
			})
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "default", UID: input.ChildModelUID}}
			require.Error(t, c.DeleteModelFromConfigMap(ctx, model, nil))
			require.True(t, injected)
			after, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, replacement.Data, after.Data)
		})
	}
}

func TestGopherHfArtifactDeleteAllowsCurrentUIDHandoff(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	ctx := context.Background()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
		BaseModel: task.BaseModel, ModelStatus: ModelStatusReady,
	}))
	_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	task.TaskType, task.SharedArtifact = Delete, true
	task.BaseModel.UID = "replacement-uid"
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(task.BaseModel))
	s.baseModelLister = modelslister.NewBaseModelLister(indexer)
	require.Equal(t, input.ChildModelUID, s.configMapReconciler.modelCache[input.ChildModelKey].ModelUID)

	handled, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
	require.NoError(t, err)
	require.True(t, handled)
	require.False(t, waiting)
	pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, task.BaseModel.UID, pending.ModelUID)
	assertChildPathMissing(t, input.ChildModelPath)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestHfArtifactDeleteSamePathDeletingChildren(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh delete", true: "pending receipt"}[interrupted], func(t *testing.T) {
			s, firstTask, first := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			second := first
			second.ChildModelKey = "clusterbasemodel.model-2"
			second.ChildModelUID = "second-uid"
			seedTestChildModelEntry(t, h.repository, second)
			for _, child := range []hfArtifactTaskInput{first, second} {
				require.NoError(t, runTestHfArtifactDownload(h, child))
			}
			deleting := metav1.Now()
			firstTask.TaskType = Delete
			firstTask.BaseModel.DeletionTimestamp = &deleting
			secondTask := &GopherTask{TaskType: Delete, ClusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model-2", UID: second.ChildModelUID, DeletionTimestamp: &deleting},
				Spec:       firstTask.BaseModel.Spec,
			}}
			require.Equal(t, second.ChildModelKey, getModelID(nil, secondTask.ClusterBaseModel))
			models := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			clusterModels := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			require.NoError(t, models.Add(firstTask.BaseModel))
			require.NoError(t, clusterModels.Add(secondTask.ClusterBaseModel))
			s.baseModelLister = modelslister.NewBaseModelLister(models)
			s.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(clusterModels)
			if interrupted {
				_, err := h.repository.removeModelReference(ctx, first.Parent, first.ChildModelKey, first.ChildModelUID, first.ChildModelPath, true)
				require.NoError(t, err)
				_, err = h.repository.removeModelReference(ctx, second.Parent, second.ChildModelKey, second.ChildModelUID, second.ChildModelPath, true)
				require.NoError(t, err)
			}
			// Keep both deleting CRs in the informer throughout both cleanups.
			for _, task := range []*GopherTask{secondTask, firstTask} {
				_, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
				require.NoError(t, err)
				require.False(t, waiting)
				require.NoError(t, s.configMapReconciler.DeleteModelFromConfigMap(ctx, task.BaseModel, task.ClusterBaseModel))
			}
			assertChildPathMissing(t, first.ChildModelPath)
			assert.NoDirExists(t, first.Parent.LocalPath)
			_, found, err := h.repository.Get(ctx, first.Parent.Identity)
			require.NoError(t, err)
			assert.False(t, found)
			for _, child := range []hfArtifactTaskInput{first, second} {
				pending, err := h.repository.pendingDeletion(ctx, child.ChildModelKey)
				require.NoError(t, err)
				assert.Nil(t, pending)
			}
		})
	}
}

func TestHfArtifactPendingDeletionOptOutRetriesLookupFailure(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	ctx := context.Background()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	s.captureStartupReadyModels(ctx)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	path := filepath.Join(input.ModelStoreRoot, "replacement")
	task.BaseModel.Spec.Storage.Path = &path
	_, finish, proceed, err := s.beginTask(task)
	require.NoError(t, err)
	require.True(t, proceed)
	defer finish(false)
	require.True(t, task.SharedArtifact)
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("cleanup lookup unavailable")
	})
	s.samePathWaitDelay = time.Millisecond
	_, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
	require.NoError(t, err)
	require.True(t, waiting, "changed CR fields must not bypass a known cleanup receipt")
	select {
	case <-s.gopherChan:
	case <-time.After(time.Second):
		t.Fatal("pending cleanup was not requeued")
	}
}

func TestHfArtifactPendingDeletionCancellationDoesNotRequeue(t *testing.T) {
	for _, tc := range []struct {
		name             string
		startupRecovered bool
		cancelAtRead     int
	}{
		{name: "receipt lookup", cancelAtRead: 1},
		{name: "startup snapshot", cancelAtRead: 2},
		{name: "parent recovery lookup", startupRecovered: true, cancelAtRead: 2},
		{name: "cleanup receipt lookup", startupRecovered: true, cancelAtRead: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(h, input))
			removal, err := h.repository.removeModelReference(context.Background(), input.Parent,
				input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			// Keep parent recovery read-only so each read identifies a distinct
			// stage: receipt lookup, startup/parent lookup, then cleanup lookup.
			require.NoError(t, h.repository.MarkFailed(context.Background(), removal.Artifact))
			s.hfArtifactStartup.recovered = tc.startupRecovered
			task.TaskType, task.SharedArtifact = Delete, true
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := s.configMapReconciler.kubeClient.(*fake.Clientset)
			reads := 0
			client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == tc.cancelAtRead {
					cancel()
					return true, nil, ctx.Err()
				}
				return false, nil, nil
			})

			handled, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			require.ErrorIs(t, ctx.Err(), context.Canceled, "the selected stage must be reached")
			require.ErrorIs(t, err, context.Canceled)
			assert.True(t, handled)
			assert.False(t, waiting)
			assert.True(t, task.SamePathWaitStartedAt.IsZero(), "cancellation must not schedule a retry")
			assert.Empty(t, s.gopherChan)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			assert.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestHfArtifactPendingDeletionGopherRecreatedUID(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(map[bool]string{false: "BaseModel", true: "ClusterBaseModel"}[cluster], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			if cluster {
				task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
				task.ClusterBaseModel.Namespace = ""
				task.BaseModel = nil
				input.ChildModelKey = getModelID(nil, task.ClusterBaseModel)
				seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, input)
			}
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			// Populate the UID through the production status/cache path.
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStatus: ModelStatusReady,
			}))
			require.NoError(t, runTestHfArtifactDownload(h, input))
			_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			oldTask := *task
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if cluster {
				task.ClusterBaseModel = task.ClusterBaseModel.DeepCopy()
				task.ClusterBaseModel.UID = "replacement-uid"
				require.NoError(t, indexer.Add(task.ClusterBaseModel))
				s.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(indexer)
			} else {
				task.BaseModel = task.BaseModel.DeepCopy()
				task.BaseModel.UID = "replacement-uid"
				require.NoError(t, indexer.Add(task.BaseModel))
				s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			}
			current := input
			current.ChildModelUID = "replacement-uid"
			// This ordinary update cannot register the new UID while the old
			// cache owner remains. The cleanup handoff must break that cycle.
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStatus: ModelStatusUpdating,
			}))
			require.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, current.ChildModelUID))
			_, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			require.NoError(t, err)
			require.False(t, waiting)
			pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, pending, "normal Gopher cleanup must finish the previous UID's receipt")
			require.False(t, h.repository.isChildMutationBlocked(input.ChildModelKey, current.ChildModelUID))
			require.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID))
			result, err := s.runHfArtifactDownload(ctx, task, current, true, nil, writeTestHfArtifactFiles)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome, "%v", result.RetryReason)
			assertChildSymlinkTarget(t, current.ChildModelPath, current.Parent.LocalPath)
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStatus: ModelStatusReady,
			}))
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: oldTask.BaseModel, ClusterBaseModel: oldTask.ClusterBaseModel, ModelStatus: ModelStatusFailed,
			}))
			cm, err := h.repository.configMaps.getConfigMap(ctx)
			require.NoError(t, err)
			child, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			assert.Equal(t, ModelStatusReady, child.Status, "old UID must remain fenced after handoff")
		})
	}
}

func TestHfArtifactPendingDeletionGopherRetriesUIDHandoffFailures(t *testing.T) {
	for _, lostResponse := range []bool{true, false} {
		t.Run(map[bool]string{true: "receipt CAS response lost", false: "post-CAS adoption GET fails"}[lostResponse], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: task.BaseModel, ModelStatus: ModelStatusReady,
			}))
			require.NoError(t, runTestHfArtifactDownload(h, input))
			_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			task.BaseModel = task.BaseModel.DeepCopy()
			task.BaseModel.UID = "replacement-uid"
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			require.NoError(t, indexer.Add(task.BaseModel))
			s.baseModelLister = modelslister.NewBaseModelLister(indexer)

			client := h.repository.configMaps.kubeClient.(*fake.Clientset)
			committed, injected, failNextGet := false, false, false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				child, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if committed || child.HfArtifactPendingDeletion == nil || child.HfArtifactPendingDeletion.ModelUID != task.BaseModel.UID {
					return false, nil, nil
				}
				committed = true
				if lostResponse {
					// Commit directly through the tracker: calling the fake client
					// from its reactor would reenter the fake's mutex.
					err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace)
					require.NoError(t, err)
					injected = true
					return true, nil, errors.New("receipt CAS response lost")
				}
				failNextGet = true
				return false, nil, nil
			})
			client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if failNextGet {
					failNextGet = false
					injected = true
					return true, nil, errors.New("post-CAS adoption GET unavailable")
				}
				return false, nil, nil
			})

			_, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			require.NoError(t, err)
			require.True(t, waiting)
			require.True(t, committed)
			require.True(t, injected)
			c := s.configMapReconciler
			c.cacheMutex.RLock()
			cachedUID := c.modelCache[input.ChildModelKey].ModelUID
			c.cacheMutex.RUnlock()
			assert.Equal(t, input.ChildModelUID, cachedUID, "failed handoff must retain the old cache owner")
			assert.False(t, h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID))
			assert.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, task.BaseModel.UID))
			pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, pending)
			assert.Equal(t, task.BaseModel.UID, pending.ModelUID, "receipt CAS must already be durable")
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)

			// Reenter Gopher so the retry reloads the committed receipt.
			retryTask := *task
			_, waiting, err = s.resumeHfArtifactChildDeletion(ctx, &retryTask, true)
			require.NoError(t, err)
			require.False(t, waiting)
			c.cacheMutex.RLock()
			cachedUID = c.modelCache[input.ChildModelKey].ModelUID
			c.cacheMutex.RUnlock()
			assert.Equal(t, task.BaseModel.UID, cachedUID)
			assert.False(t, h.repository.isChildMutationBlocked(input.ChildModelKey, task.BaseModel.UID))
			assert.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID))
			pending, err = h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			assert.Nil(t, pending)
			assertChildPathMissing(t, input.ChildModelPath)
			_, found, err := h.repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			assert.False(t, found)
		})
	}
}

func TestHfArtifactPendingDeletionPreservesDifferentParentReplacement(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	client := h.repository.configMaps.kubeClient.(*fake.Clientset)
	fail := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		child, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if _, parentPresent := cm.Data[input.Parent.Key]; fail && !parentPresent && child.HfArtifactPendingDeletion == nil {
			return true, nil, errors.New("receipt clearing unavailable")
		}
		return false, nil, nil
	})
	result, err := h.handleDelete(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildPathMissing(t, input.ChildModelPath)
	_, found, err := h.repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.False(t, found)
	fail = false
	other := input
	other.ChildModelKey = "default.basemodel.other-model"
	other.ChildModelUID = "other-uid"
	other.Parent.Identity.ModelID = "Qwen/Other"
	other.Parent.Key = hfArtifactConfigMapKey(other.Parent.Identity)
	other.Parent.LocalPath = canonicalHfArtifactPath(other.ChildModelPath, other.Parent.Identity)
	seedTestChildModelEntry(t, h.repository, other)
	otherModel := task.BaseModel.DeepCopy()
	otherModel.Name, otherModel.UID = "other-model", other.ChildModelUID
	otherModel.Annotations[hfModelIDAnnotationKey] = other.Parent.Identity.ModelID
	// The CR may retain a different spelling from the canonical persisted path.
	crPath := other.ChildModelPath + "/"
	otherModel.Spec.Storage.Path = &crPath
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, indexer.Add(otherModel))
	s.baseModelLister = modelslister.NewBaseModelLister(indexer)
	result, err = s.runHfArtifactDownload(context.Background(), &GopherTask{TaskType: Download, BaseModel: otherModel}, other, true, nil, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	// The lister-backed callback proves the replacement even though the old
	// parent's ConfigMap entry and child path were already deleted.
	_, waiting, err := s.resumeHfArtifactChildDeletion(context.Background(), task, true)
	require.NoError(t, err)
	require.False(t, waiting)
	pending, err := h.repository.pendingDeletion(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	assert.Nil(t, pending)
	assertChildSymlinkTarget(t, other.ChildModelPath, other.Parent.LocalPath)
	assertParentEntryReady(t, h.repository, other.Parent, map[string]string{other.ChildModelKey: other.ChildModelPath})
}

func TestHfArtifactPendingDeletionDoesNotRecoverReplacementParent(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	ctx := context.Background()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	removed, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	require.NoError(t, h.files.RemoveChildSymlink(input.ChildModelPath, input.Parent.LocalPath))
	require.NoError(t, h.files.RemoveParentDirectory(input.Parent.LocalPath))
	deleted, err := h.repository.DeleteIfUnreferenced(ctx, removed.Artifact)
	require.NoError(t, err)
	require.True(t, deleted)
	other := testHfArtifactTaskInput(t, t.TempDir(), "other-child")
	seedTestChildModelEntry(t, h.repository, other)
	_, acquired, err := h.repository.TryAcquireLock(ctx, other.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	before, err := h.repository.configMaps.getConfigMap(ctx)
	require.NoError(t, err)

	task.TaskType = Delete
	handled, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
	require.NoError(t, err)
	require.True(t, handled)
	require.False(t, waiting)
	after, err := h.repository.configMaps.getConfigMap(ctx)
	require.NoError(t, err)
	assert.Equal(t, before.Data[other.Parent.Key], after.Data[other.Parent.Key], "old receipt must not recover another path's owner")
	pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.NoError(t, s.configMapReconciler.DeleteModelFromConfigMap(ctx, task.BaseModel, nil))
	pending, err = h.repository.pendingDeletion(ctx, input.ChildModelKey)
	require.NoError(t, err)
	assert.Nil(t, pending)
}

func TestHfArtifactSourceTransitionPreservesSamePathConsumer(t *testing.T) {
	for _, source := range []string{"hf", "oci"} {
		for _, lateConsumer := range []bool{false, true} {
			t.Run(source+map[bool]string{false: "/existing consumer", true: "/consumer after preflight"}[lateConsumer], func(t *testing.T) {
				s, task, input := newTestHfArtifactGopher(t)
				h := s.sharedHfArtifactHandler()
				require.NoError(t, runTestHfArtifactDownload(h, input))
				path := input.ChildModelPath + "/"
				localURI := "local://" + path
				consumer := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "default"}, Spec: v1beta1.BaseModelSpec{
					Storage: &v1beta1.StorageSpec{StorageUri: &localURI, Path: &path},
				}}
				indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
				s.baseModelLister = modelslister.NewBaseModelLister(indexer)
				if lateConsumer {
					client := h.repository.configMaps.kubeClient.(*fake.Clientset)
					client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
						cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
						child, err := existingModelEntry(cm.Data, input.ChildModelKey)
						require.NoError(t, err)
						if child.HfArtifactPendingDeletion != nil {
							require.NoError(t, indexer.Add(consumer))
						}
						return false, nil, nil
					})
				} else {
					require.NoError(t, indexer.Add(consumer))
				}
				policy := v1beta1.AlwaysDownload
				task.BaseModel.Spec.Storage.DownloadPolicy = &policy
				if source == "hf" {
					uri := "hf://org/model"
					task.BaseModel.Spec.Storage.StorageUri = &uri
					waiting, err := s.detachHfArtifactForDefaultDownload(context.Background(), task, task.BaseModel.Spec, true)
					require.Error(t, err, "a preserved shared symlink must never reach ordinary HF download")
					require.False(t, waiting)
				} else {
					handled, waiting, err := s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, true)
					require.Error(t, err, "a preserved shared symlink must never reach ordinary OCI download")
					require.True(t, handled)
					require.False(t, waiting)
				}
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
				assert.DirExists(t, input.Parent.LocalPath)
			})
		}
	}
}

func TestHfArtifactPendingDeletionHandoffRequiresReceiptAndListerUID(t *testing.T) {
	for _, receipt := range []bool{false, true} {
		t.Run(map[bool]string{false: "no receipt", true: "noncurrent lister UID"}[receipt], func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			ctx := context.Background()
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: task.BaseModel, ModelStatus: ModelStatusReady,
			}))
			require.NoError(t, runTestHfArtifactDownload(h, input))
			if receipt {
				_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
				require.NoError(t, err)
			}
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			latest := task.BaseModel.DeepCopy()
			latest.UID = "latest-uid"
			require.NoError(t, indexer.Add(latest))
			s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			task.BaseModel = latest.DeepCopy()
			if receipt {
				task.BaseModel.UID = "superseded-uid"
			}
			_, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			require.NoError(t, err)
			assert.Equal(t, receipt, waiting)
			assert.True(t, h.repository.isChildMutationBlocked(input.ChildModelKey, task.BaseModel.UID))
			assert.False(t, h.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID))
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			if receipt {
				require.NotNil(t, pending)
				assert.Equal(t, input.ChildModelUID, pending.ModelUID)
			} else {
				assert.Nil(t, pending)
			}
		})
	}
}
