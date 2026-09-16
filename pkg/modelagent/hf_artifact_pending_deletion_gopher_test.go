package modelagent

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

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
	waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
	require.NoError(t, err)
	require.True(t, waiting, "changed CR fields must not bypass a known cleanup receipt")
	select {
	case <-s.gopherChan:
	case <-time.After(time.Second):
		t.Fatal("pending cleanup was not requeued")
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
			waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
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

			waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
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
			waiting, err = s.resumeHfArtifactChildDeletion(ctx, &retryTask, true)
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
	waiting, err := s.resumeHfArtifactChildDeletion(context.Background(), task, true)
	require.NoError(t, err)
	require.False(t, waiting)
	pending, err := h.repository.pendingDeletion(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	assert.Nil(t, pending)
	assertChildSymlinkTarget(t, other.ChildModelPath, other.Parent.LocalPath)
	assertParentEntryReady(t, h.repository, other.Parent, map[string]string{other.ChildModelKey: other.ChildModelPath})
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
			waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
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
