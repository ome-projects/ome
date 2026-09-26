package modelagent

import (
	"context"
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
	k8stesting "k8s.io/client-go/testing"
)

func TestSharedEvictionRetainsModelAndAcknowledgesCleanup(t *testing.T) {
	for _, sibling := range []bool{false, true} {
		t.Run(map[bool]string{false: "last child", true: "sibling"}[sibling], func(t *testing.T) {
			g, task, input := newSharedEvictionTestModel(t)
			if sibling {
				other := testHfArtifactTaskInput(t, input.ModelStoreRoot, "sibling")
				seedTestChildModelEntry(t, g.sharedHfArtifactHandler().repository, other)
				require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), other))
			}
			require.NoError(t, g.processTask(task))
			entry := directArtifactEntry(t, g, task)
			require.Equal(t, ModelStatusEvicted, entry.Status)
			require.Nil(t, entry.HfArtifactPendingDeletion)
			require.Empty(t, entry.HfArtifactKey)
			_, err := os.Lstat(input.ChildModelPath)
			require.True(t, os.IsNotExist(err))
			_, err = os.Stat(input.Parent.LocalPath)
			if sibling {
				require.NoError(t, err)
			} else {
				require.True(t, os.IsNotExist(err))
			}
			_, err = g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(context.Background(), task.BaseModel.Name, metav1.GetOptions{})
			require.NoError(t, err)
			// Completed acknowledgement must not retain ownership of a reused path.
			require.NoError(t, os.MkdirAll(input.ChildModelPath, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "new"), []byte("new owner"), 0600))
			require.NoError(t, g.processTask(task))
			require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Delete(context.Background(), task.BaseModel.Name, metav1.DeleteOptions{}))
			task.TaskType = Delete
			require.NoError(t, g.processTask(task))
			require.FileExists(t, filepath.Join(input.ChildModelPath, "new"))
		})
	}
}

func TestSharedEvictionCompletesInCleanupPhase(t *testing.T) {
	g, task, input := newSharedEvictionTestModel(t)
	client := g.kubeClient.(*k8sfake.Clientset)
	// This fixture has no bound node UID: all node reads here are readiness
	// preparation, not the independent current-node identity check.
	require.Empty(t, g.nodeUID)
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if _, err := os.Lstat(input.ChildModelPath); os.IsNotExist(err) {
			return true, nil, errors.New("readiness was already withdrawn before cleanup")
		}
		return false, nil, nil
	})
	acknowledged := false
	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if entry.Status == ModelStatusEvicted {
			acknowledged = true
			require.Nil(t, entry.HfArtifactPendingDeletion)
			unlock, acquired, err := g.sharedHfArtifactHandler().tryArtifactOperation(input)
			if acquired {
				unlock()
			}
			require.NoError(t, err)
			require.False(t, acquired, "completion retains both cleanup locks")
		}
		return false, nil, nil
	})
	require.NoError(t, g.processTask(task))
	require.True(t, acknowledged)
	require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
}

func TestSharedEvictionRetryStopsWhenIntentWithdrawn(t *testing.T) {
	ctx := context.Background()
	g, task, _ := newSharedEvictionTestModel(t)
	g.nodeUID = "old-node"
	live := task.BaseModel.DeepCopy()
	delete(live.Annotations, ArtifactResidencyAnnotation)
	live.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
	_, err := g.modelClient.OmeV1beta1().BaseModels(live.Namespace).Update(ctx, live, metav1.UpdateOptions{})
	require.NoError(t, err)
	client := g.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("cleanup lookup unavailable")
	})
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		t.Fatal("withdrawn eviction must release retry without reading Node")
		return false, nil, nil
	})
	handled, waiting, err := g.processSharedArtifactEviction(ctx, task)
	require.NoError(t, err)
	require.True(t, handled)
	require.False(t, waiting, "withdrawn eviction must release its delete barrier")
	require.Empty(t, g.gopherChan)
}

func TestSharedEvictionCompletionRequiresExactReceipt(t *testing.T) {
	ctx := context.Background()
	g, task, input := newSharedEvictionTestModel(t)
	g.guardSharedEvictionCleanup(task, &input)
	finish := input.finishDeletion
	input.finishDeletion = func(ctx context.Context, pending HfArtifactPendingDeletion) error {
		require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, entry.HfArtifactPendingDeletion)
			entry.HfArtifactPendingDeletion.ParentLockID = "new-cleanup-owner"
			return writeModelEntry(cm.Data, input.ChildModelKey, entry)
		}))
		return finish(ctx, pending)
	}
	result, err := g.sharedHfArtifactHandler().handleDelete(ctx, input)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	require.ErrorContains(t, result.RetryReason, "receipt changed before acknowledgement")
	entry := directArtifactEntry(t, g, task)
	require.NotEqual(t, ModelStatusEvicted, entry.Status)
	require.NotNil(t, entry.HfArtifactPendingDeletion)
	require.Equal(t, "new-cleanup-owner", entry.HfArtifactPendingDeletion.ParentLockID)
}

func TestSharedEvictionCancelsAndWaitsForFinalizer(t *testing.T) {
	g, task, _ := newSharedEvictionTestModel(t)
	download := *task
	download.TaskType, download.Sequence = Download, 0
	ctx, finish, proceed, err := g.beginTask(&download)
	require.NoError(t, err)
	require.True(t, proceed)
	g.enqueueTask(task)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	_, _, proceed, err = g.beginTask(task)
	require.NoError(t, err)
	require.False(t, proceed, "cancellation is not finalization")
	finish(false)
	require.NoError(t, g.processTask(task))
	require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
}

func TestSharedEvictionRefreshesPersistedRoutingAfterOptOut(t *testing.T) {
	g, task, _ := newSharedEvictionTestModel(t)
	// Another process persisted ownership after this process loaded its index.
	g.artifactRouting.known = true
	g.artifactRouting.children = nil
	task.SharedArtifact = false
	task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "changed"))
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	download := *task
	download.TaskType = Download
	ctx, finish, proceed, err := g.beginTask(&download)
	require.NoError(t, err)
	require.True(t, proceed)
	require.False(t, download.SharedArtifact)
	g.enqueueTask(task)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	_, _, proceed, err = g.beginTask(task)
	require.NoError(t, err)
	require.False(t, proceed, "persisted ownership must install the full finalization barrier")
	finish(false)
}

func TestSharedEvictionRoutingFailureFinalizesOnlyUnqueuedAttempt(t *testing.T) {
	for _, state := range []string{"retry queued", "retry exhausted", "newer barrier"} {
		t.Run(state, func(t *testing.T) {
			g, task, _ := newSharedEvictionTestModel(t)
			g.enqueueTask(task)
			queued, ok := g.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, task, queued)
			if state != "retry queued" {
				g.samePathWaitTimeout = time.Millisecond
				task.SamePathWaitStartedAt = time.Now().Add(-time.Second)
			}
			if state == "newer barrier" {
				attempt, _ := g.taskTracker.beginDelete(gopherTaskModelKey(task), task.Sequence)
				g.taskTracker.finishDelete(attempt, false)
				newer := g.taskTracker.ensureSequence(0)
				attempt, outcome := g.taskTracker.beginDelete(gopherTaskModelKey(task), newer)
				require.Equal(t, gopherTaskProceed, outcome)
				g.taskTracker.finishDelete(attempt, true)
			}
			unavailable := true
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
				if unavailable {
					return true, nil, errors.New("routing unavailable")
				}
				return false, nil, nil
			})
			err := g.processTask(task)
			if state == "retry queued" {
				require.NoError(t, err)
				select {
				case retry := <-g.gopherChan:
					require.Same(t, task, retry)
				case <-time.After(time.Second):
					t.Fatal("retry was not queued")
				}
			} else {
				require.ErrorContains(t, err, "retry budget exhausted")
				require.Empty(t, g.gopherChan)
			}
			unavailable = false
			latest := task.BaseModel.DeepCopy()
			delete(latest.Annotations, ArtifactResidencyAnnotation)
			_, err = g.modelClient.OmeV1beta1().BaseModels(latest.Namespace).Update(context.Background(), latest, metav1.UpdateOptions{})
			require.NoError(t, err)
			_, finish, proceed, err := g.beginTask(&GopherTask{TaskType: Download, BaseModel: latest})
			require.NoError(t, err)
			require.Equal(t, state == "retry exhausted", proceed, "only the exhausted eviction's own barrier may be released")
			finish(false)
		})
	}
}
