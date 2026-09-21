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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestHfArtifactStartupCorruptChildDoesNotBlockOtherParents(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	parent, acquired, err := h.repository.TryAcquireLockForRepair(context.Background(), first.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	parent, err = h.repository.MarkChildrenFailedForRepair(context.Background(), parent)
	require.NoError(t, err)
	require.NoError(t, h.files.WriteParentReadyMarker(parent))
	c := h.repository.configMaps
	cm, err := c.getConfigMap(context.Background())
	require.NoError(t, err)
	cm.Data[first.ChildModelKey] = "bad JSON"
	_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	startup := newHfArtifactStartup(h)
	require.NoError(t, startup.recover(context.Background()))
	assert.True(t, startup.recovered, "unrelated identities must not wait behind corrupt child state")
	stored, _, err := h.repository.Get(context.Background(), parent.Identity)
	require.NoError(t, err)
	assert.Equal(t, HfArtifactStatusUpdating, stored.Status, "preserve corrupt relationship for reconstruction")
}

func TestHfArtifactStartupValidatesAfterMarkerOnlyCompletion(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	ctx := context.Background()
	parent, acquired, err := h.repository.TryAcquireLockForRepair(ctx, first.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	parent, err = h.repository.MarkChildrenFailedForRepair(ctx, parent)
	require.NoError(t, err)
	require.NoError(t, h.files.WriteParentReadyMarker(parent))
	c := h.repository.configMaps
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	savedChild := cm.Data[second.ChildModelKey]
	cm.Data[second.ChildModelKey] = "bad JSON"
	_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	startup := newHfArtifactStartup(h)
	require.NoError(t, startup.recover(ctx))
	require.True(t, startup.needsValidation(parent.Key))
	cm, err = c.getConfigMap(ctx)
	require.NoError(t, err)
	cm.Data[second.ChildModelKey] = savedChild
	_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	validations := 0
	validate := func(string) (bool, error) { validations++; return true, nil }
	result, err := startup.validateOnce(ctx, first, validate, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome, "finishing a prior owner is not a startup integrity check")
	require.True(t, startup.needsValidation(parent.Key))
	require.Zero(t, validations)
	result, err = startup.validateOnce(ctx, first, validate, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.Equal(t, 1, validations)
	require.False(t, startup.needsValidation(parent.Key))
}

func TestGopherHfArtifactReservedDeleteFinishesPendingParent(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprint(completed), func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			h := s.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(h, input))
			parent, acquired, err := h.repository.TryAcquireLockForRepair(context.Background(), input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			if completed {
				require.NoError(t, h.files.WriteParentReadyMarker(parent))
			} else {
				h.pendingFailures.Store(parent.Key, &parent)
			}
			task.TaskType = Delete
			task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
			handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
			require.NoError(t, err)
			assert.True(t, handled)
			assert.False(t, waiting)
			stored, _, err := h.repository.Get(context.Background(), parent.Identity)
			require.NoError(t, err)
			assert.Empty(t, stored.LockID)
			assert.Empty(t, stored.Children)
			assert.DirExists(t, parent.LocalPath)
		})
	}
}

func TestGopherHfArtifactLookupFailureKeepsScheduledRetry(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	parent, acquired, err := h.repository.TryAcquireLockForRepair(context.Background(), input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	h.pendingFailures.Store(parent.Key, &parent)
	client := h.repository.configMaps.kubeClient.(*fake.Clientset)
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("temporary API outage")
	})
	s.samePathWaitDelay = time.Millisecond
	handled, waiting, err := s.processHfOCIArtifact(context.Background(), task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.True(t, waiting)
	select {
	case retry := <-s.gopherChan:
		assert.Same(t, task, retry)
	case <-time.After(time.Second):
		t.Fatal("pending cleanup retry was dropped after GET failure")
	}
}

func TestHfArtifactStartupMissingConfigMapIsColdStart(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	c := repository.configMaps
	require.NoError(t, client.CoreV1().ConfigMaps(c.namespace).Delete(context.Background(), c.nodeName, metav1.DeleteOptions{}))
	startup := newHfArtifactStartup(newHfArtifactTaskHandler(repository))
	require.NoError(t, startup.recover(context.Background()))
	assert.True(t, startup.recovered)
	assert.Empty(t, startup.pending)
}

func TestHfArtifactStartupWaitsForOldProcessBeforeRecovery(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	h := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	parent, acquired, err := repository.TryAcquireLock(context.Background(), input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	lock, acquired, err := tryHfArtifactParentFileLock(parent, input.ModelStoreRoot)
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { _ = lock.Close() })
	startup := newHfArtifactStartup(h)
	require.NoError(t, startup.recover(context.Background()))
	assert.True(t, startup.recovered, "one active owner must not block unrelated parents")
	require.ErrorContains(t, startup.recoverParent(context.Background(), parent.Key), "active owner")
	stored, _, err := repository.Get(context.Background(), parent.Identity)
	require.NoError(t, err)
	assert.Equal(t, parent.LockID, stored.LockID)
	// Simulate the older process exiting without finishing its ConfigMap state.
	require.NoError(t, lock.Close())
	require.NoError(t, startup.recoverParent(context.Background(), parent.Key))
	stored, _, err = repository.Get(context.Background(), parent.Identity)
	require.NoError(t, err)
	assert.Equal(t, HfArtifactStatusFailed, stored.Status)
	assert.Empty(t, stored.LockID)
}

func TestHfArtifactStartupRecoversOwnerStartedAfterSnapshot(t *testing.T) {
	for _, marker := range []string{"old", "matching", "missing"} {
		t.Run(marker, func(t *testing.T) {
			ctx := context.Background()
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			startup := newHfArtifactStartup(handler)
			require.NoError(t, startup.recover(ctx))

			// Another process begins repair after this process saw Ready.
			lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
			require.NoError(t, err)
			require.True(t, acquired)
			t.Cleanup(func() { _ = lock.Close() })
			parent, acquired, err := repository.TryAcquireLockForRepair(ctx, input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			switch marker {
			case "matching":
				require.NoError(t, handler.files.WriteParentReadyMarker(parent))
			case "missing":
				require.NoError(t, os.Remove(filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName)))
			}
			require.ErrorContains(t, startup.recoverParent(ctx, parent.Key), "active owner")
			stored, _, err := repository.Get(ctx, parent.Identity)
			require.NoError(t, err)
			require.Equal(t, parent.LockID, stored.LockID)

			// A crash releases the OS lock but leaves Updating in the ConfigMap.
			require.NoError(t, lock.Close())
			require.NoError(t, startup.recoverParent(ctx, parent.Key))
			stored, _, err = repository.Get(ctx, parent.Identity)
			require.NoError(t, err)
			expectedStatus := HfArtifactStatusFailed
			if marker == "matching" {
				expectedStatus = HfArtifactStatusReady
			}
			require.Equal(t, expectedStatus, stored.Status)
			require.Empty(t, stored.LockID)
			validations := 0
			result, err := startup.validateOnce(ctx, input, func(string) (bool, error) {
				validations++
				return true, nil
			}, nil)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			require.Equal(t, 1, validations)
			assert.False(t, startup.needsValidation(parent.Key))
		})
	}
}

func TestHfArtifactStartupChecksNewParentAfterSnapshot(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "abandoned", true: "completed"}[completed], func(t *testing.T) {
			ctx := context.Background()
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			startup := newHfArtifactStartup(handler)
			require.NoError(t, startup.recover(ctx))
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
			require.NoError(t, err)
			require.True(t, acquired)
			t.Cleanup(func() { _ = lock.Close() })
			parent, acquired, err := repository.TryAcquireLock(ctx, input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
			if completed {
				// A waiting task must not turn a new download into startup work.
				require.ErrorContains(t, startup.recoverParent(ctx, parent.Key), "active owner")
				require.NoError(t, handler.files.WriteParentReadyMarker(parent))
				require.NoError(t, repository.MarkReady(ctx, parent))
			}
			require.NoError(t, lock.Close())
			// In the abandoned case, recovery has never observed the live owner.
			require.NoError(t, startup.recoverParent(ctx, parent.Key))
			stored, found, err := repository.Get(ctx, parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			want := HfArtifactStatusFailed
			if completed {
				want = HfArtifactStatusReady
			}
			assert.Equal(t, want, stored.Status)
			assert.Empty(t, stored.LockID)
			assert.False(t, startup.needsValidation(parent.Key))
			assert.NotContains(t, startup.deferred, parent.Key)
			if completed {
				result, err := startup.validateOnce(ctx, input, func(string) (bool, error) {
					t.Fatal("a completed new download must not be revalidated as startup work")
					return false, nil
				}, nil)
				require.NoError(t, err)
				assert.Equal(t, hfArtifactTaskDone, result.Outcome)
				assertChildSymlinkTarget(t, input.ChildModelPath, parent.LocalPath)
			}
		})
	}
}

func TestHfArtifactStartupLockErrorRetainsValidation(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	h := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	parent, acquired, err := repository.TryAcquireLock(context.Background(), input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
	require.NoError(t, h.files.WriteParentReadyMarker(parent))
	lockDirectory := filepath.Join(input.ModelStoreRoot, hfArtifactLockDirectory)
	require.NoError(t, os.WriteFile(lockDirectory, []byte("unavailable lock directory"), 0o644))
	startup := newHfArtifactStartup(h)
	require.NoError(t, startup.recover(context.Background()))
	require.Contains(t, startup.deferred, parent.Key)
	require.NoError(t, os.Remove(lockDirectory))
	require.NoError(t, startup.recoverParent(context.Background(), parent.Key))
	stored, _, err := repository.Get(context.Background(), parent.Identity)
	require.NoError(t, err)
	assert.Equal(t, HfArtifactStatusReady, stored.Status)
	assert.True(t, startup.needsValidation(parent.Key), "successful recovery must still validate bytes after startup")
}

func TestHfArtifactStartupCorruptForeignRecordDoesNotBlockRecovery(t *testing.T) {
	h, first, _ := newTestHfArtifactRepair(t)
	c := h.repository.configMaps
	cm, err := c.getConfigMap(context.Background())
	require.NoError(t, err)
	cm.Data[constants.HfArtifactConfigMapKeyPrefix+"foreign"] = "corrupt"
	_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	startup := newHfArtifactStartup(h)
	require.NoError(t, startup.recover(context.Background()))
	assert.True(t, startup.needsValidation(first.Parent.Key))
}

func TestGopherHfArtifactStartupValidationRunsOnNormalWorkerOnce(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	s.hfArtifactStartup = newHfArtifactStartup(h)
	validations := 0
	validate := func(string) (bool, error) { validations++; return true, nil }
	result, err := s.runHfArtifactDownload(context.Background(), task, input, false, validate, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	assert.True(t, task.NormalPriorityOnly)
	assert.Zero(t, validations)
	for i := 0; i < 2; i++ {
		result, err = s.runHfArtifactDownload(context.Background(), task, input, true, validate, nil)
		require.NoError(t, err)
		require.Equal(t, hfArtifactTaskDone, result.Outcome)
	}
	assert.Equal(t, 1, validations)
}

func TestGopherHfArtifactStartupLookupFailureKeepsRecoveryGateClosed(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	s.hfArtifactStartup = newHfArtifactStartup(h)
	client := h.repository.configMaps.kubeClient.(*fake.Clientset)
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("unavailable")
	})
	result, err := s.runHfArtifactDownload(context.Background(), task, input, true, nil, func(string) error {
		t.Fatal("must not start a writer before startup ownership recovery")
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assert.False(t, s.hfArtifactStartup.recovered)
}

func TestHfArtifactStartupRecoversUpdatingParent(t *testing.T) {
	for _, marker := range []string{"matching", "old", "missing"} {
		t.Run(marker, func(t *testing.T) {
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
			parent, acquired, err := repository.TryAcquireLock(context.Background(), input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
			if marker == "matching" {
				require.NoError(t, handler.files.WriteParentReadyMarker(parent))
			} else if marker == "old" {
				require.NoError(t, os.WriteFile(filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName), []byte("previous-owner"), 0o644))
			}
			startup := newHfArtifactStartup(handler)
			require.NoError(t, startup.recover(context.Background()))
			stored, found, err := repository.Get(context.Background(), parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			want := HfArtifactStatusFailed
			if marker == "matching" {
				want = HfArtifactStatusReady
			}
			assert.Equal(t, want, stored.Status)
			assert.Empty(t, stored.LockID)
			pending := startup.needsValidation(parent.Key)
			assert.Equal(t, marker == "matching", pending)
		})
	}
}

func TestHfArtifactStartupRetriesSnapshotFailure(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	handler := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	unavailable := true
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if unavailable {
			return true, nil, errors.New("API unavailable")
		}
		return false, nil, nil
	})
	startup := newHfArtifactStartup(handler)
	require.ErrorContains(t, startup.recover(context.Background()), "API unavailable")
	assert.False(t, startup.recovered)
	unavailable = false
	require.NoError(t, startup.recover(context.Background()))
	pending := startup.needsValidation(input.Parent.Key)
	assert.True(t, pending)
}

func TestHfArtifactStartupSerializesSiblingValidation(t *testing.T) {
	handler, first, second := newTestHfArtifactRepair(t)
	startup := newHfArtifactStartup(handler)
	require.NoError(t, startup.recover(context.Background()))
	started, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var calls atomic.Int32
	validate := func(string) (bool, error) {
		calls.Add(1)
		close(started)
		<-finish
		return true, nil
	}
	go func() {
		result, err := startup.validateOnce(context.Background(), first, validate, nil)
		if err == nil && result.Outcome != hfArtifactTaskDone {
			err = errors.New("first validation did not complete")
		}
		done <- err
	}()
	<-started
	result, err := startup.validateOnce(context.Background(), second, validate, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	close(finish)
	require.NoError(t, <-done)
	assert.EqualValues(t, 1, calls.Load())
	pending := startup.needsValidation(first.Parent.Key)
	assert.False(t, pending)
}

func TestHfArtifactStartupRetainsValidationAfterFailure(t *testing.T) {
	handler, first, _ := newTestHfArtifactRepair(t)
	startup := newHfArtifactStartup(handler)
	require.NoError(t, startup.recover(context.Background()))
	_, err := startup.validateOnce(context.Background(), first, func(string) (bool, error) {
		return false, errors.New("object metadata unavailable")
	}, nil)
	require.ErrorContains(t, err, "object metadata unavailable")
	pending := startup.needsValidation(first.Parent.Key)
	assert.True(t, pending)
	result, err := startup.validateOnce(context.Background(), first, func(string) (bool, error) { return true, nil }, nil)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	pending = startup.needsValidation(first.Parent.Key)
	assert.False(t, pending)
}
