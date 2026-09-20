package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestStartupCoveragePendingFailureFencesEveryOperation(t *testing.T) {
	for _, operation := range []string{"download", "repair", "delete", "preserve"} {
		for _, failedVerb := range []string{"get", "update"} {
			t.Run(operation+"/"+failedVerb, func(t *testing.T) {
				ctx := context.Background()
				s, _, input := newTestHfArtifactGopher(t)
				h := s.sharedHfArtifactHandler()
				require.NoError(t, runTestHfArtifactDownload(h, input))
				require.NoError(t, s.hfArtifactStartup.recover(ctx))
				parent, acquired, err := h.repository.TryAcquireLockForRepair(ctx, input.Parent)
				require.NoError(t, err)
				require.True(t, acquired)
				h.pendingFailures.Store(parent.Key, &parent)
				unavailable := true
				h.repository.configMaps.kubeClient.(*fake.Clientset).PrependReactor(failedVerb, "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					if unavailable {
						return true, nil, errors.New("pending failure API unavailable")
					}
					return false, nil, nil
				})
				var result hfArtifactTaskResult
				switch operation {
				case "download":
					result, err = h.handleDownload(ctx, input, func(string) error {
						t.Fatal("download must wait for failed-owner publication")
						return nil
					})
				case "repair":
					result, err = h.handleDownloadOverride(ctx, input, func(string) (bool, error) {
						t.Fatal("validation must wait for failed-owner publication")
						return true, nil
					}, nil)
				case "delete":
					result, err = h.handleDelete(ctx, input)
				case "preserve":
					result, err = s.releaseHfArtifactChild(ctx, input, true)
				}
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskRetry, result.Outcome)
				require.ErrorContains(t, result.RetryReason, "pending failure API unavailable")
				_, pending := h.pendingFailures.Load(parent.Key)
				require.True(t, pending)
				assertChildSymlinkTarget(t, input.ChildModelPath, parent.LocalPath)
				unavailable = false
				stored, found, err := h.repository.Get(ctx, parent.Identity)
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, parent.LockID, stored.LockID)
				require.NoError(t, h.retryPendingParentFailure(ctx, parent.Key))
				stored, _, err = h.repository.Get(ctx, parent.Identity)
				require.NoError(t, err)
				assert.Equal(t, HfArtifactStatusFailed, stored.Status)
				assert.Empty(t, stored.LockID)
				assert.Equal(t, input.ChildModelPath, stored.Children[input.ChildModelKey])
				_, pending = h.pendingFailures.Load(parent.Key)
				assert.False(t, pending)
			})
		}
	}
}

func TestStartupCoverageRecoveryRetriesStatusPublication(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed owner", true: "completed owner"}[completed], func(t *testing.T) {
			ctx := context.Background()
			h, first, _ := newTestHfArtifactRepair(t)
			parent, acquired, err := h.repository.TryAcquireLockForRepair(ctx, first.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			if completed {
				require.NoError(t, h.files.WriteParentReadyMarker(parent))
			}
			unavailable := true
			h.repository.configMaps.kubeClient.(*fake.Clientset).PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if unavailable {
					return true, nil, errors.New("recovery publication unavailable")
				}
				return false, nil, nil
			})
			startup := newHfArtifactStartup(h)
			require.ErrorContains(t, startup.recover(ctx), "recovery publication unavailable")
			assert.False(t, startup.recovered)
			stored, _, err := h.repository.Get(ctx, parent.Identity)
			require.NoError(t, err)
			assert.Equal(t, parent.LockID, stored.LockID)
			assert.Equal(t, HfArtifactStatusUpdating, stored.Status)
			unavailable = false
			require.NoError(t, startup.recover(ctx))
			stored, _, err = h.repository.Get(ctx, parent.Identity)
			require.NoError(t, err)
			want := HfArtifactStatusFailed
			if completed {
				want = HfArtifactStatusReady
			}
			assert.Equal(t, want, stored.Status)
			assert.Empty(t, stored.LockID)
			assert.Equal(t, completed, startup.needsValidation(parent.Key))
		})
	}
}

func TestStartupCoverageQuarantinesInvalidOwnership(t *testing.T) {
	for _, status := range []HfArtifactStatus{HfArtifactStatusUpdating, HfArtifactStatus("unknown")} {
		t.Run(string(status), func(t *testing.T) {
			h, first, _ := newTestHfArtifactRepair(t)
			ctx := context.Background()
			c := h.repository.configMaps
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			parent, err := decodeHfArtifactEntry(first.Parent.Key, cm.Data[first.Parent.Key])
			require.NoError(t, err)
			parent.Status, parent.LockID = status, ""
			raw, err := json.Marshal(parent)
			require.NoError(t, err)
			cm.Data[parent.Key] = string(raw)
			_, err = c.kubeClient.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			startup := newHfArtifactStartup(h)
			require.NoError(t, startup.recover(ctx))
			assert.True(t, startup.recovered)
			assert.True(t, startup.needsValidation(parent.Key))
			after, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.Equal(t, string(raw), after.Data[parent.Key], "invalid ownership must survive for reconstruction")
			assertChildSymlinkTarget(t, first.ChildModelPath, parent.LocalPath)
		})
	}
}

func TestStartupCoverageCompletedValidationReusesParent(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	startup := newHfArtifactStartup(h)
	ctx := context.Background()
	require.NoError(t, startup.recover(ctx))
	validations := 0
	validate := func(string) (bool, error) { validations++; return true, nil }
	for _, input := range []hfArtifactTaskInput{first, second} {
		result, err := startup.validateOnce(ctx, input, validate, func(string) error {
			t.Fatal("validated parent must be reused")
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, hfArtifactTaskDone, result.Outcome)
		assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	}
	assert.Equal(t, 1, validations)
}

func TestStartupCoveragePreserveWaitsForDurableCompletion(t *testing.T) {
	s, _, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	ctx := context.Background()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	require.NoError(t, s.hfArtifactStartup.recover(ctx))
	parent, acquired, err := h.repository.TryAcquireLockForRepair(ctx, input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	result, err := s.releaseHfArtifactChild(ctx, input, true)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	assertChildSymlinkTarget(t, input.ChildModelPath, parent.LocalPath)
	require.NoError(t, h.files.WriteParentReadyMarker(parent))
	unavailable := true
	h.repository.configMaps.kubeClient.(*fake.Clientset).PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if unavailable {
			return true, nil, errors.New("Ready publication unavailable")
		}
		return false, nil, nil
	})
	result, err = s.releaseHfArtifactChild(ctx, input, true)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	require.ErrorContains(t, result.RetryReason, "Ready publication unavailable")
	assertChildSymlinkTarget(t, input.ChildModelPath, parent.LocalPath)
	unavailable = false
	result, err = s.releaseHfArtifactChild(ctx, input, true)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	stored, _, err := h.repository.Get(ctx, parent.Identity)
	require.NoError(t, err)
	assert.Empty(t, stored.Children)
	assert.Empty(t, stored.LockID)
	assert.DirExists(t, parent.LocalPath)
}

func TestStartupCoverageDeleteLookupFailureRequeues(t *testing.T) {
	s, task, input := newTestHfArtifactGopher(t)
	h := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(h, input))
	h.repository.configMaps.kubeClient.(*fake.Clientset).PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("delete lookup unavailable")
	})
	s.samePathWaitDelay = time.Millisecond
	task.TaskType = Delete
	handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.True(t, waiting)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
	select {
	case retry := <-s.gopherChan:
		assert.Same(t, task, retry)
	case <-time.After(time.Second):
		t.Fatal("delete lookup failure lost its retry")
	}
}
