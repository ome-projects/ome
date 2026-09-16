package modelagent

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestHfArtifactPendingFailureRetriesWithoutRestart(t *testing.T) {
	repository, client := newTestHfArtifactRepository(t, map[string]string{})
	h := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	seedTestChildModelEntry(t, repository, input)
	unavailable := false
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if unavailable {
			return true, nil, errors.New("API unavailable")
		}
		return false, nil, nil
	})
	result, err := h.handleDownload(context.Background(), input, func(string) error {
		unavailable = true
		return errors.New("download failed")
	})
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
	parent, found, err := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusUpdating, parent.Status)
	unavailable = false
	require.NoError(t, runTestHfArtifactDownload(h, input))
	_, pending := h.pendingFailures.Load(parent.Key)
	assert.False(t, pending)
}

func TestHfArtifactPendingFailureCannotReleaseNewOwner(t *testing.T) {
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	h := newHfArtifactTaskHandler(repository)
	input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
	old, _, err := repository.TryAcquireLock(context.Background(), input.Parent)
	require.NoError(t, err)
	h.pendingFailures.Store(old.Key, &old)
	require.NoError(t, repository.MarkFailed(context.Background(), old))
	current, acquired, err := repository.TryAcquireLock(context.Background(), input.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, h.retryPendingParentFailure(context.Background(), old.Key))
	stored, found, err := repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, current.LockID, stored.LockID)
	assert.Equal(t, HfArtifactStatusUpdating, stored.Status)
}

func TestHfArtifactDeleteFinalizesCompletedRepair(t *testing.T) {
	h, first, second := newTestHfArtifactRepair(t)
	parent, acquired, err := h.repository.TryAcquireLockForRepair(context.Background(), first.Parent)
	require.NoError(t, err)
	require.True(t, acquired)
	parent, err = h.repository.MarkChildrenFailedForRepair(context.Background(), parent)
	require.NoError(t, err)
	require.NoError(t, h.files.WriteParentReadyMarker(parent))
	result, err := h.handleDelete(context.Background(), first)
	require.NoError(t, err)
	assert.Equal(t, hfArtifactTaskDone, result.Outcome)
	assertChildPathMissing(t, first.ChildModelPath)
	assertTestRepairChildStatuses(t, h, map[string]ModelStatus{second.ChildModelKey: ModelStatusReady})
	assert.True(t, h.files.IsChildLinkedToParent(second.ChildModelPath, parent.LocalPath))
}
