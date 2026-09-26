package modelagent

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestSharedEvictionPhaseValidatesInitialReferencesOnce(t *testing.T) {
	g, task, input := newSharedEvictionTestModel(t)
	g.guardSharedEvictionCleanup(task, &input)
	unlock, acquired, err := g.sharedHfArtifactHandler().tryArtifactOperation(input)
	require.NoError(t, err)
	require.True(t, acquired)
	defer unlock()
	client := g.modelClient.(*omefake.Clientset)
	client.ClearActions()

	require.NoError(t, input.prepareDeletionPhase(context.Background()))

	lists := map[string]int{}
	for _, action := range client.Actions() {
		if action.GetVerb() == "list" {
			lists[action.GetResource().Resource]++
		}
	}
	require.Equal(t, map[string]int{"basemodels": 1, "clusterbasemodels": 1}, lists,
		"phase entry validates references before readiness preparation")
	require.Equal(t, ModelStatusEvicting, directArtifactEntry(t, g, task).Status)
}

func TestSharedCleanupPreparesOncePerLockedPhase(t *testing.T) {
	for _, operation := range []string{"delete", "download", "repair"} {
		for _, revoke := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/current", true: "/revoked"}[revoke], func(t *testing.T) {
				h, input, _ := newRecoveryCoveragePendingDeletion(t)
				checks, preparations := 0, 0
				revoked := false
				input.validateDeletion = func(context.Context) error {
					checks++
					if revoked {
						return errors.New("request revoked after preparation")
					}
					return nil
				}
				input.prepareDeletion = func(context.Context) error {
					preparations++
					unlock, acquired, err := h.tryArtifactOperation(input)
					if acquired {
						unlock()
					}
					require.NoError(t, err)
					require.False(t, acquired, "preparation must own the operation locks")
					return nil
				}
				input.pathReferenced = func(context.Context, string) (bool, error) {
					revoked = revoke
					return false, nil
				}
				unexpectedDownload := func(string) error {
					t.Fatal("cleanup must finish before downloading")
					return nil
				}
				var result hfArtifactTaskResult
				var err error
				switch operation {
				case "delete":
					result, err = h.handleDelete(context.Background(), input)
				case "download":
					result, err = h.handleDownload(context.Background(), input, unexpectedDownload)
				case "repair":
					result, err = h.handleDownloadOverride(context.Background(), input, func(string) (bool, error) {
						t.Fatal("cleanup must finish before validating replacement bytes")
						return false, nil
					}, unexpectedDownload)
				}
				require.NoError(t, err)
				require.Equal(t, 1, preparations)
				require.GreaterOrEqual(t, checks, 3, "intent remains checked after phase preparation")
				pending, err := h.repository.pendingDeletion(context.Background(), input.ChildModelKey)
				require.NoError(t, err)
				if revoke {
					require.Equal(t, hfArtifactTaskRetry, result.Outcome)
					require.ErrorContains(t, result.RetryReason, "request revoked")
					require.NotNil(t, pending)
					require.True(t, h.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
				} else {
					require.Nil(t, pending)
					assertChildPathMissing(t, input.ChildModelPath)
				}
			})
		}
	}
}

func TestSharedCleanupFinishesUnderOperationLocks(t *testing.T) {
	for _, fail := range []bool{false, true} {
		h, input, _ := newRecoveryCoveragePendingDeletion(t)
		input.RetainDeletionReceipt = true
		called := false
		input.finishDeletion = func(ctx context.Context, pending HfArtifactPendingDeletion) error {
			called = true
			unlock, acquired, err := h.tryArtifactOperation(input)
			if acquired {
				unlock()
			}
			require.NoError(t, err)
			require.False(t, acquired)
			current, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, &pending, current)
			if fail {
				return errors.New("terminal state unavailable")
			}
			return h.repository.finishPendingDeletion(ctx, input.ChildModelKey, pending)
		}
		result, err := h.handleDelete(context.Background(), input)
		require.NoError(t, err)
		require.True(t, called)
		pending, err := h.repository.pendingDeletion(context.Background(), input.ChildModelKey)
		require.NoError(t, err)
		if fail {
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.ErrorContains(t, result.RetryReason, "terminal state unavailable")
			require.NotNil(t, pending)
		} else {
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			require.Nil(t, pending)
		}
	}
}
