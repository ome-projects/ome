package modelagent

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestHfArtifactEvictionGuardRetainsParent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		children  int
		consumers bool
	}{
		{"one child with parent consumer", 1, true},
		{"two siblings with parent consumer", 2, true},
		{"unreferenced parent", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			root := t.TempDir()
			inputs := make([]hfArtifactTaskInput, tc.children)
			for i := range inputs {
				inputs[i] = testHfArtifactTaskInput(t, root, fmt.Sprintf("child-%d", i))
				seedTestChildModelEntry(t, repository, inputs[i])
				require.NoError(t, runTestHfArtifactDownload(handler, inputs[i]))
			}
			calls := 0
			handler.hasParentConsumers = func(callbackCtx context.Context, parent HfArtifactEntry) (bool, error) {
				calls++
				require.Equal(t, ctx, callbackCtx)
				require.Equal(t, inputs[0].Parent.LocalPath, parent.LocalPath)
				require.Empty(t, parent.Children)
				require.NotEmpty(t, parent.LockID)
				require.Equal(t, HfArtifactStatusUpdating, parent.Status)
				return tc.consumers, nil
			}
			for i, input := range inputs {
				result, err := handler.handleDelete(ctx, input)
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome)
				assertChildPathMissing(t, input.ChildModelPath)
				pending, err := repository.pendingDeletion(ctx, input.ChildModelKey)
				require.NoError(t, err)
				require.Nil(t, pending)
				if i < len(inputs)-1 {
					require.Zero(t, calls, "only the last sibling may check parent deletion")
					require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
				}
			}
			require.Equal(t, 1, calls)
			parent, found, err := repository.Get(ctx, inputs[0].Parent.Identity)
			require.NoError(t, err)
			if tc.consumers {
				require.True(t, found)
				require.Empty(t, parent.Children)
				require.Empty(t, parent.LockID)
				require.Equal(t, HfArtifactStatusReady, parent.Status)
				require.FileExists(t, filepath.Join(parent.LocalPath, "config.json"))
			} else {
				require.False(t, found)
				require.NoDirExists(t, inputs[0].Parent.LocalPath)
			}
		})
	}
}

func TestHfArtifactEvictionGuardAPIFailurePreservesReceipt(t *testing.T) {
	for _, resource := range []string{"pods", "basemodels"} {
		t.Run(resource, func(t *testing.T) {
			ctx := context.Background()
			g, _, input := newTestHfArtifactGopher(t)
			handler := g.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			unavailable := errors.New("parent consumer API unavailable")
			fail := true
			calls := 0
			reactor := func(k8stesting.Action) (bool, runtime.Object, error) {
				calls++
				if fail {
					return true, nil, unavailable
				}
				return false, nil, nil
			}
			if resource == "pods" {
				g.kubeClient.(*k8sfake.Clientset).PrependReactor("list", resource, reactor)
			} else {
				g.modelClient.(*omefake.Clientset).PrependReactor("list", resource, reactor)
			}

			result, err := handler.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.ErrorIs(t, result.RetryReason, unavailable)
			if resource == "pods" {
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			} else {
				assertChildPathMissing(t, input.ChildModelPath)
			}
			require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
			pending, err := handler.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, pending)
			parent, found, err := handler.repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			require.Empty(t, parent.Children)
			require.Equal(t, HfArtifactStatusUpdating, parent.Status)
			require.NotEmpty(t, parent.LockID)
			require.Equal(t, pending.ParentLockID, parent.LockID)

			fail = false
			result, err = handler.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			if resource == "pods" {
				require.Equal(t, 3, calls, "check the child before unlink and the parent after unlink")
			} else {
				require.Equal(t, 2, calls)
			}
			require.NoDirExists(t, input.Parent.LocalPath)
			pending, err = handler.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, pending)
			_, found, err = handler.repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestHfArtifactEvictionGuardMissingClientFailsClosed(t *testing.T) {
	for _, missing := range []string{"pod", "model"} {
		t.Run(missing, func(t *testing.T) {
			ctx := context.Background()
			g, _, input := newTestHfArtifactGopher(t)
			handler := g.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			podClient, modelClient := g.kubeClient, g.modelClient
			if missing == "pod" {
				g.kubeClient = nil
			} else {
				g.modelClient = nil
			}
			result, err := handler.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.ErrorContains(t, result.RetryReason, "live "+missing+" client")
			require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
			pending, err := handler.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.NotNil(t, pending)

			g.kubeClient, g.modelClient = podClient, modelClient
			result, err = handler.handleDelete(ctx, input)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			require.NoDirExists(t, input.Parent.LocalPath)
			pending, err = handler.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, pending)
		})
	}
}
