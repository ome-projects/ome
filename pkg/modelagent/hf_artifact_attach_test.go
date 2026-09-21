package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestHfArtifactStaleAttachRemovesOnlyNewLink(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			repository, client := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			seedTestChildModelEntry(t, repository, input)
			parent, acquired, err := repository.TryAcquireLock(context.Background(), input.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			require.NoError(t, writeTestHfArtifactFiles(parent.LocalPath))
			require.NoError(t, handler.files.WriteParentReadyMarker(parent))
			require.NoError(t, repository.MarkReady(context.Background(), parent))
			if existing {
				require.NoError(t, handler.files.CreateChildSymlink(input.ChildModelPath, parent.LocalPath))
			}
			invalidateOnAttach(t, repository, client, input)
			result, err := handler.handleDownload(context.Background(), input, nil)
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskDone, result.Outcome)
			if existing {
				assertChildSymlinkTarget(t, input.ChildModelPath, parent.LocalPath)
			} else {
				assertChildPathMissing(t, input.ChildModelPath)
			}
		})
	}
}

func invalidateOnAttach(t *testing.T, repository *HfArtifactRepository, client *fake.Clientset, input hfArtifactTaskInput) {
	t.Helper()
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if (hfArtifactFiles{}).IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath) {
			// GET may run under mutationMutex. Inject the newer informer-cache
			// view directly; invoking the public invalidator here would deadlock.
			repository.configMaps.cacheMutex.Lock()
			repository.configMaps.modelCache[input.ChildModelKey] = &CacheEntry{ModelUID: types.UID("replacement-uid")}
			repository.configMaps.cacheMutex.Unlock()
		}
		return false, nil, nil
	})
}
