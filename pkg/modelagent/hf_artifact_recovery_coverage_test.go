package modelagent

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRecoveryCoverageDeferredStartupRetriesAPIOutage(t *testing.T) {
	for _, failedVerb := range []string{"get", "update"} {
		t.Run(failedVerb, func(t *testing.T) {
			h, first, _ := newTestHfArtifactRepair(t)
			ctx := context.Background()
			parent, acquired, err := h.repository.TryAcquireLockForRepair(ctx, first.Parent)
			require.NoError(t, err)
			require.True(t, acquired)
			lock, acquired, err := tryHfArtifactParentFileLock(parent, first.ModelStoreRoot)
			require.NoError(t, err)
			require.True(t, acquired)
			t.Cleanup(func() { _ = lock.Close() })
			startup := newHfArtifactStartup(h)
			require.NoError(t, startup.recover(ctx))
			require.Contains(t, startup.deferred, parent.Key)
			require.NoError(t, lock.Close())
			unavailable := true
			h.repository.configMaps.kubeClient.(*fake.Clientset).PrependReactor(failedVerb, "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if unavailable {
					return true, nil, errors.New("deferred recovery unavailable")
				}
				return false, nil, nil
			})
			require.ErrorContains(t, startup.recoverParent(ctx, parent.Key), "deferred recovery unavailable")
			assert.Contains(t, startup.deferred, parent.Key)
			assert.True(t, startup.needsValidation(parent.Key))
			unavailable = false
			require.NoError(t, startup.recoverParent(ctx, parent.Key))
			assert.NotContains(t, startup.deferred, parent.Key)
			stored, _, err := h.repository.Get(ctx, parent.Identity)
			require.NoError(t, err)
			assert.Equal(t, HfArtifactStatusFailed, stored.Status)
			assert.Empty(t, stored.LockID)
			assertChildSymlinkTarget(t, first.ChildModelPath, parent.LocalPath)
		})
	}
}
