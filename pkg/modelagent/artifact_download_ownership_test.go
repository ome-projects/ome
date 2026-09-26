package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestSharedDownloadChecksLiveOwnerUnderBothLocks(t *testing.T) {
	for _, kind := range []GopherTaskType{Download, DownloadOverride} {
		for _, state := range []string{"current", "replaced", "canceled"} {
			t.Run(fmt.Sprintf("%s/%s", kind, state), func(t *testing.T) {
				g, task, input := newTestHfArtifactGopher(t)
				defer g.taskQueue.close()
				task.TaskType = kind
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reads := 0
				g.modelClient.(*omefake.Clientset).PrependReactor("get", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) {
					reads++
					parent, acquired, err := tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
					require.NoError(t, err)
					if acquired {
						require.NoError(t, parent.Close())
					}
					require.False(t, acquired, "live validation must own the parent lock")
					child, acquired, err := tryHfArtifactChildFileLock(input)
					require.NoError(t, err)
					if acquired {
						require.NoError(t, child.Close())
					}
					require.False(t, acquired, "live validation must own the child lock")
					if state == "canceled" {
						cancel()
						return true, nil, ctx.Err()
					}
					live := task.BaseModel.DeepCopy()
					if state == "replaced" {
						live.UID = "replacement-uid"
					}
					return true, live, nil
				})
				writes := 0
				result, err := g.runHfArtifactDownload(ctx, task, input, true, func(string) (bool, error) { return false, nil }, func(path string) error {
					writes++
					require.NoError(t, os.MkdirAll(path, 0755))
					return os.WriteFile(filepath.Join(path, "config.json"), []byte("{}"), 0600)
				})
				require.Positive(t, reads)
				if state == "current" {
					require.NoError(t, err)
					require.Equal(t, hfArtifactTaskDone, result.Outcome)
					require.Equal(t, 1, writes)
					return
				}
				require.Zero(t, writes)
				assertChildPathMissing(t, input.ChildModelPath)
				if state == "canceled" {
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, result.Outcome, "cancellation must not turn into a retry")
				} else {
					require.NoError(t, err)
					require.Equal(t, hfArtifactTaskRetry, result.Outcome)
				}
			})
		}
	}
}

func TestSharedDownloadRejectsChildChangedDuringTransfer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.BaseModel)
	}{
		{name: "UID", change: func(model *v1beta1.BaseModel) { model.UID = "replacement-uid" }},
		{name: "source", change: func(model *v1beta1.BaseModel) {
			model.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/replacement")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newTestHfArtifactGopher(t)
			defer g.taskQueue.close()
			writes := 0
			result, err := g.runHfArtifactDownload(ctx, task, input, true, nil, func(path string) error {
				writes++
				latest := task.BaseModel.DeepCopy()
				tc.change(latest)
				_, err := g.modelClient.OmeV1beta1().BaseModels(latest.Namespace).Update(ctx, latest, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(path, "config.json"), []byte("complete"), 0600)
			})
			require.NoError(t, err)
			require.Equal(t, 1, writes)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.ErrorContains(t, result.RetryReason, "no longer current")
			assertChildPathMissing(t, input.ChildModelPath)
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Empty(t, entry.HfArtifactKey)
			parent, found, err := g.sharedHfArtifactHandler().repository.Get(ctx, input.Parent.Identity)
			require.NoError(t, err)
			require.True(t, found)
			require.Empty(t, parent.Children)
		})
	}
}
