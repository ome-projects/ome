package modelagent

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestHfArtifactRetriesUncertainAcquisition(t *testing.T) {
	for _, repair := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			name := map[bool]string{false: "download", true: "repair"}[repair] + "/" + map[bool]string{false: "rejected", true: "lost response"}[committed]
			t.Run(name, func(t *testing.T) {
				repository, client := newTestHfArtifactRepository(t, map[string]string{})
				h := newHfArtifactTaskHandler(repository)
				input := testHfArtifactTaskInput(t, t.TempDir(), "model-1")
				seedTestChildModelEntry(t, repository, input)
				if repair {
					require.NoError(t, runTestHfArtifactDownload(h, input))
				}
				failed := false
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					if failed {
						return false, nil, nil
					}
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					parent, err := decodeHfArtifactEntry(input.Parent.Key, cm.Data[input.Parent.Key])
					require.NoError(t, err)
					require.Equal(t, HfArtifactStatusUpdating, parent.Status)
					failed = true
					if committed {
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					}
					return true, nil, errors.New("acquisition response unavailable")
				})
				calls := 0
				run := func() (hfArtifactTaskResult, error) {
					if repair {
						return h.handleDownloadOverride(context.Background(), input, func(string) (bool, error) {
							calls++
							return true, nil
						}, nil)
					}
					return h.handleDownload(context.Background(), input, func(path string) error {
						calls++
						return writeTestHfArtifactFiles(path)
					})
				}
				result, err := run()
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskRetry, result.Outcome)
				require.Zero(t, calls, "uncertain acquisition must not authorize work")
				result, err = run()
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome, "%v", result.RetryReason)
				require.Equal(t, 1, calls)
				parent, found, err := repository.Get(context.Background(), input.Parent.Identity)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, HfArtifactStatusReady, parent.Status)
				_, pending := h.pendingFailures.Load(parent.Key)
				require.False(t, pending)
			})
		}
	}
}
