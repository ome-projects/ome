package modelagent

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestDirectReceiptDeleteRetryAfterSharedPolicyChange(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newEvictionTestModel(t)
			g.gopherChan = make(chan *GopherTask, 1)
			g.samePathWaitDelay = time.Millisecond
			require.NoError(t, g.persistDirectEviction(ctx, task, path, false))
			// The original direct receipt survives an edit making the current
			// source shared-eligible; placement loss still authorizes Delete.
			if shared {
				policy := v1beta1.ReuseIfExists
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("hf://org/model")
				task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			}
			task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"pool": "another-node"}
			_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			task.TaskType = Delete
			patches := 0
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				patches++
				if patches == 1 {
					return true, nil, fmt.Errorf("temporary readiness failure")
				}
				return false, nil, nil
			})
			require.NoError(t, g.processTask(task))
			require.Equal(t, shared, task.SharedArtifact)
			require.Equal(t, 1, patches)
			require.NotNil(t, task.directEvictionDelete)
			require.FileExists(t, filepath.Join(path, "weights"))
			sequence := task.Sequence
			select {
			case retry := <-g.gopherChan:
				require.Same(t, task, retry)
				require.Equal(t, sequence, retry.Sequence)
				require.NoError(t, g.processTask(retry))
			case <-time.After(time.Second):
				t.Fatal("transient cleanup failure did not queue a retry")
			}
			require.NoDirExists(t, path, "the unchanged receipt must actually resume, not be discarded as finished")
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil))
		})
	}
}
