package modelagent

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestSharedRestoreRefreshesMetadataAfterReadyReportCommits(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	s.metrics = NewMetrics(prometheus.NewRegistry())
	s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Second
	task.SamePathWaitStartedAt = time.Time{}
	task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
	current := task.BaseModel.DeepCopy()
	_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{current}}
	client := s.kubeClient.(*fake.Clientset)
	changed := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		if changed || err != nil || entry.Status != ModelStatusReady || entry.ArtifactRehydrationID != "request-1" {
			return false, nil, nil
		}
		// Commit the Ready report, then change authority before the Ready label publishes.
		require.NoError(t, client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, cm, cm.Namespace))
		changed = true
		current = current.DeepCopy()
		current.Annotations["description"] = "changed after report"
		_, err = s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
		require.NoError(t, err)
		s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{current}}
		return true, cm, nil
	})
	adapter := func(ctx context.Context, attempt *GopherTask, _ v1beta1.BaseModelSpec, _ bool) (bool, error) {
		result, err := s.runHfArtifactDownload(ctx, attempt, input, true, nil, nil)
		if err == nil && result.Outcome == hfArtifactTaskRetry {
			err = s.requeueHfArtifactTask(attempt, result)
		}
		return false, err
	}
	require.NoError(t, s.processTaskWithSourceAdapters(task, true, adapter, nil))
	require.True(t, changed, "metadata must change after an actual committed Ready report")
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Equal(t, ModelStatusReady, entry.Status)
	require.Equal(t, input.ChildModelUID, entry.ModelUID)
	require.Equal(t, "request-1", entry.ArtifactRehydrationID)
	node, err := client.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEqual(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)], "stale attempt must not publish Ready")
	refreshed := receiveDirectRetry(t, s)
	require.NotSame(t, task, refreshed)
	require.Equal(t, "changed after report", refreshed.BaseModel.Annotations["description"])
	require.NoError(t, s.processTaskWithSourceAdapters(refreshed, true, adapter, nil))
	node, err = client.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
}
