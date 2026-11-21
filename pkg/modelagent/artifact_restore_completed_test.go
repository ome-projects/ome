package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestCompletedRestoreAllowsOrdinaryPolicyTransition(t *testing.T) {
	for _, mode := range []string{"normal", "demoted", "retry", "placement drift"} {
		t.Run(mode, func(t *testing.T) {
			demote := mode == "demoted"
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			_, err = s.runHfArtifactDownload(ctx, task, input, true, nil, nil)
			require.NoError(t, err)
			node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.Labels["pool"] = "gpu"
			_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			policy := v1beta1.AlwaysDownload
			task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			task.BaseModel.Status.Rehydration = &v1beta1.ModelRehydrationStatus{CompletedRequestID: artifactRehydrationID(task)}
			_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
			s.metrics = NewMetrics(prometheus.NewRegistry())
			s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Minute
			downloads := 0
			source := directHfSource{resolve: func(context.Context, string, string, string, string) (string, error) {
				return input.Parent.Identity.CommitSHA, nil
			}, download: func(_ context.Context, _ *GopherTask, c *xet.DownloadConfig) error {
				downloads++
				if mode == "placement drift" && downloads == 1 {
					current := task.BaseModel.DeepCopy()
					current.Spec.Storage.NodeSelector = map[string]string{"pool": "gpu"}
					_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
					require.NoError(t, err)
					s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{current}}
				}
				if mode == "retry" && downloads == 1 {
					return errors.New("transient download failure")
				}
				require.NoError(t, os.MkdirAll(c.LocalDir, 0755))
				return os.WriteFile(filepath.Join(c.LocalDir, "config.json"), []byte("{}"), 0600)
			}}
			adapter := func(ctx context.Context, task *GopherTask, spec v1beta1.BaseModelSpec, allow bool) (bool, error) {
				return source.process(ctx, s, task, spec, allow)
			}
			if mode == "retry" {
				require.ErrorContains(t, s.processTaskWithSourceAdapters(task, true, adapter, nil), "transient download failure")
				cm, err := s.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				require.Empty(t, entry.HfArtifactKey)
				require.NotEqual(t, ModelStatusReady, entry.Status)
			}
			require.NoError(t, s.processTaskWithSourceAdapters(task, !demote, adapter, nil))
			if mode == "placement drift" {
				cm, err := s.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				require.Equal(t, ModelStatusUpdating, entry.Status)
				require.Empty(t, entry.HfArtifactKey)
				task = receiveDirectRetry(t, s)
				require.NoError(t, s.processTaskWithSourceAdapters(task, true, adapter, nil))
			}
			if demote {
				require.Zero(t, downloads)
				var ok bool
				task, ok = s.taskQueue.popNormal()
				require.True(t, ok)
				require.NoError(t, s.processTaskWithSourceAdapters(task, true, adapter, nil))
			}
			expected := 1
			if mode == "retry" || mode == "placement drift" {
				expected = 2
			}
			require.Equal(t, expected, downloads)
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusReady, entry.Status)
			require.Empty(t, entry.HfArtifactKey)
			require.Equal(t, "request-1", task.BaseModel.Annotations[testRestoreAnnotation])
			_, found, err := s.sharedHfArtifactHandler().repository.GetParentForChild(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestOrdinaryPolicyTransitionRequiresThisNodesCompletedReport(t *testing.T) {
	for _, mode := range []string{"new request", "not completed", "foreign UID", "pending cleanup", "missing report", "unreadable report"} {
		t.Run(mode, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			entry.Status, entry.ModelUID, entry.ArtifactRehydrationID = ModelStatusReady, input.ChildModelUID, "request-1"
			switch mode {
			case "new request":
				task.BaseModel.Annotations[testRestoreAnnotation] = "request-2"
			case "not completed":
				entry.Status = ModelStatusUpdating
			case "foreign UID":
				entry.ModelUID = "other-model"
			case "pending cleanup":
				entry.HfArtifactPendingDeletion = &HfArtifactPendingDeletion{}
			}
			_, err = writeModelEntry(cm.Data, input.ChildModelKey, entry)
			require.NoError(t, err)
			if mode == "missing report" {
				delete(cm.Data, input.ChildModelKey)
			}
			if mode == "unreadable report" {
				cm.Data[input.ChildModelKey] = "{"
			}
			_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			policy := v1beta1.AlwaysDownload
			task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			task.BaseModel.Status.Rehydration = &v1beta1.ModelRehydrationStatus{CompletedRequestID: artifactRehydrationID(task)}
			_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
			err = s.processTaskWithSourceAdapters(task, true, func(context.Context, *GopherTask, v1beta1.BaseModelSpec, bool) (bool, error) {
				t.Fatal("unproved retained request must not enter ordinary source")
				return false, nil
			}, nil)
			require.Error(t, err)
			require.Nil(t, task.completedArtifactRequest)
		})
	}
}

func TestCompletedRestoreAllowsOrdinaryOCIPolicyTransition(t *testing.T) {
	for _, demote := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "demoted"}[demote], func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			_, err := s.runHfArtifactDownload(ctx, task, input, true, nil, nil)
			require.NoError(t, err)
			policy := v1beta1.AlwaysDownload
			task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
			s.metrics = NewMetrics(prometheus.NewRegistry())
			s.modelConfigParser = modelparser.NewModelConfigParser(s.logger)
			s.downloadRetry = 1
			downloads := 0
			downloader := func(_ context.Context, _ *ociobjectstore.ObjectURI, path string, _ *GopherTask) error {
				downloads++
				require.False(t, isSharedHfArtifactSymlink(path), "detach must precede ordinary OCI download")
				require.NoError(t, os.MkdirAll(path, 0755))
				return os.WriteFile(filepath.Join(path, "config.json"), []byte("{}"), 0600)
			}
			require.NoError(t, s.processTaskWithSourceAdapters(task, !demote, nil, downloader))
			if demote {
				require.Zero(t, downloads)
				var ok bool
				task, ok = s.taskQueue.popNormal()
				require.True(t, ok)
				require.NoError(t, s.processTaskWithSourceAdapters(task, true, nil, downloader))
			}
			require.Equal(t, 1, downloads)
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusReady, entry.Status)
			require.Empty(t, entry.HfArtifactKey)
			_, found, err := s.sharedHfArtifactHandler().repository.GetParentForChild(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.False(t, found)
			require.Equal(t, "request-1", task.BaseModel.Annotations[testRestoreAnnotation])
		})
	}
}
