package modelagent

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestSharedRestoreRefreshesMetadataSnapshotAndCompletes(t *testing.T) {
	for _, stage := range []string{"queued", "publication"} {
		t.Run(stage, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			s.metrics = NewMetrics(prometheus.NewRegistry())
			s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Minute
			task.SamePathWaitStartedAt = time.Time{}
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			current := task.BaseModel.DeepCopy()
			if stage == "queued" {
				current.Annotations["description"] = "updated"
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{current}}
			calls := 0
			adapter := func(ctx context.Context, attempt *GopherTask, _ v1beta1.BaseModelSpec, _ bool) (bool, error) {
				calls++
				if stage == "publication" && calls == 1 {
					current = current.DeepCopy()
					current.Annotations["description"] = "updated"
					_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
					require.NoError(t, err)
					return false, s.validateArtifactDownload(ctx, attempt)
				}
				_, err := s.runHfArtifactDownload(ctx, attempt, input, true, nil, nil)
				return false, err
			}
			require.NoError(t, s.processTaskWithSourceAdapters(task, true, adapter, nil))
			refreshed := receiveDirectRetry(t, s)
			require.NotSame(t, task, refreshed)
			require.Equal(t, "updated", refreshed.BaseModel.Annotations["description"])
			require.Equal(t, task.Sequence, refreshed.Sequence, "metadata refresh retains the logical request ordering")
			require.Equal(t, "", task.BaseModel.Annotations["description"], "do not mutate the original snapshot")
			require.NoError(t, s.processTaskWithSourceAdapters(refreshed, true, adapter, nil))
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusReady, entry.Status)
			require.Equal(t, "request-1", entry.ArtifactRehydrationID)
			select {
			case <-s.gopherChan:
				t.Fatal("completed restore must not refresh again")
			default:
			}
		})
	}
}

func TestSharedRestoreMetadataRefreshDoesNotReviveInvalidIntent(t *testing.T) {
	for _, mode := range []string{"request changed", "evicted", "replaced", "node ineligible"} {
		t.Run(mode, func(t *testing.T) {
			s, task, _ := newSharedRestoreFixture(t)
			s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Second
			task.SamePathWaitStartedAt = time.Time{}
			current := task.BaseModel.DeepCopy()
			current.Annotations["description"] = "updated"
			switch mode {
			case "request changed":
				current.Annotations[testRestoreAnnotation] = "request-2"
			case "evicted":
				current.Annotations[artifactResidencyAnnotation] = "Evicted"
			case "replaced":
				current.UID = "replacement"
			case "node ineligible":
				current.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
				task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{current}}
			if mode == "replaced" {
				require.NoError(t, s.processTask(task), "the coordinator skips replaced models")
			} else {
				require.Error(t, s.processTask(task))
			}
			select {
			case <-s.gopherChan:
				t.Fatal("invalid intent must not requeue")
			default:
			}
		})
	}
}

func TestSharedRestoreMetadataRefreshHasBoundedWait(t *testing.T) {
	s, task, _ := newSharedRestoreFixture(t)
	s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Second
	task.SamePathWaitStartedAt = time.Time{}
	for i := 0; i < 2; i++ {
		current := task.BaseModel.DeepCopy()
		current.Annotations["description"] = fmt.Sprint(i)
		_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
		require.NoError(t, err)
		if i == 0 {
			require.NoError(t, s.processTask(task))
			task = receiveDirectRetry(t, s)
			task.restoreRefreshStartedAt = time.Now().Add(-time.Minute)
		} else {
			require.Error(t, s.processTask(task))
			select {
			case <-s.gopherChan:
				t.Fatal("expired refresh budget must not requeue")
			default:
			}
		}
	}
}

func TestSharedRestoreRefreshesEligiblePlacementSnapshot(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, placement := range []string{"selector", "affinity"} {
			t.Run(fmt.Sprintf("cluster=%v/%s", cluster, placement), func(t *testing.T) {
				s, task, input := newSharedRestoreFixture(t)
				s.metrics = NewMetrics(prometheus.NewRegistry())
				s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Second
				task.SamePathWaitStartedAt = time.Time{}
				task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
				if cluster {
					task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
					task.ClusterBaseModel.Namespace = ""
					task.BaseModel = nil
					s.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{task.ClusterBaseModel}}
					WithArtifactEviction(omefake.NewSimpleClientset(task.ClusterBaseModel.DeepCopy()))(s)
				} else {
					s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
					WithArtifactEviction(omefake.NewSimpleClientset(task.BaseModel.DeepCopy()))(s)
				}
				current, err := liveArtifactModel(context.Background(), s.omeClient, task)
				require.NoError(t, err)
				node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				node.Labels["pool"] = "gpu"
				_, err = s.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
				require.NoError(t, err)
				if placement == "selector" {
					taskModelSpec(current).Storage.NodeSelector = map[string]string{"pool": "gpu"}
				} else {
					taskModelSpec(current).Storage.NodeAffinity = &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"}}}}}}}
				}
				updateRestoreRefreshModel(t, s, current)
				reachedSource := errors.New("fresh snapshot reached source adapter")
				calls := 0
				adapter := func(ctx context.Context, attempt *GopherTask, _ v1beta1.BaseModelSpec, _ bool) (bool, error) {
					calls++
					guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard)
					require.NotNil(t, guard)
					require.False(t, guard.validated, "new attempt must establish readiness again")
					require.Equal(t, taskModelSpec(current).Storage, taskModelSpec(attempt).Storage)
					return false, reachedSource
				}
				require.NoError(t, s.processTaskWithSourceAdapters(task, true, adapter, nil))
				require.Zero(t, calls, "stale attempt must stop before source work")
				refreshed := receiveDirectRetry(t, s)
				require.Equal(t, task.TaskType, refreshed.TaskType)
				require.ErrorIs(t, s.processTaskWithSourceAdapters(refreshed, true, adapter, nil), reachedSource)
				require.Equal(t, 1, calls)
			})
		}
	}
}

func updateRestoreRefreshModel(t *testing.T, s *Gopher, current *GopherTask) {
	t.Helper()
	if current.BaseModel != nil {
		_, err := s.omeClient.OmeV1beta1().BaseModels(current.BaseModel.Namespace).Update(context.Background(), current.BaseModel, metav1.UpdateOptions{})
		require.NoError(t, err)
	} else {
		_, err := s.omeClient.OmeV1beta1().ClusterBaseModels().Update(context.Background(), current.ClusterBaseModel, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
}

func TestSharedRestoreMetadataRefreshUsesIndependentBudget(t *testing.T) {
	s, task, _ := newSharedRestoreFixture(t)
	s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Second
	expiredFileWait := time.Now().Add(-time.Minute)
	task.SamePathWaitStartedAt = expiredFileWait
	current := &GopherTask{BaseModel: task.BaseModel.DeepCopy()}
	current.BaseModel.Annotations["description"] = "updated"
	updateRestoreRefreshModel(t, s, current)
	require.NoError(t, s.processTask(task))
	refreshed := receiveDirectRetry(t, s)
	require.Equal(t, expiredFileWait, refreshed.SamePathWaitStartedAt)
	require.False(t, refreshed.restoreRefreshStartedAt.IsZero())
	require.False(t, s.requeueArtifactWait(refreshed, gopherTaskModelKey(refreshed)), "refresh must not extend the original file-wait budget")
	// A fresh worker must check current request authority before source work.
	current.BaseModel.Annotations[testRestoreAnnotation] = "request-2"
	updateRestoreRefreshModel(t, s, current)
	require.Error(t, s.processTaskWithSourceAdapters(refreshed, true, func(context.Context, *GopherTask, v1beta1.BaseModelSpec, bool) (bool, error) {
		t.Fatal("refreshed task cannot reuse the previous attempt's authority")
		return false, nil
	}, nil))
}

func TestSharedRestoreSnapshotRefreshPreservesScoutOverrideClassification(t *testing.T) {
	for _, changed := range []string{"source", "path", "policy"} {
		t.Run(changed, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Second
			task.SamePathWaitStartedAt = time.Time{}
			task.HfResolvedRevision = "old-source-revision"
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			current := &GopherTask{BaseModel: task.BaseModel.DeepCopy()}
			switch changed {
			case "source":
				current.BaseModel.Spec.Storage.StorageUri = ptr("hf://other/model")
			case "path":
				current.BaseModel.Spec.Storage.Path = ptr(input.ChildModelPath + "-new")
			case "policy":
				policy := v1beta1.AlwaysDownload
				current.BaseModel.Spec.Storage.DownloadPolicy = &policy
			}
			updateRestoreRefreshModel(t, s, current)
			require.NoError(t, s.processTask(task))
			refreshed := receiveDirectRetry(t, s)
			require.Equal(t, DownloadOverride, refreshed.TaskType)
			require.Empty(t, refreshed.HfResolvedRevision, "changed source execution must not inherit an old pinned revision")
			require.Equal(t, current.BaseModel.Spec.Storage, refreshed.BaseModel.Spec.Storage)
			if changed == "policy" {
				require.True(t, isDirectHfRestoreEligible(refreshed, taskModelSpec(refreshed).Storage), "Direct HF policy remains eligible for restoration")
			}
		})
	}
}

func TestSharedRestoreRefreshedIneligibleSourceStopsBeforeFiles(t *testing.T) {
	s, task, _ := newSharedRestoreFixture(t)
	s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Second
	task.SamePathWaitStartedAt = time.Time{}
	policy := v1beta1.AlwaysDownload
	task.BaseModel.Spec.Storage.DownloadPolicy = &policy
	task.BaseModel.Spec.Storage.StorageUri = ptr("local://" + *task.BaseModel.Spec.Storage.Path)
	current := &GopherTask{BaseModel: task.BaseModel.DeepCopy()}
	current.BaseModel.Annotations["description"] = "updated"
	updateRestoreRefreshModel(t, s, current)
	require.NoError(t, s.processTask(task))
	refreshed := receiveDirectRetry(t, s)
	require.Equal(t, current.BaseModel.Spec.Storage, refreshed.BaseModel.Spec.Storage)
	require.ErrorContains(t, s.processTaskWithSourceAdapters(refreshed, true, func(context.Context, *GopherTask, v1beta1.BaseModelSpec, bool) (bool, error) {
		t.Fatal("active restore with an ineligible source cannot touch files")
		return false, nil
	}, nil), "eligible HF or OCI source")
}
