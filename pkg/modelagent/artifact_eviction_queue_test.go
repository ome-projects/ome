package modelagent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/modelparser"
)

func TestRejectedArtifactEvictionPreservesQueuedDownload(t *testing.T) {
	for _, protection := range []string{"service", "reserve"} {
		t.Run(protection, func(t *testing.T) {
			g, eviction, path := newArtifactEvictionTestModel(t)
			eviction.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
			eviction.BaseModel.Annotations[ConfigParsingAnnotation] = "true"
			if protection == "reserve" {
				eviction.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
			}
			_, err := g.modelClient.OmeV1beta1().BaseModels(eviction.BaseModel.Namespace).Update(context.Background(), eviction.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			model := eviction.BaseModel.DeepCopy()
			// Restart must retain the ordinary download replay even when the stored
			// eviction request is later refused because a consumer still needs it.
			queue := make(chan *GopherTask, 2)
			scout := &Scout{gopherChan: queue, logger: g.logger}
			scout.enqueueBaseModelDownload(model)
			require.Len(t, queue, 2, "eviction intent must not replace startup download intent")
			download, eviction := <-queue, <-queue
			require.Equal(t, Download, download.TaskType)
			require.Equal(t, Evict, eviction.TaskType)
			g.enqueueTask(download)
			g.enqueueTask(eviction)
			defer g.taskQueue.close()
			if protection == "service" {
				service := &v1beta1.InferenceService{
					ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: model.Namespace},
					Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: model.Name}},
				}
				_, err = g.modelClient.OmeV1beta1().InferenceServices(model.Namespace).Create(context.Background(), service, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			g.gopherChan = make(chan *GopherTask, 1)
			g.samePathWaitDelay = time.Millisecond

			queued, ok := g.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, eviction, queued)
			require.NoError(t, g.processTask(queued))
			if protection == "reserve" {
				select {
				case retry := <-g.gopherChan:
					require.Same(t, eviction, retry)
				case <-time.After(time.Second):
					t.Fatal("reserved eviction was not requeued")
				}
			}
			require.DirExists(t, path)
			require.Equal(t, 1, g.taskQueue.len(), "rejected eviction must retain the queued download")
			queued, ok = g.taskQueue.popHighPriority()
			require.True(t, ok)
			require.Same(t, download, queued)
			index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			require.NoError(t, index.Add(eviction.BaseModel))
			// A Ready copy at the same path exercises the full ordinary OCI task
			// without requiring a remote download in this regression test.
			peer := model.DeepCopy()
			peer.Name, peer.UID = "same-path-copy", "peer-uid"
			require.NoError(t, index.Add(peer))
			g.baseModelLister = modelslister.NewBaseModelLister(index)
			g.modelConfigParser = modelparser.NewModelConfigParser(g.modelClient, g.logger)
			g.metrics = NewMetrics(prometheus.NewRegistry())
			cm, err := g.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			cm.Data[getModelID(peer, nil)] = modelEntryJSON(ModelStatusReady)
			cm.Data[getModelID(model, nil)] = modelEntryJSON(ModelStatusUpdating)
			_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, g.processTask(queued))
			cm, err = g.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			require.True(t, hasModelEntryStatus(cm.Data[getModelID(model, nil)], ModelStatusReady), "retained download must finish, not just survive queue admission")
		})
	}
}

func TestArtifactEvictionScoutPreservesDownloadIntent(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, event := range []string{"startup", "source", "policy", "annotation", "unchanged", "residency-only"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, event), func(t *testing.T) {
				queue := make(chan *GopherTask, 3)
				scout := &Scout{gopherChan: queue, nodeInfo: &corev1.Node{}, logger: zap.NewNop().Sugar()}
				old := &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "uid",
						Labels:      map[string]string{constants.ReserveModelArtifact: "true"},
						Annotations: map[string]string{constants.ModelArtifactResidencyAnnotation: constants.ModelArtifactResidencyEvicted}},
					Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("hf://org/model"), Path: stringPtr("/models/model")}},
				}
				updated := old.DeepCopy()
				expected := []GopherTaskType{DownloadOverride, Evict}
				switch event {
				case "startup":
					expected[0] = Download
				case "source":
					updated.Spec.Storage.StorageUri = stringPtr("hf://org/model@new-revision")
				case "policy":
					policy := v1beta1.ReuseIfExists
					updated.Spec.Storage.DownloadPolicy = &policy
				case "annotation":
					updated.Annotations["refresh"] = "new"
				case "residency-only":
					delete(old.Annotations, constants.ModelArtifactResidencyAnnotation)
					expected = []GopherTaskType{Evict}
				case "unchanged":
					expected = []GopherTaskType{Evict}
				}
				if cluster {
					before := &v1beta1.ClusterBaseModel{ObjectMeta: old.ObjectMeta, Spec: old.Spec}
					after := &v1beta1.ClusterBaseModel{ObjectMeta: updated.ObjectMeta, Spec: updated.Spec}
					before.Namespace, after.Namespace = "", ""
					if event == "startup" {
						scout.enqueueClusterBaseModelDownload(after)
					} else {
						scout.updateClusterBaseModel(before, after)
					}
				} else if event == "startup" {
					scout.enqueueBaseModelDownload(updated)
				} else {
					scout.updateBaseModel(old, updated)
				}
				require.Len(t, queue, len(expected))
				for _, taskType := range expected {
					require.Equal(t, taskType, (<-queue).TaskType, "eviction must have a newer sequence than preserved download intent")
				}
			})
		}
	}
}

func TestRejectedArtifactEvictionAllowsDownloadReady(t *testing.T) {
	g, eviction, path := newArtifactEvictionTestModel(t)
	model := eviction.BaseModel.DeepCopy()
	delete(model.Annotations, constants.ModelArtifactResidencyAnnotation)
	download := &GopherTask{TaskType: Download, BaseModel: model}
	ctx, finish, proceed, err := g.beginTask(download)
	defer finish(false)
	require.NoError(t, err)
	require.True(t, proceed)
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: model, ModelStateOnNode: Updating}))
	service := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: model.Namespace},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: model.Name}},
	}
	_, err = g.modelClient.OmeV1beta1().InferenceServices(model.Namespace).Create(ctx, service, metav1.CreateOptions{})
	require.NoError(t, err)
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, index.Add(eviction.BaseModel))
	g.baseModelLister = modelslister.NewBaseModelLister(index)
	g.enqueueTask(eviction)
	defer g.taskQueue.close()
	require.NoError(t, g.processTask(eviction))
	require.NoError(t, ctx.Err())

	// This is the post-download check, after a rejected eviction has left the
	// old writer running and the CR still carries its eviction request.
	skip, _, err := g.shouldSkipStaleDownloadTask(ctx, download)
	require.NoError(t, err)
	require.False(t, skip, "completed protected downloads must still publish Ready")
	published, err := g.finishDownloadStatus(ctx, download, &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready})
	require.NoError(t, err)
	require.True(t, published)
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	require.True(t, hasModelEntryStatus(cm.Data[getModelID(model, nil)], ModelStatusReady))
	require.DirExists(t, path)
}

func TestRejectedArtifactEvictionRetriesUnavailableState(t *testing.T) {
	for _, stage := range []string{"before-download", "before-ready"} {
		for _, failure := range []string{"read-error", "corrupt-entry", "canceled"} {
			t.Run(stage+"/"+failure, func(t *testing.T) {
				ctx := context.Background()
				g, eviction, path := newArtifactEvictionTestModel(t)
				eviction.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
				eviction.BaseModel.Annotations[ConfigParsingAnnotation] = "true"
				_, err := g.modelClient.OmeV1beta1().BaseModels(eviction.BaseModel.Namespace).Update(ctx, eviction.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
				model := eviction.BaseModel.DeepCopy()
				delete(model.Annotations, constants.ModelArtifactResidencyAnnotation)
				download := &GopherTask{TaskType: Download, BaseModel: model}
				g.enqueueTask(download)
				g.enqueueTask(eviction)
				defer g.taskQueue.close()
				_, err = g.kubeClient.CoreV1().Pods(model.Namespace).Create(ctx, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "direct-consumer"},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{Path: path},
					}}}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
				queued, ok := g.taskQueue.popHighPriority()
				require.True(t, ok)
				require.Same(t, eviction, queued)
				require.NoError(t, g.processTask(queued))
				queued, ok = g.taskQueue.popHighPriority()
				require.True(t, ok)
				require.Same(t, download, queued)

				peer := model.DeepCopy()
				peer.Name, peer.UID = "same-path-copy", "peer-uid"
				index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
				require.NoError(t, index.Add(eviction.BaseModel))
				require.NoError(t, index.Add(peer))
				g.baseModelLister = modelslister.NewBaseModelLister(index)
				g.modelConfigParser = modelparser.NewModelConfigParser(g.modelClient, g.logger)
				g.metrics = NewMetrics(prometheus.NewRegistry())
				g.gopherChan = make(chan *GopherTask, 1)
				g.samePathWaitDelay = time.Millisecond
				require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: model, ModelStateOnNode: Updating}))
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				cm.Data[getModelID(peer, nil)] = modelEntryJSON(ModelStatusReady)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)

				// The local reuse path reads eviction state and any pending direct
				// cleanup, writes Updating, then reads the Ready peer before its
				// final eviction check. Inject at that final check, not at a write.
				failRead := 1
				if stage == "before-ready" {
					failRead = 6
				}
				reads, failures := 0, 0
				g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
					reads++
					if reads != failRead {
						return false, nil, nil
					}
					failures++
					if failure == "canceled" {
						attempt, outcome := g.taskTracker.beginDelete(gopherTaskModelKey(download), g.taskTracker.ensureSequence(0))
						require.Equal(t, gopherTaskWait, outcome)
						g.taskTracker.finishDelete(attempt, true)
						return true, nil, context.Canceled
					}
					if failure == "read-error" {
						return true, nil, fmt.Errorf("eviction state temporarily unavailable")
					}
					corrupt := cm.DeepCopy()
					corrupt.Data[getModelID(model, nil)] = "{"
					return true, corrupt, nil
				})
				err = g.processTask(queued)
				if failure == "canceled" {
					require.ErrorIs(t, err, context.Canceled)
					require.True(t, download.SamePathWaitStartedAt.IsZero(), "canceled eviction-state lookup must not schedule a retry")
					require.Equal(t, 1, failures)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 1, failures)
				cm, err = g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				require.True(t, hasModelEntryStatus(cm.Data[getModelID(model, nil)], ModelStatusUpdating), "unreadable state must not permit Ready")
				node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, cm.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, "Updating", node.Labels[constants.GetBaseModelLabel(model.Namespace, model.Name)])

				select {
				case retry := <-g.gopherChan:
					require.Same(t, download, retry)
					require.NoError(t, g.processTask(retry))
				case <-time.After(time.Second):
					t.Fatal("unavailable eviction state discarded the download instead of requeueing")
				}
				cm, err = g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				require.True(t, hasModelEntryStatus(cm.Data[getModelID(model, nil)], ModelStatusReady))
				node, err = g.kubeClient.CoreV1().Nodes().Get(ctx, cm.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, "Ready", node.Labels[constants.GetBaseModelLabel(model.Namespace, model.Name)])
				require.DirExists(t, path)
			})
		}
	}
}

func TestAcceptedArtifactEvictionFencesQueuedDownload(t *testing.T) {
	g, eviction, path := newArtifactEvictionTestModel(t)
	model := eviction.BaseModel.DeepCopy()
	delete(model.Annotations, constants.ModelArtifactResidencyAnnotation)
	download := &GopherTask{TaskType: DownloadOverride, BaseModel: model}
	g.enqueueTask(download)
	g.enqueueTask(eviction)
	defer g.taskQueue.close()

	queued, ok := g.taskQueue.popHighPriority()
	require.True(t, ok)
	require.Same(t, eviction, queued)
	require.NoError(t, g.processTask(queued))
	require.NoDirExists(t, path)
	require.Equal(t, 1, g.taskQueue.len(), "the tracker, not queue pruning, fences superseded work")
	queued, ok = g.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, download, queued)
	_, finish, proceed, err := g.beginTask(queued)
	finish(false)
	require.NoError(t, err)
	require.False(t, proceed, "completed eviction must fence older queued downloads")

	delete(model.Annotations, constants.ModelArtifactResidencyAnnotation)
	model.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-1"
	_, err = g.modelClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
	require.NoError(t, err)
	hydration := &GopherTask{TaskType: Download, BaseModel: model}
	g.enqueueTask(hydration)
	queued, ok = g.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, hydration, queued)
	_, finish, proceed, err = g.beginTask(queued)
	defer finish(false)
	require.NoError(t, err)
	require.True(t, proceed, "a newer hydration task must remain admissible")
}

func TestRejectedArtifactEvictionStateRetryExhaustion(t *testing.T) {
	g, eviction, path := newArtifactEvictionTestModel(t)
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, index.Add(eviction.BaseModel))
	g.baseModelLister = modelslister.NewBaseModelLister(index)
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitTimeout = time.Second
	download := &GopherTask{
		TaskType: Download, BaseModel: eviction.BaseModel.DeepCopy(),
		SamePathWaitStartedAt: time.Now().Add(-2 * time.Second),
	}
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("eviction state unavailable")
	})
	err := g.processTask(download)
	require.ErrorContains(t, err, "retry budget exhausted")
	require.ErrorContains(t, err, "eviction state unavailable")
	require.Empty(t, g.gopherChan)
	require.DirExists(t, path)
}

func TestCompletedArtifactEvictionBlocksLateDownloads(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, state := range []string{"evicted", "updating", "missing-entry", "missing-configmap", "read-error", "corrupt-entry"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, state), func(t *testing.T) {
				g, eviction, _ := newArtifactEvictionTestModel(t)
				task := &GopherTask{TaskType: Download, BaseModel: eviction.BaseModel.DeepCopy()}
				index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
				if cluster {
					model := &v1beta1.ClusterBaseModel{ObjectMeta: *task.BaseModel.ObjectMeta.DeepCopy(), Spec: task.BaseModel.Spec}
					model.Namespace = ""
					require.NoError(t, index.Add(model))
					g.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(index)
					task.BaseModel, task.ClusterBaseModel = nil, model.DeepCopy()
				} else {
					require.NoError(t, index.Add(task.BaseModel.DeepCopy()))
					g.baseModelLister = modelslister.NewBaseModelLister(index)
				}
				taskModelMeta(task).Annotations = nil
				ctx := context.Background()
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				key := getModelID(task.BaseModel, task.ClusterBaseModel)
				cm.Data[key] = modelEntryJSON(ModelStatusEvicted)
				switch state {
				case "updating":
					cm.Data[key] = modelEntryJSON(ModelStatusUpdating)
				case "missing-entry":
					delete(cm.Data, key)
				case "corrupt-entry":
					cm.Data[key] = "{"
				}
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
				if state == "missing-configmap" {
					require.NoError(t, g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{}))
				}
				if state == "read-error" {
					g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
						return true, nil, fmt.Errorf("unavailable")
					})
				}
				skip, deleteCR, err := g.shouldSkipStaleDownloadTask(ctx, task)
				require.Equal(t, state == "evicted" || state == "read-error" || state == "corrupt-entry", skip)
				require.False(t, deleteCR)
				if state == "read-error" || state == "corrupt-entry" {
					require.Error(t, err, "unavailable state must be retried, not treated as completed eviction")
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
