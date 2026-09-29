package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func newArtifactReportRecoveryFixture(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	t.Helper()
	s, task, input := newSharedRestoreFixture(t)
	task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
	s.metrics = NewMetrics(prometheus.NewRegistry())
	s.samePathWaitDelay, s.samePathWaitTimeout = time.Millisecond, time.Minute
	task.SamePathWaitStartedAt = time.Time{}
	return s, task, input
}

// Only external byte validation/transfer is replaced; queue admission, live
// model checks, Shared attachment, report CAS, and Node patches all run normally.
func runArtifactReportRecoveryTask(s *Gopher, task *GopherTask, input hfArtifactTaskInput, validate func(string) (bool, error)) error {
	return s.processTaskWithSourceAdapters(task, true,
		func(ctx context.Context, task *GopherTask, _ v1beta1.BaseModelSpec, allow bool) (bool, error) {
			result, err := s.runHfArtifactDownload(ctx, task, input, allow, validate, writeTestHfArtifactFiles)
			if err == nil && result.Outcome == hfArtifactTaskRetry {
				return true, s.requeueHfArtifactTask(task, result)
			}
			return false, err
		}, nil)
}

func popArtifactRecovery(t *testing.T, s *Gopher) *GopherTask {
	t.Helper()
	require.Equal(t, 1, s.taskQueue.len())
	task, ok := s.taskQueue.popNormal()
	require.True(t, ok)
	require.True(t, task.ArtifactReportRecovery)
	require.Equal(t, Download, task.TaskType)
	return task
}

func TestArtifactReportRecoveryRepairsReportsAndPairedLabels(t *testing.T) {
	for _, missing := range []string{"entry", "report", "request label", "Ready label", "old request", "old node", "old model", "sibling repair"} {
		t.Run(missing, func(t *testing.T) {
			s, task, input := newArtifactReportRecoveryFixture(t)
			ctx := context.Background()
			require.NoError(t, runArtifactReportRecoveryTask(s, task, input, func(string) (bool, error) { return true, nil }))
			if missing == "request label" || missing == "Ready label" {
				node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				key := testRequestLabel(task)
				if missing == "Ready label" {
					key, err = getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
					require.NoError(t, err)
				}
				delete(node.Labels, key)
				_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				require.NoError(t, err)
			} else {
				cm, err := s.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				switch missing {
				case "report":
					entry.ArtifactRehydrationID, entry.NodeUID = "", ""
				case "old request":
					entry.ArtifactRehydrationID = "previous-request"
				case "old node":
					entry.NodeUID = "previous-node"
				case "old model":
					entry.ModelUID = "previous-model"
				case "sibling repair":
					entry.Status = ModelStatusUpdating
				}
				_, err = writeModelEntry(cm.Data, input.ChildModelKey, entry)
				require.NoError(t, err)
				if missing == "entry" {
					delete(cm.Data, input.ChildModelKey)
				}
				_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			require.NoError(t, s.recoverArtifactReports(ctx))
			recovery := popArtifactRecovery(t, s)
			validations := 0
			require.NoError(t, runArtifactReportRecoveryTask(s, recovery, input, func(string) (bool, error) { validations++; return true, nil }))
			require.Equal(t, 1, validations, "missing labels still require byte validation")
			require.NoError(t, s.validateArtifactRestoreReport(ctx, recovery))
			require.NoError(t, s.recoverArtifactReports(ctx))
			require.Zero(t, s.taskQueue.len(), "satisfied report must not be admitted")
		})
	}
}

func TestArtifactReportRecoverySatisfiedBeforeWorkerAdmission(t *testing.T) {
	s, task, input := newArtifactReportRecoveryFixture(t)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	recovery := popArtifactRecovery(t, s)
	require.NoError(t, runArtifactReportRecoveryTask(s, task, input, func(string) (bool, error) { return true, nil }))
	sequence := s.taskTracker.models[gopherTaskModelKey(task)].newestDownloadSequence
	require.NoError(t, runArtifactReportRecoveryTask(s, recovery, input, func(string) (bool, error) { t.Fatal("satisfied recovery entered worker"); return true, nil }))
	require.Equal(t, sequence, s.taskTracker.models[gopherTaskModelKey(task)].newestDownloadSequence)
	// Ordinary Downloads must still perform their requested validation.
	normal := &GopherTask{TaskType: Download, BaseModel: task.BaseModel.DeepCopy()}
	validations := 0
	require.NoError(t, runArtifactReportRecoveryTask(s, normal, input, func(string) (bool, error) { validations++; return true, nil }))
	require.Equal(t, 1, validations)
}

func TestArtifactReportRecoveryRejectsUnsupportedOrStaleRequests(t *testing.T) {
	for _, change := range []string{"empty", "invalid", "evicted", "deleting", "placement", "replaced node", "local", "ordinary symlink", "shape filtered"} {
		t.Run(change, func(t *testing.T) {
			s, task, input := newArtifactReportRecoveryFixture(t)
			ctx := context.Background()
			switch change {
			case "empty":
				delete(task.BaseModel.Annotations, testRestoreAnnotation)
			case "invalid":
				task.BaseModel.Annotations[testRestoreAnnotation] = "invalid/request"
			case "evicted":
				task.BaseModel.Annotations[artifactResidencyAnnotation] = "Evicted"
			case "deleting":
				now := metav1.Now()
				task.BaseModel.DeletionTimestamp = &now
			case "placement":
				task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
			case "replaced node":
				node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				node.UID = "replacement"
				_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "local":
				task.BaseModel.Spec.Storage.StorageUri = ptr("local://" + input.ChildModelPath)
			case "shape filtered":
				task.BaseModel.Spec.ModelFormat.Name = constants.TensorRTLLM
			case "ordinary symlink":
				require.NoError(t, os.Remove(input.ChildModelPath))
				directory := t.TempDir()
				require.NoError(t, writeTestHfArtifactFiles(directory))
				require.NoError(t, os.Symlink(directory, input.ChildModelPath))
				cm, err := s.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				cm.Data = map[string]string{}
				_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, s.recoverArtifactReports(ctx))
			require.Zero(t, s.taskQueue.len())
			if change == "ordinary symlink" {
				require.FileExists(t, filepath.Join(input.ChildModelPath, "config.json"))
			}
		})
	}
}

func TestArtifactReportRecoveryReleasesAbandonedRetry(t *testing.T) {
	s, task, input := newArtifactReportRecoveryFixture(t)
	close(s.gopherChan)
	require.NoError(t, s.processTaskWithSourceAdapters(task, true,
		func(context.Context, *GopherTask, v1beta1.BaseModelSpec, bool) (bool, error) {
			return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(input.Parent.Key, errors.New("retry")))
		}, nil))
	require.Eventually(t, func() bool {
		s.taskQueue.mutex.Lock()
		defer s.taskQueue.mutex.Unlock()
		return len(s.taskQueue.pending) == 0
	}, time.Second, time.Millisecond, "failed delayed send must release reservation")
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Equal(t, 1, s.taskQueue.len())
	s.taskQueue.close()
	require.Empty(t, s.taskQueue.pending)
}

func TestArtifactReportRecoveryCanceledOrSupersededWhileQueued(t *testing.T) {
	for _, request := range []string{"", "request-2"} {
		t.Run(request, func(t *testing.T) {
			s, task, input := newArtifactReportRecoveryFixture(t)
			ctx := context.Background()
			require.NoError(t, s.recoverArtifactReports(ctx))
			task.BaseModel.Annotations[testRestoreAnnotation] = request
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.Error(t, runArtifactReportRecoveryTask(s, popArtifactRecovery(t, s), input, func(string) (bool, error) { t.Fatal("stale request validated bytes"); return true, nil }))
			require.Empty(t, s.taskTracker.models, "stale work must not advance admission fences")
			require.NoError(t, s.recoverArtifactReports(ctx))
			if request == "" {
				require.Zero(t, s.taskQueue.len())
			} else {
				require.Equal(t, request, artifactRehydrationID(popArtifactRecovery(t, s)))
			}
		})
	}
}

func TestArtifactReportRecoveryPinsNodeAtWorkerAdmission(t *testing.T) {
	s, _, input := newArtifactReportRecoveryFixture(t)
	ctx := context.Background()
	require.NoError(t, s.recoverArtifactReports(ctx))
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.UID = "replacement"
	_, err = s.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Error(t, runArtifactReportRecoveryTask(s, popArtifactRecovery(t, s), input, func(string) (bool, error) {
		t.Fatal("a replaced Node admitted restoration")
		return true, nil
	}))
	require.Empty(t, s.taskTracker.models)
}

func TestArtifactReportRecoveryClusterModel(t *testing.T) {
	s, original, input := newArtifactReportRecoveryFixture(t)
	ctx := context.Background()
	model := &v1beta1.ClusterBaseModel{ObjectMeta: *original.BaseModel.ObjectMeta.DeepCopy(), Spec: original.BaseModel.Spec}
	model.Namespace, model.UID = "", "cluster-uid"
	require.NoError(t, s.omeClient.OmeV1beta1().BaseModels(original.BaseModel.Namespace).Delete(ctx, original.BaseModel.Name, metav1.DeleteOptions{}))
	_, err := s.omeClient.OmeV1beta1().ClusterBaseModels().Create(ctx, model, metav1.CreateOptions{})
	require.NoError(t, err)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, indexer.Add(model))
	s.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(indexer)
	input.ChildModelKey, input.ChildModelUID = getModelID(nil, model), model.UID
	require.NoError(t, s.recoverArtifactReports(ctx))
	task := popArtifactRecovery(t, s)
	require.NotNil(t, task.ClusterBaseModel)
	require.NoError(t, runArtifactReportRecoveryTask(s, task, input, func(string) (bool, error) { return true, nil }))
	require.NoError(t, s.validateArtifactRestoreReport(ctx, task))
	client := s.kubeClient.(*fake.Clientset)
	client.ClearActions()
	require.NoError(t, s.recoverArtifactReports(ctx))
	require.Zero(t, s.taskQueue.len())
	nodeReads := 0
	for _, action := range client.Actions() {
		if action.Matches("get", "nodes") {
			nodeReads++
		}
	}
	require.Equal(t, 1, nodeReads, "satisfied requests use the pass snapshot without per-model live calls")
}

func TestArtifactReportRecoveryCoalescesActiveAndDelayedTasks(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovery", true: "override"}[explicit], func(t *testing.T) {
			s, task, input := newArtifactReportRecoveryFixture(t)
			ctx := context.Background()
			if explicit {
				task.TaskType = DownloadOverride
				s.enqueueTask(task)
				task, _ = s.taskQueue.popNormal()
			} else {
				require.NoError(t, s.recoverArtifactReports(ctx))
				task = popArtifactRecovery(t, s)
			}
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			// Force a retry through the real worker and timer rather than adding
			// synthetic pending entries to the queue.
			go func() {
				done <- s.processTaskWithSourceAdapters(task, true, func(context.Context, *GopherTask, v1beta1.BaseModelSpec, bool) (bool, error) {
					close(started)
					<-release
					return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(input.Parent.Key, errors.New("publication unavailable")))
				}, nil)
			}()
			<-started
			for i := 0; i < 3; i++ {
				require.NoError(t, s.recoverArtifactReports(ctx))
				require.Zero(t, s.taskQueue.len())
			}
			unblock()
			require.NoError(t, <-done)
			for i := 0; i < 3; i++ {
				require.NoError(t, s.recoverArtifactReports(ctx))
				require.Zero(t, s.taskQueue.len(), "timer/channel retry remains reserved")
			}
			retry := receiveDirectRetry(t, s)
			require.Same(t, task, retry)
			sequence := task.Sequence
			s.enqueueTask(retry)
			if shouldUseHighPriorityQueue(task) {
				task, _ = s.taskQueue.popHighPriority()
			} else {
				task, _ = s.taskQueue.popNormal()
			}
			require.Equal(t, sequence, task.Sequence)
			if explicit {
				require.Equal(t, DownloadOverride, task.TaskType)
			}
			require.NoError(t, runArtifactReportRecoveryTask(s, task, input, func(string) (bool, error) { return true, nil }))
			require.NoError(t, s.recoverArtifactReports(ctx))
			require.Zero(t, s.taskQueue.len())
		})
	}
}

func TestArtifactReportRecoveryDoesNotSupersedeExplicitOverride(t *testing.T) {
	s, task, input := newArtifactReportRecoveryFixture(t)
	task.TaskType = DownloadOverride
	task.Sequence = s.taskTracker.ensureSequence(0)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	recovery := popArtifactRecovery(t, s)
	require.Greater(t, recovery.Sequence, task.Sequence)
	s.enqueueTask(task)
	require.NoError(t, runArtifactReportRecoveryTask(s, recovery, input, func(string) (bool, error) { t.Fatal("periodic task overtook explicit override"); return true, nil }))
	require.Empty(t, s.taskTracker.models)
	queued, ok := s.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, task, queued)
	require.NoError(t, runArtifactReportRecoveryTask(s, task, input, func(string) (bool, error) { return true, nil }))
	require.Equal(t, task.Sequence, s.taskTracker.models[gopherTaskModelKey(task)].newestDownloadSequence)
}

func TestArtifactReportRecoveryOverlappingDemotionKeepsReservation(t *testing.T) {
	s, explicit, _ := newArtifactReportRecoveryFixture(t)
	explicit.TaskType = DownloadOverride
	s.enqueueTask(explicit)
	_, _ = s.taskQueue.popNormal()
	require.True(t, s.taskQueue.startTask(explicit))
	s.demoteToNormalPriority(explicit)
	_, _ = s.taskQueue.popNormal()
	require.True(t, s.taskQueue.startTask(explicit))
	s.taskQueue.finishTask(explicit) // The first worker returns after the second starts.
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Zero(t, s.taskQueue.len(), "old finalizer must retain the active second attempt")
	s.taskQueue.finishTask(explicit)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Equal(t, 1, s.taskQueue.len(), "terminal completion releases recovery reservation")
}

func TestArtifactReportRecoveryLostPublicationAndRestart(t *testing.T) {
	for _, lost := range []string{"report response", "node response", "node rejection"} {
		t.Run(lost, func(t *testing.T) {
			s, task, input := newArtifactReportRecoveryFixture(t)
			ctx := context.Background()
			client := s.kubeClient.(*fake.Clientset)
			injected := false
			if lost == "report response" {
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
					require.NoError(t, err)
					if injected || entry.Status != ModelStatusReady || entry.ArtifactRehydrationID == "" {
						return false, nil, nil
					}
					injected = true
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					return true, nil, errors.New("report response lost")
				})
			} else {
				client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
					patch := string(action.(ktesting.PatchAction).GetPatch())
					if injected || !strings.Contains(patch, "request-1") {
						return false, nil, nil
					}
					injected = true
					if lost == "node response" {
						object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
						require.NoError(t, err)
						node := object.(*corev1.Node).DeepCopy()
						key, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
						require.NoError(t, err)
						node.Labels[key], node.Labels[testRequestLabel(task)] = string(Ready), "request-1"
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
					}
					return true, nil, errors.New("node publication response unavailable")
				})
			}
			s.samePathWaitTimeout = time.Nanosecond
			task.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
			_ = runArtifactReportRecoveryTask(s, task, input, func(string) (bool, error) { return true, nil })
			require.True(t, injected)
			// Restart loses retry/ownership caches but keeps API state and files.
			s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.kubeClient, s.logger)
			WithArtifactEviction(s.omeClient, s.artifactNodeUID)(s)
			s.taskQueue, s.taskTracker, s.artifactRouting = newGopherTaskQueue(), gopherTaskTracker{}, gopherArtifactRouting{}
			s.hfArtifactHandlerOnce = sync.Once{}
			require.NoError(t, s.recoverArtifactReports(ctx))
			if s.taskQueue.len() != 0 {
				require.NoError(t, runArtifactReportRecoveryTask(s, popArtifactRecovery(t, s), input, func(string) (bool, error) { return true, nil }))
			}
			require.NoError(t, s.validateArtifactRestoreReport(ctx, task))
			node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, "request-1", node.Labels[testRequestLabel(task)])
		})
	}
}

func TestArtifactReportRecoveryRunSchedules(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
	s.configMapReconciler.reconcileInterval = 5 * time.Millisecond
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { s.Run(stop, 0, 1); close(done) }()
	t.Cleanup(func() { close(stop); <-done })
	require.Eventually(t, func() bool { return s.taskQueue.len() == 1 }, time.Second, time.Millisecond,
		"initial or periodic reconciliation must queue a missing current-request report")
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, s.taskQueue.len(), "repeated ticks must coalesce pending recovery")
}
